"""Application Service for day-level and month-level Gold dimensional processing.

Coordinates:
1. Extracting Silver revisions and references for a target date range.
2. Running business transformations (linking relations, dedup, status).
3. Projecting into Conformed Dimensions and Fact Cohorts.
4. Aggregating facts to their contract grain.
5. Upserting Dimensions (Insert Where Not Exists) and Partition Overwrite for Facts.
"""

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
import time
from typing import Any

import duckdb

from procurement.common.settings import Settings
from procurement.gold.domain.models import (
    INPUT_TABLES,
    ResourceBudget,
    RunPlan,
    SilverReadyInput,
    Snapshot,
)
from procurement.gold.domain.schema import DIMENSIONS, FACTS, TABLES, group_hash
from procurement.gold.v2.compute.engine import run_business
from procurement.gold.v2.compute.projection import project
from procurement.storage.iceberg import connect


def get_date_bounds(target_month: str) -> tuple[str, str]:
    """Calculate [start_date, end_date) for a YYYY-MM month string."""
    parts = target_month.split("-")
    year, month = int(parts[0]), int(parts[1])
    start_date = f"{year}-{month:02d}-01"
    if month == 12:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{month + 1:02d}-01"
    return start_date, end_date


def get_day_bounds(target_date: str) -> tuple[str, str]:
    """Calculate [start_date, end_date) for a single YYYY-MM-DD day string."""
    d = date.fromisoformat(target_date)
    next_d = d + timedelta(days=1)
    return str(d), str(next_d)


@contextmanager
def get_iceberg_connection(remote_conn=None, memory_limit="2GB", threads=4):
    """Context manager returning an active Iceberg connection."""
    if remote_conn is not None:
        yield remote_conn
    else:
        config = Settings.from_environment().iceberg_config()
        with connect(config, max_memory=memory_limit, threads=threads) as conn:
            yield conn


def process_gold_range(
    start_date: str,
    end_date: str,
    target_facts: list[str] | None = None,
    remote_conn=None,
    silver_namespace: str = "silver_v2",
    gold_namespace: str = "gold_v2",
    memory_limit: str = "4GB",
    threads: int = 4,
) -> dict[str, Any]:
    """Execute Gold processing for any date range with Partition Overwrite on facts and idempotent dimension upserts."""
    t0 = time.perf_counter()
    target_facts = target_facts or list(FACTS)

    local = duckdb.connect(":memory:", config={"max_memory": memory_limit, "threads": str(threads)})
    local.execute("CREATE SCHEMA frozen")

    with get_iceberg_connection(remote_conn, memory_limit="2GB", threads=threads) as conn:
        # 1. Extract Silver tables
        pkg_arrow = conn.execute(f"""
            SELECT *, '{{}}' AS root_json 
            FROM lake.{silver_namespace}.package_revision 
            WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
        """).to_arrow_table()
        local.register("_pkgs", pkg_arrow)
        local.execute("CREATE TABLE frozen.package_revision AS SELECT * FROM _pkgs")
        local.unregister("_pkgs")

        not_arrow = conn.execute(f"""
            SELECT *, '{{}}' AS root_json 
            FROM lake.{silver_namespace}.notice_revision 
            WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
        """).to_arrow_table()
        local.register("_nots", not_arrow)
        local.execute("CREATE TABLE frozen.notice_revision AS SELECT * FROM _nots")
        local.unregister("_nots")

        res_arrow = conn.execute(f"""
            SELECT *, '{{}}' AS root_json 
            FROM lake.{silver_namespace}.result_revision 
            WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
        """).to_arrow_table()
        local.register("_res", res_arrow)
        local.execute("CREATE TABLE frozen.result_revision AS SELECT * FROM _res")
        local.unregister("_res")

        plan_arrow = conn.execute(f"""
            SELECT *, '{{}}' AS root_json 
            FROM lake.{silver_namespace}.plan_revision 
            WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
        """).to_arrow_table()
        local.register("_plans", plan_arrow)
        local.execute("CREATE TABLE frozen.plan_revision AS SELECT * FROM _plans")
        local.unregister("_plans")

        lot_arrow = conn.execute(f"""
            SELECT al.*, '{{}}' AS raw_json 
            FROM lake.{silver_namespace}.award_lot al
            WHERE al.revision_id IN (
                SELECT revision_id FROM lake.{silver_namespace}.result_revision 
                WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
            )
        """).to_arrow_table()
        local.register("_lots", lot_arrow)
        local.execute("CREATE TABLE frozen.award_lot AS SELECT * FROM _lots")
        local.unregister("_lots")

        ctr_arrow = conn.execute(f"""
            SELECT ac.* 
            FROM lake.{silver_namespace}.award_contractor ac
            WHERE ac.revision_id IN (
                SELECT revision_id FROM lake.{silver_namespace}.result_revision 
                WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
            )
        """).to_arrow_table()
        local.register("_ctrs", ctr_arrow)
        local.execute("CREATE TABLE frozen.award_contractor AS SELECT * FROM _ctrs")
        local.unregister("_ctrs")

        loc_arrow = conn.execute(f"""
            SELECT pl.* 
            FROM lake.{silver_namespace}.package_location pl
            WHERE pl.revision_id IN (
                SELECT revision_id FROM lake.{silver_namespace}.package_revision 
                WHERE public_date >= '{start_date}' AND public_date < '{end_date}'
            )
        """).to_arrow_table()
        local.register("_locs", loc_arrow)
        local.execute("CREATE TABLE frozen.package_location AS SELECT * FROM _locs")
        local.unregister("_locs")

        obs_arrow = conn.execute(f"""
            SELECT o.*, '{{}}' AS payload_json 
            FROM lake.{silver_namespace}.observation o
            WHERE o.source_date >= '{start_date}' AND o.source_date < '{end_date}'
        """).to_arrow_table()
        local.register("_obs", obs_arrow)
        local.execute("CREATE TABLE frozen.observation AS SELECT * FROM _obs")
        local.unregister("_obs")

        # Synthetic observations for any orphaned revisions
        for tbl in ("package_revision", "notice_revision", "result_revision", "plan_revision"):
            local.execute(f"""
                INSERT INTO frozen.observation (
                    observation_id, resource, source_date, run_id, table_name,
                    file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash, payload_json
                )
                SELECT DISTINCT
                    r.observation_id, '{tbl}', '{start_date}'::DATE, 'range_run', '{tbl}',
                    'k', 's', 0, coalesce(r.public_date, '{start_date}'::DATE)::TIMESTAMP, r.entity_id, 1, 'h', '{{}}'
                FROM frozen.{tbl} r
                WHERE NOT EXISTS (SELECT 1 FROM frozen.observation o WHERE o.observation_id = r.observation_id)
            """)

        local.execute("""
            CREATE OR REPLACE TABLE frozen.observation AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY observation_id ORDER BY observed_at DESC) as rn
                FROM frozen.observation
            ) WHERE rn = 1
        """)

        t_extract = time.perf_counter() - t0
        input_rows = (
            len(pkg_arrow) + len(not_arrow) + len(res_arrow) + len(plan_arrow)
            + len(lot_arrow) + len(ctr_arrow) + len(loc_arrow)
        )

        if input_rows == 0:
            local.close()
            return {
                "start_date": start_date,
                "end_date": end_date,
                "input_rows": 0,
                "fact_rows": 0,
                "extract_time": t_extract,
                "biz_time": 0.0,
                "proj_time": 0.0,
                "publish_time": 0.0,
                "total_time": time.perf_counter() - t0,
                "facts": {f: 0 for f in target_facts},
            }

        # 2. Run Business Transformation
        t_biz_start = time.perf_counter()
        ready = SilverReadyInput(
            input_id=f"input_{start_date}_{end_date}",
            cohort_id=f"cohort_{start_date}_{end_date}",
            namespace=silver_namespace,
            mapping_version="v1",
            completed_at=datetime.now(UTC),
            source_start=date.fromisoformat(start_date),
            source_end=date.fromisoformat(end_date),
            source_complete=True,
            blocking_issues=0,
            quarantined=0,
            tables=tuple(Snapshot(table=n, table_uuid="uuid_" + n, snapshot_id=1, rows=0) for n in INPUT_TABLES),
        )
        plan = RunPlan(
            run_id=f"run_{start_date.replace('-', '_')}_{end_date.replace('-', '_')}",
            mode="backfill",
            logic_version="a" * 64,
            reference_version="raw_v1",
            target_namespace=gold_namespace,
            source=ready,
            source_cutoff=date.fromisoformat(end_date),
            event_start=date.fromisoformat(start_date),
            event_end=date.fromisoformat(end_date),
            observation_day=date.fromisoformat(end_date),
            budget=ResourceBudget(memory_limit=memory_limit, threads=threads),
        )

        run_business(local, plan, source="frozen")
        t_biz = time.perf_counter() - t_biz_start

        # 3. Project Dimensions & Fact Cohorts
        t_proj_start = time.perf_counter()
        project(local, plan)
        t_proj = time.perf_counter() - t_proj_start

        # 4. Fact Group Aggregation
        local.execute("CREATE SCHEMA outbox")
        fact_counts = {}
        for fact in target_facts:
            table = TABLES[fact]
            grain = table.grain
            measures = table.measures
            measure_exprs = ", ".join(f"SUM({m.name}) AS {m.name}" for m in measures)
            grain_cols = ", ".join(grain)
            key_hash = group_hash(grain)

            local.execute(f"""
                CREATE TABLE outbox.{fact} AS
                SELECT
                    {key_hash} AS group_key,
                    {grain_cols},
                    {measure_exprs}
                FROM changes.{fact}
                GROUP BY {grain_cols}
            """)
            fact_counts[fact] = local.execute(f"SELECT count(*) FROM outbox.{fact}").fetchone()[0]

        total_fact_rows = sum(fact_counts.values())

        # 5. Publish to Iceberg Lakehouse (lake.{gold_namespace})
        t_pub_start = time.perf_counter()

        # Dimensions: Insert Where Not Exists
        for dim in DIMENSIONS:
            pk = TABLES[dim].key
            dim_table = local.execute(f"SELECT * FROM dimensions.{dim}").to_arrow_table()
            if len(dim_table) > 0:
                conn.register("_upload_dim", dim_table)
                try:
                    conn.execute(f"""
                        INSERT INTO lake.{gold_namespace}.{dim}
                        SELECT * FROM _upload_dim u
                        WHERE NOT EXISTS (SELECT 1 FROM lake.{gold_namespace}.{dim} d WHERE d.{pk} = u.{pk})
                    """)
                finally:
                    conn.unregister("_upload_dim")

        # Facts: Partition Overwrite for date window
        for fact in target_facts:
            fact_table = local.execute(f"SELECT * FROM outbox.{fact}").to_arrow_table()
            cols = ", ".join(c.name for c in TABLES[fact].columns)
            conn.execute("BEGIN")
            try:
                conn.execute(f"""
                    DELETE FROM lake.{gold_namespace}.{fact}
                    WHERE report_date >= '{start_date}' AND report_date < '{end_date}'
                """)
                if len(fact_table) > 0:
                    conn.register("_upload_fact", fact_table)
                    try:
                        conn.execute(f"INSERT INTO lake.{gold_namespace}.{fact} ({cols}) SELECT {cols} FROM _upload_fact")
                    finally:
                        conn.unregister("_upload_fact")
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

        t_pub = time.perf_counter() - t_pub_start
        total_time = time.perf_counter() - t0

        local.close()

        return {
            "start_date": start_date,
            "end_date": end_date,
            "input_rows": input_rows,
            "fact_rows": total_fact_rows,
            "extract_time": t_extract,
            "biz_time": t_biz,
            "proj_time": t_proj,
            "publish_time": t_pub,
            "total_time": total_time,
            "facts": fact_counts,
        }


def process_gold_day(
    target_date: str,
    target_facts: list[str] | None = None,
    remote_conn=None,
    silver_namespace: str = "silver_v2",
    gold_namespace: str = "gold_v2",
    memory_limit: str = "2GB",
    threads: int = 2,
) -> dict[str, Any]:
    """Execute 1-day incremental Gold processing."""
    start_date, end_date = get_day_bounds(target_date)
    return process_gold_range(
        start_date=start_date,
        end_date=end_date,
        target_facts=target_facts,
        remote_conn=remote_conn,
        silver_namespace=silver_namespace,
        gold_namespace=gold_namespace,
        memory_limit=memory_limit,
        threads=threads,
    )


def process_gold_month(
    target_month: str,
    remote_conn=None,
    silver_namespace: str = "silver_v2",
    gold_namespace: str = "gold_v2",
    memory_limit: str = "4GB",
    threads: int = 4,
) -> dict[str, Any]:
    """Execute monthly Gold processing with Partition Overwrite on facts and idempotent dimension upserts."""
    start_date, end_date = get_date_bounds(target_month)
    return process_gold_range(
        start_date=start_date,
        end_date=end_date,
        target_facts=list(FACTS),
        remote_conn=remote_conn,
        silver_namespace=silver_namespace,
        gold_namespace=gold_namespace,
        memory_limit=memory_limit,
        threads=threads,
    )
