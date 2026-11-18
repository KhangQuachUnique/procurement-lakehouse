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
from procurement.common.silver_gold_contract import projection
from procurement.gold.domain.models import (
    INPUT_TABLES,
    ResourceBudget,
    RunPlan,
    SilverReadyInput,
    Snapshot,
)
from procurement.gold.application.warehouse import ensure_gold_tables
from procurement.gold.domain.schema import DIMENSIONS, FACTS, TABLES, group_hash
from procurement.gold.application.compute.engine import run_business
from procurement.gold.application.compute.projection import project
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


_UPSTREAM_FALLBACK_SPECS: tuple[tuple[str, str, str], ...] = (
    (
        "contract_revision",
        """
        SELECT DISTINCT NULLIF(TRIM(contract_code), '') AS ref_key
        FROM frozen.contract_performance_revision
        WHERE NULLIF(TRIM(contract_code), '') IS NOT NULL
          AND NULLIF(TRIM(contract_code), '') NOT IN (
              SELECT DISTINCT contract_code FROM frozen.contract_revision WHERE contract_code IS NOT NULL
          )
        """,
        "contract_code IN (SELECT ref_key FROM _missing_refs)",
    ),
    (
        "opening_revision",
        """
        SELECT DISTINCT NULLIF(TRIM(notify_no), '') AS ref_key
        FROM frozen.result_revision
        WHERE NULLIF(TRIM(notify_no), '') IS NOT NULL
          AND NULLIF(TRIM(notify_no), '') NOT IN (
              SELECT DISTINCT notify_no FROM frozen.opening_revision WHERE notify_no IS NOT NULL
          )
        """,
        "notify_no IN (SELECT ref_key FROM _missing_refs)",
    ),
    (
        "package_revision",
        """
        SELECT DISTINCT pkg_id AS ref_key FROM (
            SELECT NULLIF(TRIM(bid_id), '') AS pkg_id FROM frozen.result_revision
            UNION
            SELECT NULLIF(TRIM(package_id), '') AS pkg_id FROM frozen.notice_revision
            UNION
            SELECT NULLIF(TRIM(package_id), '') AS pkg_id FROM frozen.opening_revision
        ) WHERE pkg_id IS NOT NULL
          AND pkg_id NOT IN (
              SELECT COALESCE(NULLIF(TRIM(json_extract_string(root_json, '$.id')), ''), entity_id)
              FROM frozen.package_revision
          )
        """,
        "source_id IN (SELECT ref_key FROM _missing_refs)",
    ),
    (
        "package_revision",
        """
        SELECT DISTINCT pkg_no AS ref_key FROM (
            SELECT NULLIF(TRIM(package_no), '') AS pkg_no FROM frozen.result_revision
            UNION
            SELECT NULLIF(TRIM(package_no), '') AS pkg_no FROM frozen.notice_revision
            UNION
            SELECT NULLIF(TRIM(package_no), '') AS pkg_no FROM frozen.opening_revision
        ) WHERE pkg_no IS NOT NULL
          AND pkg_no NOT IN (
              SELECT DISTINCT package_no FROM frozen.package_revision WHERE package_no IS NOT NULL
          )
        """,
        "package_no IN (SELECT ref_key FROM _missing_refs)",
    ),
    (
        "notice_revision",
        """
        SELECT DISTINCT notify_no AS ref_key FROM (
            SELECT NULLIF(TRIM(notify_no), '') AS notify_no FROM frozen.result_revision
            UNION
            SELECT NULLIF(TRIM(notify_no), '') AS notify_no FROM frozen.opening_revision
            UNION
            SELECT NULLIF(TRIM(notice_no), '') AS notify_no FROM frozen.contract_revision
        ) WHERE notify_no IS NOT NULL
          AND notify_no NOT IN (
              SELECT DISTINCT notify_no FROM frozen.notice_revision WHERE notify_no IS NOT NULL
          )
        """,
        "notify_no IN (SELECT ref_key FROM _missing_refs)",
    ),
    (
        "plan_revision",
        """
        SELECT DISTINCT NULLIF(TRIM(plan_id), '') AS ref_key
        FROM frozen.package_revision
        WHERE NULLIF(TRIM(plan_id), '') IS NOT NULL
          AND NULLIF(TRIM(plan_id), '') NOT IN (
              SELECT COALESCE(NULLIF(TRIM(json_extract_string(root_json, '$.id')), ''), entity_id)
              FROM frozen.plan_revision
          )
        """,
        "source_id IN (SELECT ref_key FROM _missing_refs)",
    ),
)


def _pull_upstream_references(
    conn, local, silver_namespace: str, start_date: str, append_fn
) -> None:
    """Resolve cross-month upstream revisions via linkage_index (O(1)) or declarative reference closure."""
    has_linkage = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_catalog='lake' AND table_schema=? AND table_name='linkage_index'",
        [silver_namespace],
    ).fetchone()[0]
    if has_linkage:
        seed_revs = local.execute("""
            SELECT DISTINCT revision_id FROM frozen.result_revision
            UNION
            SELECT DISTINCT revision_id FROM frozen.notice_revision
            UNION
            SELECT DISTINCT revision_id FROM frozen.opening_revision
            UNION
            SELECT DISTINCT revision_id FROM frozen.contract_revision
            UNION
            SELECT DISTINCT revision_id FROM frozen.contract_performance_revision
        """).to_arrow_table()
        if len(seed_revs) == 0:
            return
        conn.register("_seed_revs", seed_revs)
        try:
            conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE _upstream_links AS
                SELECT li.entity_type, li.revision_id, li.public_date
                FROM lake.{silver_namespace}.linkage_index li
                WHERE li.component_id IN (
                    SELECT DISTINCT component_id
                    FROM lake.{silver_namespace}.linkage_index
                    WHERE revision_id IN (SELECT revision_id FROM _seed_revs)
                )
                  AND li.public_date < '{start_date}'
            """)
            for up_tbl in (
                "package_revision",
                "notice_revision",
                "plan_revision",
                "opening_revision",
                "contract_revision",
            ):
                bounds = conn.execute(
                    "SELECT count(*), min(public_date), max(public_date), min(revision_id), max(revision_id) FROM _upstream_links WHERE entity_type=?",
                    [up_tbl],
                ).fetchone()
                if bounds and bounds[0] > 0:
                    min_d, max_d, min_r, max_r = bounds[1], bounds[2], bounds[3], bounds[4]
                    date_pred = (
                        f"public_date >= '{min_d}' AND public_date <= '{max_d}' AND public_date IN (SELECT DISTINCT public_date FROM _upstream_links WHERE entity_type='{up_tbl}' AND public_date IS NOT NULL) AND "
                        if min_d and max_d
                        else ""
                    )
                    rev_pred = ""
                    if min_r and max_r:
                        min_r_esc = str(min_r).replace("'", "''")
                        max_r_esc = str(max_r).replace("'", "''")
                        rev_pred = f"revision_id >= '{min_r_esc}' AND revision_id <= '{max_r_esc}' AND "
                    append_fn(
                        up_tbl,
                        f"{date_pred}{rev_pred}revision_id IN (SELECT revision_id FROM _upstream_links WHERE entity_type='{up_tbl}')",
                    )
        finally:
            conn.unregister("_seed_revs")
        return

    for target_tbl, local_missing_sql, remote_pred in _UPSTREAM_FALLBACK_SPECS:
        missing_refs = local.execute(local_missing_sql).to_arrow_table()
        if len(missing_refs) > 0:
            conn.register("_missing_refs", missing_refs)
            try:
                append_fn(
                    target_tbl,
                    f"public_date < '{start_date}' AND is_current = TRUE AND ({remote_pred})",
                )
            finally:
                conn.unregister("_missing_refs")


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
        # 1. Extract Silver tables using the standard canonical projection
        def _extract_table(table_name: str, where_sql: str) -> int:
            cols = [r[0] for r in conn.execute(f"DESCRIBE lake.{silver_namespace}.{table_name}").fetchall()]
            select_cols = ", ".join(projection(cols, table_name))
            arrow_tbl = conn.execute(f"SELECT {select_cols} FROM lake.{silver_namespace}.{table_name} WHERE {where_sql}").to_arrow_table()
            count = len(arrow_tbl)
            temp_name = f"_tmp_{table_name}"
            local.register(temp_name, arrow_tbl)
            local.execute(f"CREATE TABLE frozen.{table_name} AS SELECT * FROM {temp_name}")
            local.unregister(temp_name)
            return count

        def _append_table(table_name: str, where_sql: str) -> int:
            cols = [r[0] for r in conn.execute(f"DESCRIBE lake.{silver_namespace}.{table_name}").fetchall()]
            select_cols = ", ".join(projection(cols, table_name))
            arrow_tbl = conn.execute(
                f"SELECT {select_cols} FROM lake.{silver_namespace}.{table_name} WHERE {where_sql}"
            ).to_arrow_table()
            count = len(arrow_tbl)
            if count == 0:
                return 0
            temp_name = f"_tmp_append_{table_name}"
            local.register(temp_name, arrow_tbl)
            local.execute(f"""
                INSERT INTO frozen.{table_name}
                SELECT * FROM {temp_name} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM frozen.{table_name} f WHERE f.revision_id = t.revision_id
                )
            """)
            local.unregister(temp_name)
            return count

        def _extract_child_by_revisions(table_name: str, parent_table: str) -> int:
            rev_arrow = local.execute(f"SELECT DISTINCT revision_id FROM frozen.{parent_table} WHERE revision_id IS NOT NULL").to_arrow_table()
            if len(rev_arrow) == 0:
                cols = [r[0] for r in conn.execute(f"DESCRIBE lake.{silver_namespace}.{table_name}").fetchall()]
                select_cols = ", ".join(projection(cols, table_name))
                empty_tbl = conn.execute(f"SELECT {select_cols} FROM lake.{silver_namespace}.{table_name} LIMIT 0").to_arrow_table()
                local.register("_tmp_empty", empty_tbl)
                local.execute(f"CREATE TABLE frozen.{table_name} AS SELECT * FROM _tmp_empty")
                local.unregister("_tmp_empty")
                return 0
            bounds = local.execute(
                f"SELECT min(revision_id), max(revision_id) FROM frozen.{parent_table} WHERE revision_id IS NOT NULL"
            ).fetchone()
            rev_range_pred = ""
            if bounds and bounds[0] and bounds[1]:
                min_r = str(bounds[0]).replace("'", "''")
                max_r = str(bounds[1]).replace("'", "''")
                rev_range_pred = f"revision_id >= '{min_r}' AND revision_id <= '{max_r}' AND "
            conn.register("_target_revs", rev_arrow)
            try:
                return _extract_table(
                    table_name,
                    f"{rev_range_pred}revision_id IN (SELECT revision_id FROM _target_revs)",
                )
            finally:
                conn.unregister("_target_revs")

        input_rows = (
            _extract_table("package_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
            + _extract_table("notice_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
            + _extract_table("result_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
            + _extract_table("plan_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
            + _extract_table("opening_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
            + _extract_table("contract_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
            + _extract_table("contract_performance_revision", f"public_date >= '{start_date}' AND public_date < '{end_date}'")
        )

        if input_rows > 0:
            _pull_upstream_references(conn, local, silver_namespace, start_date, _append_table)

        input_rows += (
            _extract_child_by_revisions("award_lot", "result_revision")
            + _extract_child_by_revisions("award_contractor", "result_revision")
            + _extract_child_by_revisions("package_location", "package_revision")
            + _extract_child_by_revisions("opening_submission", "opening_revision")
        )
        has_notice_loc = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_catalog='lake' AND table_schema=? AND table_name='notice_location'",
            [silver_namespace],
        ).fetchone()[0]
        if has_notice_loc:
            _extract_child_by_revisions("notice_location", "notice_revision")
        _extract_table("observation", f"source_date >= '{start_date}' AND source_date < '{end_date}'")

        # Synthetic observations for any orphaned revisions
        for tbl in ("package_revision", "notice_revision", "result_revision", "plan_revision", "opening_revision", "contract_revision", "contract_performance_revision"):
            local.execute(f"""
                INSERT INTO frozen.observation (
                    observation_id, resource, source_date, run_id, table_name,
                    file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash
                )
                SELECT DISTINCT
                    r.observation_id, '{tbl}', '{start_date}'::DATE, 'range_run', '{tbl}',
                    'k', 's', 0, coalesce(r.public_date, '{start_date}'::DATE)::TIMESTAMP, r.entity_id, 1, 'h'
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
        start_d = date.fromisoformat(start_date)
        inclusive_end = max(start_d, date.fromisoformat(end_date) - timedelta(days=1))
        ready = SilverReadyInput(
            input_id=f"input_{start_date}_{end_date}",
            cohort_id=f"cohort_{start_date}_{end_date}",
            namespace=silver_namespace,
            mapping_version="v1",
            completed_at=datetime.now(UTC),
            source_start=start_d,
            source_end=inclusive_end,
            source_complete=True,
            blocking_issues=0,
            quarantined=0,
            tables=tuple(Snapshot(table=n, table_uuid="uuid_" + n, snapshot_id=1, rows=0) for n in INPUT_TABLES),
        )
        from procurement.gold.domain.policies import DEFAULT_REFERENCE_POLICY

        plan = RunPlan(
            run_id=f"run_{start_date.replace('-', '_')}_{end_date.replace('-', '_')}",
            mode="backfill",
            logic_version="a" * 64,
            reference_version=DEFAULT_REFERENCE_POLICY.version,
            references_json=DEFAULT_REFERENCE_POLICY.model_dump_json(),
            target_namespace=gold_namespace,
            source=ready,
            source_cutoff=inclusive_end,
            event_start=start_d,
            event_end=inclusive_end,
            observation_day=inclusive_end,
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
                WHERE report_date IS NOT NULL
                  AND report_date >= DATE '{start_date}'
                  AND report_date < DATE '{end_date}'
                GROUP BY {grain_cols}
            """)
            fact_counts[fact] = local.execute(f"SELECT count(*) FROM outbox.{fact}").fetchone()[0]

        total_fact_rows = sum(fact_counts.values())

        # 5. Publish to Iceberg Lakehouse (lake.{gold_namespace})
        t_pub_start = time.perf_counter()
        ensure_gold_tables(conn, namespace=gold_namespace)

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
