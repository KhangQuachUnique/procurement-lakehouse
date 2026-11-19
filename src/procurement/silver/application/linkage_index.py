"""Silver-layer Union-Find Linkage Index builder.

Precomputes connected components (Union-Find) across all Silver procurement
root entities (plan, package, notice, opening, result, contract, contract_performance)
so downstream Gold slice/batch processors can resolve cross-month upstream
packages, plans, notices, and locations in O(1) without full-table scans or
repeated Union-Find runs.
"""

from array import array
import contextlib
import ctypes
import time
from typing import Any
import pyarrow as pa

from procurement.gold.domain.clustering.union_find import UnionFind
from procurement.gold.domain.models import identifier


def _trim_memory() -> None:
    with contextlib.suppress(Exception):
        ctypes.CDLL("libc.so.6").malloc_trim(0)

LINKAGE_INDEX_DDL = """
    component_id VARCHAR,
    entity_type VARCHAR,
    entity_id VARCHAR,
    revision_id VARCHAR,
    public_date DATE,
    source_month VARCHAR,
    package_id VARCHAR,
    package_no VARCHAR,
    notify_no VARCHAR,
    plan_id VARCHAR,
    plan_no VARCHAR
"""


def _table_exists(conn, schema_expr: str, table_name: str) -> bool:
    parts = schema_expr.split(".")
    if len(parts) == 2:
        cat, sch = parts
        return bool(
            conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_catalog=? AND table_schema=? AND table_name=?",
                [cat, sch, table_name],
            ).fetchone()[0]
        )
    return bool(
        conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema=? AND table_name=?",
            [schema_expr, table_name],
        ).fetchone()[0]
    )


def _column_exists(conn, schema_expr: str, table_name: str, column_name: str) -> bool:
    parts = schema_expr.split(".")
    if len(parts) == 2:
        cat, sch = parts
        return bool(
            conn.execute(
                "SELECT count(*) FROM information_schema.columns WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=?",
                [cat, sch, table_name, column_name],
            ).fetchone()[0]
        )
    return bool(
        conn.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_catalog=current_database() AND table_schema=? AND table_name=? AND column_name=?",
            [schema_expr, table_name, column_name],
        ).fetchone()[0]
    )


def build_silver_linkage_index(
    conn,
    source_schema: str = "silver",
    target_table: str = "silver.linkage_index",
    *,
    include_plan_links: bool = False,
) -> dict[str, int]:
    """Build the Silver Union-Find linkage_index table from Silver root revisions.

    Args:
        conn: DuckDB connection.
        source_schema: Source schema or catalog.schema (e.g. 'silver' or 'lake.silver_v2').
        target_table: Target table name to create/replace (e.g. 'silver.linkage_index').
        include_plan_links: Whether to union packages sharing the same plan_id/plan_no
            into a single component. Defaults to False for procurement package-level
            granularity while still recording plan_id/plan_no for direct plan lookup.
    """
    for part in source_schema.split("."):
        identifier(part)
    for part in target_table.split("."):
        identifier(part)

    with contextlib.suppress(Exception):
        conn.execute("SET preserve_insertion_order=false")
        conn.execute("SET temp_directory='/tmp/duckdb_linkage_spill'")

    arms = _build_revision_arms(conn, source_schema)
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _silver_uf_revisions AS " + " UNION ALL ".join(arms)
    )
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _silver_uf_nodes AS
        SELECT
            node,
            CAST(row_number() OVER (ORDER BY node) - 1 AS UINTEGER) AS node_id
        FROM (
            SELECT DISTINCT entity_type || ':' || entity_id AS node
            FROM _silver_uf_revisions
        )
    """)

    node_count = conn.execute("SELECT count(*) FROM _silver_uf_nodes").fetchone()[0]
    uf = UnionFind(node_count)

    token_arms = [
        "SELECT 'pkg_id:' || r.package_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.package_id IS NOT NULL",
        "SELECT 'pkg_id:' || r.entity_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type = 'package_revision'",
        "SELECT 'pkg_no:' || r.package_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.package_no IS NOT NULL",
        "SELECT 'notify_no:' || r.notify_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.notify_no IS NOT NULL",
        "SELECT 'contract:' || r.plan_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type IN ('contract_revision', 'contract_performance_revision') AND r.plan_no IS NOT NULL",
    ]
    if include_plan_links:
        token_arms.extend([
            "SELECT 'plan_id:' || r.plan_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.plan_id IS NOT NULL",
            "SELECT 'plan_id:' || r.entity_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type = 'plan_revision'",
            "SELECT 'plan_no:' || r.plan_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type IN ('package_revision', 'plan_revision') AND r.plan_no IS NOT NULL",
        ])

    cursor = conn.execute(
        f"SELECT DISTINCT token, node_id FROM ({' UNION ALL '.join(token_arms)}) ORDER BY token, node_id"
    )
    while rows := cursor.fetchmany(16384):
        uf.feed(rows)

    conn.execute("CREATE OR REPLACE TEMP TABLE _silver_uf_labels (node_id UINTEGER, min_id UINTEGER)")
    start = 0
    while start < node_count:
        end = min(start + 65536, node_count)
        ids = array("I", range(start, end))
        minima = array("I", (uf.label(i) for i in range(start, end)))
        batch = pa.RecordBatch.from_arrays(
            [
                pa.Array.from_buffers(pa.uint32(), end - start, [None, pa.py_buffer(ids)]),
                pa.Array.from_buffers(pa.uint32(), end - start, [None, pa.py_buffer(minima)]),
            ],
            ["node_id", "min_id"],
        )
        conn.register("_uf_label_batch", batch)
        conn.execute("INSERT INTO _silver_uf_labels SELECT * FROM _uf_label_batch")
        conn.unregister("_uf_label_batch")
        start = end

    components = conn.execute("SELECT count(DISTINCT min_id) FROM _silver_uf_labels").fetchone()[0]
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _silver_uf_node_comp AS
        SELECT n.node, m.node AS component_id
        FROM _silver_uf_nodes n
        JOIN _silver_uf_labels l ON l.node_id = n.node_id
        JOIN _silver_uf_nodes m ON m.node_id = l.min_id
    """)
    conn.execute("DROP TABLE IF EXISTS _silver_uf_nodes")
    conn.execute("DROP TABLE IF EXISTS _silver_uf_labels")
    _trim_memory()

    conn.execute(f"DROP TABLE IF EXISTS {target_table}")
    conn.execute(f"CREATE TABLE {target_table} ({LINKAGE_INDEX_DDL})")

    if target_table.startswith("lake."):
        years = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT COALESCE(substr(source_month, 1, 4), 'NULL') AS yr FROM _silver_uf_revisions ORDER BY yr"
            ).fetchall()
        ]
        for yr in years:
            cond = (
                "r.source_month IS NULL"
                if yr == "NULL"
                else f"substr(r.source_month, 1, 4) = '{yr}'"
            )
            conn.execute("BEGIN")
            try:
                conn.execute(f"""
                    INSERT INTO {target_table}
                    SELECT
                        nc.component_id,
                        r.entity_type,
                        r.entity_id,
                        r.revision_id,
                        r.public_date,
                        r.source_month,
                        r.package_id,
                        r.package_no,
                        r.notify_no,
                        r.plan_id,
                        r.plan_no
                    FROM _silver_uf_revisions r
                    JOIN _silver_uf_node_comp nc ON nc.node = r.entity_type || ':' || r.entity_id
                    WHERE {cond}
                    ORDER BY nc.component_id, r.entity_type, r.entity_id
                """)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            _trim_memory()
    else:
        conn.execute(f"""
            INSERT INTO {target_table}
            SELECT
                nc.component_id,
                r.entity_type,
                r.entity_id,
                r.revision_id,
                r.public_date,
                r.source_month,
                r.package_id,
                r.package_no,
                r.notify_no,
                r.plan_id,
                r.plan_no
            FROM _silver_uf_revisions r
            JOIN _silver_uf_node_comp nc ON nc.node = r.entity_type || ':' || r.entity_id
            ORDER BY nc.component_id, r.entity_type, r.entity_id
        """)

    total_rows = conn.execute("SELECT count(*) FROM _silver_uf_revisions").fetchone()[0]
    for tmp in ("_silver_uf_revisions", "_silver_uf_node_comp"):
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")
    _trim_memory()

    return {
        "mode": "full",
        "nodes": node_count,
        "edges": uf.edges,
        "components": components,
        "rows": total_rows,
    }


def _build_revision_arms(conn, source_schema: str, date_filter: str = "") -> list[str]:
    pkg_src_col = (
        "COALESCE(NULLIF(TRIM(source_id), ''), entity_id)"
        if _column_exists(conn, source_schema, "package_revision", "source_id")
        else "entity_id"
    )
    plan_src_col = (
        "COALESCE(NULLIF(TRIM(source_id), ''), entity_id)"
        if _column_exists(conn, source_schema, "plan_revision", "source_id")
        else "entity_id"
    )
    where_clause = f"WHERE entity_id IS NOT NULL {date_filter}"
    arms = [
        f"""
        SELECT
            'package_revision' AS entity_type,
            entity_id,
            revision_id,
            public_date,
            strftime(public_date, '%Y-%m') AS source_month,
            {pkg_src_col} AS package_id,
            NULLIF(TRIM(package_no), '') AS package_no,
            NULL::VARCHAR AS notify_no,
            NULLIF(TRIM(plan_id), '') AS plan_id,
            NULLIF(TRIM(plan_no), '') AS plan_no
        FROM {source_schema}.package_revision
        {where_clause}
        """,
        f"""
        SELECT
            'plan_revision' AS entity_type,
            entity_id,
            revision_id,
            public_date,
            strftime(public_date, '%Y-%m') AS source_month,
            NULL::VARCHAR AS package_id,
            NULL::VARCHAR AS package_no,
            NULL::VARCHAR AS notify_no,
            {plan_src_col} AS plan_id,
            NULLIF(TRIM(plan_no), '') AS plan_no
        FROM {source_schema}.plan_revision
        {where_clause}
        """,
        f"""
        SELECT
            'notice_revision' AS entity_type,
            entity_id,
            revision_id,
            public_date,
            strftime(public_date, '%Y-%m') AS source_month,
            NULLIF(TRIM(package_id), '') AS package_id,
            NULLIF(TRIM(package_no), '') AS package_no,
            NULLIF(TRIM(notify_no), '') AS notify_no,
            NULL::VARCHAR AS plan_id,
            NULL::VARCHAR AS plan_no
        FROM {source_schema}.notice_revision
        {where_clause}
        """,
        f"""
        SELECT
            'result_revision' AS entity_type,
            entity_id,
            revision_id,
            public_date,
            strftime(public_date, '%Y-%m') AS source_month,
            NULLIF(TRIM(bid_id), '') AS package_id,
            NULLIF(TRIM(package_no), '') AS package_no,
            NULLIF(TRIM(notify_no), '') AS notify_no,
            NULL::VARCHAR AS plan_id,
            NULL::VARCHAR AS plan_no
        FROM {source_schema}.result_revision
        {where_clause}
        """,
    ]
    if _table_exists(conn, source_schema, "opening_revision"):
        arms.append(
            f"""
            SELECT
                'opening_revision' AS entity_type,
                entity_id,
                revision_id,
                public_date,
                strftime(public_date, '%Y-%m') AS source_month,
                NULLIF(TRIM(package_id), '') AS package_id,
                NULLIF(TRIM(package_no), '') AS package_no,
                NULLIF(TRIM(notify_no), '') AS notify_no,
                NULL::VARCHAR AS plan_id,
                NULL::VARCHAR AS plan_no
            FROM {source_schema}.opening_revision
            {where_clause}
            """
        )
    if _table_exists(conn, source_schema, "contract_revision"):
        c_date_col = (
            "COALESCE(public_date, source_date)"
            if _column_exists(conn, source_schema, "contract_revision", "source_date")
            else "public_date"
        )
        c_where = where_clause.replace("public_date", c_date_col) if date_filter else where_clause
        arms.append(
            f"""
            SELECT
                'contract_revision' AS entity_type,
                entity_id,
                revision_id,
                {c_date_col} AS public_date,
                strftime({c_date_col}, '%Y-%m') AS source_month,
                NULL::VARCHAR AS package_id,
                NULL::VARCHAR AS package_no,
                NULLIF(TRIM(notice_no), '') AS notify_no,
                NULL::VARCHAR AS plan_id,
                NULLIF(TRIM(contract_code), '') AS plan_no
            FROM {source_schema}.contract_revision
            {c_where}
            """
        )
    if _table_exists(conn, source_schema, "contract_performance_revision"):
        cp_date_col = (
            "COALESCE(public_date, source_date)"
            if _column_exists(conn, source_schema, "contract_performance_revision", "source_date")
            else "public_date"
        )
        cp_where = where_clause.replace("public_date", cp_date_col) if date_filter else where_clause
        arms.append(
            f"""
            SELECT
                'contract_performance_revision' AS entity_type,
                entity_id,
                revision_id,
                {cp_date_col} AS public_date,
                strftime({cp_date_col}, '%Y-%m') AS source_month,
                NULL::VARCHAR AS package_id,
                NULL::VARCHAR AS package_no,
                NULL::VARCHAR AS notify_no,
                NULL::VARCHAR AS plan_id,
                NULLIF(TRIM(contract_code), '') AS plan_no
            FROM {source_schema}.contract_performance_revision
            {cp_where}
            """
        )
    return arms


def sync_silver_linkage_index(
    conn,
    source_schema: str = "silver",
    target_table: str = "silver.linkage_index",
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    include_plan_links: bool = False,
) -> dict[str, int | str]:
    """Incrementally synchronize or full-build the Silver Union-Find linkage_index.

    If `target_table` does not exist yet or `start_date`/`end_date` is omitted,
    delegates to `build_silver_linkage_index`. Otherwise executes Incremental
    Subgraph Union-Find on only the components touched by revisions in
    `[start_date, end_date]`.
    """
    for part in source_schema.split("."):
        identifier(part)
    t_parts = target_table.split(".")
    for part in t_parts:
        identifier(part)

    target_schema = ".".join(t_parts[:-1]) if len(t_parts) > 1 else "main"
    target_name = t_parts[-1]

    if not start_date or not end_date or not _table_exists(conn, target_schema, target_name):
        return build_silver_linkage_index(
            conn,
            source_schema=source_schema,
            target_table=target_table,
            include_plan_links=include_plan_links,
        )

    date_filter = f"AND public_date >= DATE '{start_date}' AND public_date <= DATE '{end_date}'"
    arms = _build_revision_arms(conn, source_schema, date_filter=date_filter)
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _delta_uf_revisions AS " + " UNION ALL ".join(arms)
    )
    delta_count = conn.execute("SELECT count(*) FROM _delta_uf_revisions").fetchone()[0]
    if delta_count == 0:
        conn.execute("DROP TABLE IF EXISTS _delta_uf_revisions")
        return {
            "mode": "incremental",
            "nodes": 0,
            "edges": 0,
            "components": 0,
            "rows": 0,
        }

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _touched_components AS
        SELECT DISTINCT li.component_id
        FROM {target_table} li
        WHERE EXISTS (
            SELECT 1 FROM _delta_uf_revisions d
            WHERE (li.entity_type = d.entity_type AND li.entity_id = d.entity_id)
               OR (li.package_id IS NOT NULL AND li.package_id = d.package_id)
               OR (li.package_no IS NOT NULL AND li.package_no = d.package_no)
               OR (li.notify_no IS NOT NULL AND li.notify_no = d.notify_no)
               OR (
                   li.entity_type IN ('contract_revision', 'contract_performance_revision')
                   AND d.entity_type IN ('contract_revision', 'contract_performance_revision')
                   AND li.plan_no IS NOT NULL AND li.plan_no = d.plan_no
               )
        )
    """)

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _silver_uf_revisions AS
        SELECT * FROM _delta_uf_revisions
        UNION ALL
        SELECT
            li.entity_type,
            li.entity_id,
            li.revision_id,
            li.public_date,
            li.source_month,
            li.package_id,
            li.package_no,
            li.notify_no,
            li.plan_id,
            li.plan_no
        FROM {target_table} li
        WHERE li.component_id IN (SELECT component_id FROM _touched_components)
          AND li.revision_id NOT IN (SELECT revision_id FROM _delta_uf_revisions)
    """)

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _silver_uf_nodes AS
        SELECT
            node,
            CAST(row_number() OVER (ORDER BY node) - 1 AS UINTEGER) AS node_id
        FROM (
            SELECT DISTINCT entity_type || ':' || entity_id AS node
            FROM _silver_uf_revisions
        )
    """)
    node_count = conn.execute("SELECT count(*) FROM _silver_uf_nodes").fetchone()[0]
    uf = UnionFind(node_count)

    token_arms = [
        "SELECT 'pkg_id:' || r.package_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.package_id IS NOT NULL",
        "SELECT 'pkg_id:' || r.entity_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type = 'package_revision'",
        "SELECT 'pkg_no:' || r.package_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.package_no IS NOT NULL",
        "SELECT 'notify_no:' || r.notify_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.notify_no IS NOT NULL",
        "SELECT 'contract:' || r.plan_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type IN ('contract_revision', 'contract_performance_revision') AND r.plan_no IS NOT NULL",
    ]
    if include_plan_links:
        token_arms.extend([
            "SELECT 'plan_id:' || r.plan_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.plan_id IS NOT NULL",
            "SELECT 'plan_id:' || r.entity_id AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type = 'plan_revision'",
            "SELECT 'plan_no:' || r.plan_no AS token, n.node_id FROM _silver_uf_revisions r JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id WHERE r.entity_type IN ('package_revision', 'plan_revision') AND r.plan_no IS NOT NULL",
        ])

    cursor = conn.execute(
        f"SELECT DISTINCT token, node_id FROM ({' UNION ALL '.join(token_arms)}) ORDER BY token, node_id"
    )
    while rows := cursor.fetchmany(16384):
        uf.feed(rows)

    conn.execute("CREATE OR REPLACE TEMP TABLE _silver_uf_labels (node_id UINTEGER, min_id UINTEGER)")
    start = 0
    while start < node_count:
        end = min(start + 65536, node_count)
        ids = array("I", range(start, end))
        minima = array("I", (uf.label(i) for i in range(start, end)))
        batch = pa.RecordBatch.from_arrays(
            [
                pa.Array.from_buffers(pa.uint32(), end - start, [None, pa.py_buffer(ids)]),
                pa.Array.from_buffers(pa.uint32(), end - start, [None, pa.py_buffer(minima)]),
            ],
            ["node_id", "min_id"],
        )
        conn.register("_uf_label_batch", batch)
        conn.execute("INSERT INTO _silver_uf_labels SELECT * FROM _uf_label_batch")
        conn.unregister("_uf_label_batch")
        start = end

    conn.execute("BEGIN")
    try:
        conn.execute(f"""
            DELETE FROM {target_table}
            WHERE component_id IN (SELECT component_id FROM _touched_components)
               OR revision_id IN (SELECT revision_id FROM _delta_uf_revisions)
        """)
        conn.execute(f"""
            INSERT INTO {target_table}
            SELECT
                m.node AS component_id,
                r.entity_type,
                r.entity_id,
                r.revision_id,
                r.public_date,
                r.source_month,
                r.package_id,
                r.package_no,
                r.notify_no,
                r.plan_id,
                r.plan_no
            FROM _silver_uf_revisions r
            JOIN _silver_uf_nodes n ON n.node = r.entity_type || ':' || r.entity_id
            JOIN _silver_uf_labels l ON l.node_id = n.node_id
            JOIN _silver_uf_nodes m ON m.node_id = l.min_id
            ORDER BY m.node, r.entity_type, r.entity_id
        """)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

    sub_rows = conn.execute("SELECT count(*) FROM _silver_uf_revisions").fetchone()[0]
    sub_components = conn.execute("SELECT count(DISTINCT min_id) FROM _silver_uf_labels").fetchone()[0]
    for tmp in (
        "_delta_uf_revisions",
        "_touched_components",
        "_silver_uf_revisions",
        "_silver_uf_nodes",
        "_silver_uf_labels",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    return {
        "mode": "incremental",
        "nodes": node_count,
        "edges": uf.edges,
        "components": sub_components,
        "rows": sub_rows,
    }


def run_linkage_index(
    namespace: str = "silver",
    *,
    mode: str = "full",
    start_date: str | None = None,
    end_date: str | None = None,
    include_plan_links: bool = False,
    memory_limit: str = "2GB",
    threads: int = 4,
) -> dict[str, Any]:
    """Connect to the Iceberg catalog and execute full or incremental linkage_index build."""
    from procurement.common.settings import settings
    from procurement.storage.iceberg import connect

    clean_ns = identifier(namespace)
    cfg = settings.iceberg_config()
    t0 = time.perf_counter()
    with connect(cfg, max_memory=memory_limit, threads=threads) as conn:
        if mode == "incremental" and start_date and end_date:
            res = sync_silver_linkage_index(
                conn,
                source_schema=f"lake.{clean_ns}",
                target_table=f"lake.{clean_ns}.linkage_index",
                start_date=start_date,
                end_date=end_date,
                include_plan_links=include_plan_links,
            )
        else:
            res = build_silver_linkage_index(
                conn,
                source_schema=f"lake.{clean_ns}",
                target_table=f"lake.{clean_ns}.linkage_index",
                include_plan_links=include_plan_links,
            )
    _trim_memory()
    res["wall_time_s"] = round(time.perf_counter() - t0, 2)
    res["namespace"] = clean_ns
    return res

