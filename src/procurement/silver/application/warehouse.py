import threading
from datetime import date
from typing import Any

import pyarrow as pa

from procurement.silver.domain.schema import (
    get_partition_spec,
    get_primary_key,
    get_silver_schemas,
    to_iceberg_ddl,
)
from procurement.storage.iceberg import identifier

SCHEMAS = get_silver_schemas()

_local_duckdb = threading.local()
_ICEBERG_COMMIT_LOCK = threading.Lock()


def _get_duckdb_conn():
    if not hasattr(_local_duckdb, "conn") or _local_duckdb.conn is None:
        import duckdb

        _local_duckdb.conn = duckdb.connect(
            ":memory:",
            config={"max_memory": "512MB", "threads": "2"},
        )
    return _local_duckdb.conn


def typed_arrow_rows(name: str, rows: list[dict[str, Any]]) -> pa.Table:
    """Validate and cast rows against the SQL contract before writing local Parquet."""
    clean_name = identifier(name)
    db = _get_duckdb_conn()
    db.execute(f"CREATE OR REPLACE TEMP TABLE typed_rows ({SCHEMAS[clean_name]})")
    try:
        if rows:
            db.register("input_rows", pa.Table.from_pylist(rows))
            try:
                db.execute("INSERT INTO typed_rows BY NAME SELECT * FROM input_rows")
            finally:
                db.unregister("input_rows")
        return db.execute("SELECT * FROM typed_rows").to_arrow_table()
    finally:
        db.execute("DROP TABLE IF EXISTS typed_rows")


def stage_partition(
    db,
    catalog,
    namespace: str,
    tables: dict[str, list[dict[str, Any]] | pa.Table],
    *,
    partition_date: date | str | None = None,
    release_id: str | None = None,
    schemas: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Stage candidate tables into Iceberg Lake using idempotent partition/key overwrites.

    For partitioned tables:
      - Deletes only records in matching partition(s) (either explicitly supplied via
        partition_date or dynamically discovered from candidate rows).
      - Re-inserts candidate records atomically inside an Iceberg ACID transaction.
      - Previous partitions remain untouched (Zero write amplification).

    For unpartitioned tables:
      - If a primary key is configured, deletes only matching IDs before insertion.
      - If no candidate rows are supplied, existing data is untouched.

    Args:
        db: DuckDB connection attached to Iceberg catalog 'lake'.
        catalog: RestCatalog instance for metadata queries and release pinning.
        namespace: Target namespace (e.g. 'silver').
        tables: Dict of table_name -> candidate rows (list of dicts or pyarrow.Table).
        partition_date: Optional explicit partition date (e.g. '2024-05-01').
        release_id: Optional release ID to pin snapshot tag in catalog.
        schemas: Optional dictionary of SQL table schemas.

    Returns:
        Dict of table_name -> {'snapshot_id': int, 'row_count': int}.
    """
    schemas = schemas or get_silver_schemas()
    clean_ns = identifier(namespace)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{clean_ns}")
    snapshots = {}

    for name, candidate in tables.items():
        clean_name = identifier(name)
        schema = schemas.get(clean_name)
        if schema is None:
            continue

        table = f"lake.{clean_ns}.{clean_name}"
        ddl = to_iceberg_ddl(clean_name, schema, clean_ns)
        db.execute(ddl)

        arrow_table: pa.Table | None = None
        if isinstance(candidate, pa.Table):
            arrow_table = candidate
        elif isinstance(candidate, list):
            arrow_table = pa.Table.from_pylist(candidate) if candidate else None

        spec = get_partition_spec(clean_name)
        pk = get_primary_key(clean_name)

        max_retries = 3
        for attempt in range(max_retries):
            with _ICEBERG_COMMIT_LOCK:
                db.execute("BEGIN")
                try:
                    # 1. True Key-Level UPSERT (or Partition Cleanup if empty batch with explicit partition_date)
                    if pk and arrow_table is not None and len(arrow_table) > 0:
                        db.register("_incoming_cand", arrow_table)
                        try:
                            db.execute(
                                f"DELETE FROM {table} WHERE {pk} IN (SELECT {pk} FROM _incoming_cand)"
                            )
                        finally:
                            db.unregister("_incoming_cand")
                    elif spec and partition_date is not None and (arrow_table is None or len(arrow_table) == 0):
                        if isinstance(partition_date, date):
                            p_str = partition_date.isoformat()
                        else:
                            p_str = str(partition_date)
                        db.execute(f"DELETE FROM {table} WHERE {spec} = '{p_str}'")

                    # 2. Atomic Insert
                    if arrow_table is not None and len(arrow_table) > 0:
                        db.register("candidate_rows", arrow_table)
                        try:
                            db.execute(f"INSERT INTO {table} BY NAME SELECT * FROM candidate_rows")
                        finally:
                            db.unregister("candidate_rows")

                    db.execute("COMMIT")
                    break
                except BaseException as exc:
                    import contextlib

                    with contextlib.suppress(Exception):
                        db.execute("ROLLBACK")
                    # On OCC conflict (e.g. HTTP 409 Conflict / CommitFailedException), retry with backoff
                    if attempt < max_retries - 1 and (
                        "Conflict" in str(exc) or "CommitFailed" in str(exc)
                    ):
                        import time

                        time.sleep(0.1 * (2**attempt))
                        continue
                    raise

        # 3. Snapshot verification & release pinning
        metadata = catalog.table(clean_ns, clean_name)["metadata"]
        if metadata.get("format-version") != 2:
            raise ValueError("Only verified Iceberg format v2 is supported")

        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id is not None and snapshot_id >= 0:
            if release_id:
                catalog.pin(clean_ns, clean_name, snapshot_id, release_id)
            # O(1) total row count from Iceberg metadata summary without full S3 table scan
            curr_snap = next(
                (s for s in metadata.get("snapshots", []) if s.get("snapshot-id") == snapshot_id),
                None,
            )
            if curr_snap and "summary" in curr_snap and "total-records" in curr_snap["summary"]:
                actual = int(curr_snap["summary"]["total-records"])
            else:
                actual = db.sql(
                    f"SELECT count(*) FROM {table} AT (VERSION => {snapshot_id})"
                ).fetchone()[0]
            snapshots[clean_name] = {"snapshot_id": snapshot_id, "row_count": actual}

    # Automatically ensure active vectorized current views in DuckDB
    ensure_views(db, clean_ns)

    return snapshots


VIEW_DEFINITIONS: dict[str, dict[str, str]] = {
    "v_latest_notice": {
        "source_table": "notice_revision",
        "partition_by": "entity_id",
        "order_by": "notify_version DESC NULLS LAST, public_date DESC NULLS LAST, published_at DESC NULLS LAST",
    },
    "v_latest_project": {
        "source_table": "project_revision",
        "partition_by": "entity_id",
        "order_by": "public_date DESC NULLS LAST, decision_date DESC NULLS LAST",
    },
    "v_latest_plan": {
        "source_table": "plan_revision",
        "partition_by": "entity_id",
        "order_by": "public_date DESC NULLS LAST, decision_date DESC NULLS LAST",
    },
    "v_latest_package": {
        "source_table": "package_revision",
        "partition_by": "entity_id",
        "order_by": "public_date DESC NULLS LAST, bid_price DESC NULLS LAST",
    },
    "v_latest_result": {
        "source_table": "result_revision",
        "partition_by": "entity_id",
        "order_by": "public_date DESC NULLS LAST, decision_date DESC NULLS LAST",
    },
    "v_latest_opening": {
        "source_table": "opening_revision",
        "partition_by": "entity_id",
        "order_by": "public_date DESC NULLS LAST, bid_open_date DESC NULLS LAST",
    },
}


def ensure_views(
    db,
    namespace: str = "silver",
    view_schema: str | None = None,
) -> list[str]:
    """Ensure DuckDB vectorized views exist for active revision deduplication.

    Views are created in DuckDB's session view_schema (defaults to namespace, e.g. 'silver')
    and query the underlying Iceberg tables in 'lake.{namespace}.{table}'.

    Args:
        db: DuckDB connection.
        namespace: Underlying Iceberg catalog namespace (defaults to 'silver').
        view_schema: Schema name in DuckDB for the views (defaults to namespace).

    Returns:
        List of view names created.
    """
    clean_ns = identifier(namespace)
    v_schema = identifier(view_schema or clean_ns)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {v_schema}")

    created: list[str] = []
    for view_name, cfg in VIEW_DEFINITIONS.items():
        src_table = f"lake.{clean_ns}.{cfg['source_table']}"
        try:
            db.sql(f"SELECT 1 FROM {src_table} LIMIT 0")
        except Exception:
            continue

        sql = f"""
        CREATE OR REPLACE VIEW {v_schema}.{view_name} AS
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {cfg['partition_by']} 
                ORDER BY {cfg['order_by']}
            ) AS rn
            FROM {src_table}
        ) WHERE rn = 1
        """
        db.execute(sql)
        created.append(f"{v_schema}.{view_name}")

    return created


def stage(db, catalog, namespace, tables, release_id, schemas=None):
    """Stage candidate tables into local Iceberg lake (backward-compatible wrapper)."""
    return stage_partition(
        db,
        catalog,
        namespace,
        tables,
        release_id=release_id,
        schemas=schemas,
    )


def read_pinned(db, release: dict[str, Any], name: str):
    """Read a snapshot pinned to a release manifest."""
    clean_ns = identifier(release["namespace"])
    clean_name = identifier(name)
    snapshot_id = release["tables"][clean_name]["snapshot_id"]
    return db.sql(
        f"SELECT * FROM lake.{clean_ns}.{clean_name} AT (VERSION => {snapshot_id})"
    ).to_arrow_table()


__all__ = [
    "SCHEMAS",
    "VIEW_DEFINITIONS",
    "ensure_views",
    "get_partition_spec",
    "get_primary_key",
    "read_pinned",
    "stage",
    "stage_partition",
    "to_iceberg_ddl",
    "typed_arrow_rows",
]
