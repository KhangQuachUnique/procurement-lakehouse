import threading
from datetime import date
from typing import Any

import pyarrow as pa

from procurement.silver.domain.schema import (
    get_partition_spec,
    get_primary_key,
    get_silver_schemas,
    get_table_column_types,
    to_iceberg_ddl,
)
from procurement.storage.iceberg import identifier

SCHEMAS = get_silver_schemas()

_local_duckdb = threading.local()
_ICEBERG_COMMIT_LOCK = threading.RLock()


def _get_duckdb_conn():
    if not hasattr(_local_duckdb, "conn") or _local_duckdb.conn is None:
        import duckdb

        _local_duckdb.conn = duckdb.connect(
            ":memory:",
            config={"max_memory": "2GB", "threads": "2"},
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


RESULT_SCHEMA_MIGRATIONS: dict[str, dict[str, str]] = {
    "award_lot": {
        "source_lot_id": "VARCHAR",
        "source_month": "VARCHAR",
    },
    "award_contractor": {
        "bid_result": "BIGINT",
        "price_source": "VARCHAR",
        "currency": "VARCHAR",
        "source_month": "VARCHAR",
    },
    "notice_lot": {
        "source_month": "VARCHAR",
    },
    "notice_location": {
        "source_month": "VARCHAR",
    },
    "notice_reoffer_submission": {
        "source_month": "VARCHAR",
    },
    "project_location": {
        "source_month": "VARCHAR",
    },
    "package_lot": {
        "source_month": "VARCHAR",
    },
    "package_location": {
        "source_month": "VARCHAR",
    },
    "plan_revision": {
        "source_id": "VARCHAR",
    },
    "package_revision": {
        "source_id": "VARCHAR",
    },
}

def _get_provisioned_set(db) -> set[tuple[str, str]]:
    if not hasattr(db, "_provisioned_silver_tables"):
        try:
            db._provisioned_silver_tables = set()
        except Exception:
            return set()
    return db._provisioned_silver_tables


def ensure_silver_tables(
    db,
    catalog: Any,
    namespace: str = "silver",
    tables: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    """Pre-flight schema provisioning: create and verify all Silver Iceberg tables upfront.

    Ensures that the inner daily processing loop executes ZERO DDL statements,
    eliminating lock contention and REST catalog roundtrips during execution.
    """
    clean_ns = identifier(namespace)
    if hasattr(catalog, "create_namespace_if_not_exists"):
        catalog.create_namespace_if_not_exists(clean_ns)
    elif hasattr(catalog, "catalog") and hasattr(catalog.catalog, "create_namespace_if_not_exists"):
        catalog.catalog.create_namespace_if_not_exists(clean_ns)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{clean_ns}")

    all_schemas = get_silver_schemas()
    target_tables = tables or list(all_schemas.keys())
    provisioned: list[str] = []
    prov_set = _get_provisioned_set(db)

    for name in target_tables:
        clean_name = identifier(name)
        schema = all_schemas.get(clean_name)
        if schema is None:
            continue
        key = (clean_ns, clean_name)
        if key in prov_set:
            continue

        table_id = f"lake.{clean_ns}.{clean_name}"
        ddl = to_iceberg_ddl(clean_name, schema, clean_ns)
        with _ICEBERG_COMMIT_LOCK:
            db.execute(ddl)
            _migrate_result_schema_if_needed(db, table_id, clean_name)
            prov_set.add(key)
        provisioned.append(clean_name)

    return provisioned


def _migrate_result_schema_if_needed(
    db, table_identifier: str, clean_name: str, arrow_table: pa.Table | None = None
) -> None:
    expected_additions = RESULT_SCHEMA_MIGRATIONS.get(clean_name)
    if not expected_additions:
        return

    with _ICEBERG_COMMIT_LOCK:
        try:
            res = db.execute(f"PRAGMA table_info('{table_identifier}')")
            if res is None:
                return
            rows = res.fetchall()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to inspect schema for {table_identifier}: {type(exc).__name__}"
            ) from exc

        if not rows:
            raise RuntimeError(f"Table inspection returned no columns for {table_identifier}")

        existing_cols = {row[1].lower(): str(row[2]).upper() for row in rows}

        for col_name, expected_type in expected_additions.items():
            if col_name.lower() in existing_cols:
                curr_type = existing_cols[col_name.lower()]
                if expected_type == "BIGINT" and "INT" not in curr_type:
                    raise ValueError(
                        f"Incompatible schema for {table_identifier}.{col_name}: "
                        f"expected {expected_type}, found {curr_type}"
                    )
                if expected_type == "VARCHAR" and not any(
                    t in curr_type for t in ("VARCHAR", "TEXT", "STRING")
                ):
                    raise ValueError(
                        f"Incompatible schema for {table_identifier}.{col_name}: "
                        f"expected {expected_type}, found {curr_type}"
                    )
            else:
                try:
                    db.execute(
                        f"ALTER TABLE {table_identifier} ADD COLUMN {col_name} {expected_type}"
                    )
                    existing_cols[col_name.lower()] = expected_type
                except Exception as exc:
                    if "already exists" not in str(exc).lower():
                        raise RuntimeError(
                            f"Failed to migrate column {col_name} on {table_identifier}: {type(exc).__name__}"
                        ) from exc


def stage_partition(
    db,
    catalog,
    namespace: str,
    tables: dict[str, list[dict[str, Any]] | pa.Table],
    *,
    partition_date: date | str | None = None,
    release_id: str | None = None,
    schemas: dict[str, str] | None = None,
    change_journal=None,
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
    if hasattr(catalog, "create_namespace_if_not_exists"):
        catalog.create_namespace_if_not_exists(clean_ns)
    elif hasattr(catalog, "catalog") and hasattr(catalog.catalog, "create_namespace_if_not_exists"):
        catalog.catalog.create_namespace_if_not_exists(clean_ns)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{clean_ns}")
    snapshots = {}

    def _extract_rids(cand: Any) -> set[str]:
        rids = set()
        if isinstance(cand, pa.Table):
            if "revision_id" in cand.column_names:
                for v in cand["revision_id"].to_pylist():
                    if v is not None:
                        rids.add(str(v))
        elif isinstance(cand, list):
            for row in cand:
                if isinstance(row, dict) and row.get("revision_id"):
                    rids.add(str(row["revision_id"]))
        return rids

    replaced_result_rids: set[str] = set()
    if "result_revision" in tables:
        replaced_result_rids.update(_extract_rids(tables["result_revision"]))
    if "award_lot" in tables:
        replaced_result_rids.update(_extract_rids(tables["award_lot"]))
    if "award_contractor" in tables:
        replaced_result_rids.update(_extract_rids(tables["award_contractor"]))

    def _extract_obs_ids(cand: Any) -> set[str]:
        obs_ids = set()
        if isinstance(cand, pa.Table):
            if "observation_id" in cand.column_names:
                for v in cand["observation_id"].to_pylist():
                    if v is not None:
                        obs_ids.add(str(v))
        elif isinstance(cand, list):
            for row in cand:
                if isinstance(row, dict) and row.get("observation_id"):
                    obs_ids.add(str(row["observation_id"]))
        return obs_ids

    incoming_observation_ids: set[str] = set()
    if "observation" in tables:
        incoming_observation_ids.update(_extract_obs_ids(tables["observation"]))
    if "quality_issue" in tables:
        incoming_observation_ids.update(_extract_obs_ids(tables["quality_issue"]))
    if "quarantine" in tables:
        incoming_observation_ids.update(_extract_obs_ids(tables["quarantine"]))

    for name, candidate in tables.items():
        clean_name = identifier(name)
        schema = schemas.get(clean_name)
        if schema is None:
            continue

        arrow_table: pa.Table | None = None
        if isinstance(candidate, pa.Table):
            arrow_table = candidate
        elif isinstance(candidate, list):
            arrow_table = pa.Table.from_pylist(candidate) if candidate else None

        table = f"lake.{clean_ns}.{clean_name}"
        key = (clean_ns, clean_name)
        prov_set = _get_provisioned_set(db)
        if key not in prov_set:
            ddl = to_iceberg_ddl(clean_name, schema, clean_ns)
            with _ICEBERG_COMMIT_LOCK:
                if key not in prov_set:
                    db.execute(ddl)
                    _migrate_result_schema_if_needed(db, table, clean_name, arrow_table)
                    prov_set.add(key)

        spec = get_partition_spec(clean_name)
        pk = get_primary_key(clean_name)

        journal_before = None
        if change_journal is not None:
            from procurement.silver.application.change_journal import capture_before

            journal_before = capture_before(
                db, catalog, clean_ns, clean_name, arrow_table, replaced_result_rids, partition_date
            )
        max_retries = 3
        for attempt in range(max_retries):
            with _ICEBERG_COMMIT_LOCK:
                db.execute("BEGIN")
                try:
                    if clean_name == "contract_performance_revision" and partition_date is not None:
                        # Replace this publication-day slice, including records removed on refresh.
                        day_value = date.fromisoformat(str(partition_date)).isoformat()
                        db.execute(f"DELETE FROM {table} WHERE source_date = DATE '{day_value}'")
                    elif clean_name == "observation":
                        if (
                            arrow_table is not None
                            and "resource" in arrow_table.column_names
                            and "source_date" in arrow_table.column_names
                            and len(arrow_table) > 0
                        ):
                            db.register("_incoming_obs_dates", arrow_table)
                            try:
                                db.execute(
                                    f"DELETE FROM {table} WHERE (source_date, resource) IN "
                                    f"(SELECT DISTINCT source_date, resource FROM _incoming_obs_dates)"
                                )
                            finally:
                                db.unregister("_incoming_obs_dates")
                        elif partition_date is not None:
                            day_value = date.fromisoformat(str(partition_date)).isoformat()
                            db.execute(f"DELETE FROM {table} WHERE source_date = DATE '{day_value}'")
                    # 1. True Key-Level UPSERT (or Partition Cleanup if empty batch with explicit partition_date)
                    if clean_name in {"quality_issue", "quarantine"} and incoming_observation_ids:
                        db.register(
                            "_cohort_obs_ids",
                            pa.Table.from_arrays(
                                [pa.array(sorted(incoming_observation_ids), type=pa.string())],
                                names=["observation_id"],
                            ),
                        )
                        try:
                            db.execute(
                                f"DELETE FROM {table} WHERE observation_id IN (SELECT observation_id FROM _cohort_obs_ids)"
                            )
                        finally:
                            db.unregister("_cohort_obs_ids")
                    elif clean_name in {"award_lot", "award_contractor"} and replaced_result_rids:
                        db.register(
                            "_cand_result_rids",
                            pa.Table.from_arrays(
                                [pa.array(sorted(replaced_result_rids), type=pa.string())],
                                names=["revision_id"],
                            ),
                        )
                        try:
                            db.execute(
                                f"DELETE FROM {table} WHERE revision_id IN (SELECT revision_id FROM _cand_result_rids)"
                            )
                        finally:
                            db.unregister("_cand_result_rids")
                    elif pk and arrow_table is not None and len(arrow_table) > 0:
                        db.register("_incoming_cand", arrow_table)
                        try:
                            db.execute(
                                f"DELETE FROM {table} WHERE {pk} IN (SELECT {pk} FROM _incoming_cand)"
                            )
                        finally:
                            db.unregister("_incoming_cand")
                    elif (
                        clean_name != "contract_revision"
                        and spec
                        and partition_date is not None
                        and (arrow_table is None or len(arrow_table) == 0)
                    ):
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
            has_deletes = False
            physical_count = None
            if curr_snap and "summary" in curr_snap:
                summary = curr_snap["summary"]
                if "total-records" in summary:
                    try:
                        physical_count = int(summary["total-records"])
                    except (ValueError, TypeError):
                        physical_count = None
                try:
                    total_pos_deletes = int(summary.get("total-position-deletes", 0))
                except (ValueError, TypeError):
                    total_pos_deletes = 0
                try:
                    total_del_files = int(summary.get("total-delete-files", 0))
                except (ValueError, TypeError):
                    total_del_files = 0
                try:
                    total_eq_deletes = int(summary.get("total-equality-deletes", 0))
                except (ValueError, TypeError):
                    total_eq_deletes = 0

                if total_pos_deletes > 0 or total_del_files > 0 or total_eq_deletes > 0:
                    has_deletes = True

            if has_deletes or physical_count is None:
                try:
                    actual = db.sql(
                        f"SELECT count(*) FROM {table} AT (VERSION => {snapshot_id})"
                    ).fetchone()[0]
                except Exception:  # noqa: BLE001
                    actual = db.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
            else:
                actual = physical_count

            snap_entry = {"snapshot_id": snapshot_id, "row_count": actual,
                          "table_uuid": metadata.get("table-uuid")}
            if physical_count is not None:
                snap_entry["physical_record_count"] = physical_count
            if change_journal is not None:
                from procurement.silver.application.change_journal import capture_after

                snap_entry["change_tracking"] = capture_after(
                    db, change_journal, clean_ns, clean_name, journal_before, metadata
                )
            snapshots[clean_name] = snap_entry
        elif clean_name in {"award_lot", "award_contractor"}:
            snapshots[clean_name] = {"snapshot_id": None, "row_count": 0,
                                     "table_uuid": metadata.get("table-uuid")}

    # Automatically ensure active vectorized current views in DuckDB
    ensure_views(db, clean_ns)

    return snapshots


VIEW_DEFINITIONS: dict[str, dict[str, str]] = {
    "v_contract_version": {
        "source_table": "contract_revision",
        "partition_by": "entity_id, contract_version",
        "order_by": "observed_at DESC, revision_id DESC",
    },
    "v_contract_performance_version": {
        "source_table": "contract_performance_revision",
        "partition_by": "entity_id, evaluation_version",
        "order_by": "observed_at DESC, revision_id DESC",
    },
    "v_latest_contract": {
        "source_table": "contract_revision",
        "partition_by": "entity_id",
        "order_by": "TRY_CAST(contract_version AS BIGINT) DESC NULLS LAST, observed_at DESC, revision_id DESC",
    },
    "v_latest_contract_performance": {
        "source_table": "contract_performance_revision",
        "partition_by": "entity_id",
        "order_by": "TRY_CAST(evaluation_version AS BIGINT) DESC NULLS LAST, observed_at DESC, revision_id DESC",
    },
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
        "order_by": "public_date DESC NULLS LAST, actual_open_date DESC NULLS LAST",
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
        except Exception:  # noqa: BLE001, S112 — missing tables are normal on first deployment
            continue

        projection = "* EXCLUDE (rn)"
        if view_name in {"v_latest_contract", "v_latest_contract_performance"}:
            projection = "* EXCLUDE (rn, is_current), TRUE AS is_current"
        sql = f"""
        CREATE OR REPLACE VIEW {v_schema}.{view_name} AS
        SELECT {projection}
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {cfg["partition_by"]}
                ORDER BY {cfg["order_by"]}
            ) AS rn
            FROM {src_table}
        ) WHERE rn = 1
        """
        try:
            db.execute(sql)
            created.append(f"{v_schema}.{view_name}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to create view %s.%s: %s", v_schema, view_name, exc)

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


def ensure_release_views(
    db,
    target_namespace: str = "silver_v2",
    release_schema: str = "silver",
) -> list[str]:
    """Mount full logical release views in DuckDB schema 'silver' pointing to physical lake.{target_namespace}.*."""
    clean_ns = identifier(target_namespace)
    v_schema = identifier(release_schema)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {v_schema}")
    created: list[str] = []

    # 1. Mount raw tables
    schemas = get_silver_schemas()
    for clean_name in schemas:
        src_table = f"lake.{clean_ns}.{clean_name}"
        try:
            db.sql(f"SELECT 1 FROM {src_table} LIMIT 0")
            db.execute(
                f"CREATE OR REPLACE VIEW {v_schema}.{clean_name} AS SELECT * FROM {src_table}"
            )
            created.append(f"{v_schema}.{clean_name}")
        except Exception:
            continue

    # 2. Mount deduplicated SIMD views
    latest_views = ensure_views(db, namespace=target_namespace, view_schema=release_schema)
    created.extend(latest_views)
    return created


__all__ = [
    "SCHEMAS",
    "VIEW_DEFINITIONS",
    "ensure_release_views",
    "ensure_silver_tables",
    "ensure_views",
    "get_partition_spec",
    "get_primary_key",
    "read_pinned",
    "stage",
    "stage_partition",
    "to_iceberg_ddl",
    "typed_arrow_rows",
]
