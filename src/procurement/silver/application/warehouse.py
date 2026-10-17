"""Typed Iceberg and DuckDB writes and Silver Lakehouse schemas."""

from typing import Any

import pyarrow as pa

from procurement.silver.domain.schema import get_silver_schemas
from procurement.storage.iceberg import identifier

SCHEMAS = get_silver_schemas()


def typed_arrow_rows(name: str, rows: list[dict[str, Any]]) -> pa.Table:
    """Validate and cast rows against the SQL contract before writing local Parquet."""
    import duckdb

    clean_name = identifier(name)
    with duckdb.connect(":memory:") as db:
        db.execute(f"CREATE TABLE typed_rows ({SCHEMAS[clean_name]})")
        if rows:
            db.register("input_rows", pa.Table.from_pylist(rows))
            db.execute("INSERT INTO typed_rows BY NAME SELECT * FROM input_rows")
        return db.execute("SELECT * FROM typed_rows").to_arrow_table()


def stage(db, catalog, namespace, tables, release_id, schemas=None):
    """Stage candidate tables into local Iceberg lake."""
    schemas = schemas or get_silver_schemas()
    clean_ns = identifier(namespace)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{clean_ns}")
    snapshots = {}
    for name, rows in tables.items():
        clean_name = identifier(name)
        schema = schemas.get(clean_name)
        if schema is None:
            continue
        table = f"lake.{clean_ns}.{clean_name}"
        db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")
        metadata = catalog.table(clean_ns, clean_name)["metadata"]
        if metadata["format-version"] != 2:
            raise ValueError("Only verified Iceberg format v2 is supported")
        db.execute("BEGIN")
        try:
            db.execute(f"DELETE FROM {table}")
            if rows:
                db.register("candidate_rows", pa.Table.from_pylist(rows))
                try:
                    db.execute(f"INSERT INTO {table} BY NAME SELECT * FROM candidate_rows")
                finally:
                    db.unregister("candidate_rows")
            db.execute("COMMIT")
        except BaseException:
            import contextlib

            with contextlib.suppress(Exception):
                db.execute("ROLLBACK")
            raise
        metadata = catalog.table(clean_ns, clean_name)["metadata"]
        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id is not None and snapshot_id >= 0:
            catalog.pin(clean_ns, clean_name, snapshot_id, release_id)
            actual = db.sql(
                f"SELECT count(*) FROM {table} AT (VERSION => {snapshot_id})"
            ).fetchone()[0]
            snapshots[clean_name] = {"snapshot_id": snapshot_id, "row_count": actual}
    return snapshots


def read_pinned(db, release: dict[str, Any], name: str):
    """Read a snapshot pinned to a release manifest."""
    clean_ns = identifier(release["namespace"])
    clean_name = identifier(name)
    snapshot_id = release["tables"][clean_name]["snapshot_id"]
    return db.sql(
        f"SELECT * FROM lake.{clean_ns}.{clean_name} AT (VERSION => {snapshot_id})"
    ).to_arrow_table()


__all__ = ["SCHEMAS", "read_pinned", "stage", "typed_arrow_rows"]
