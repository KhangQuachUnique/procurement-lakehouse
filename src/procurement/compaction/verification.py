"""Verification utilities for compacted Bronze data."""

import hashlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_BYTES = 1024 * 1024


def copy_file(source: Any, target: Any) -> dict[str, Any]:
    """Copy file while computing SHA-256 and size."""
    digest, size = hashlib.sha256(), 0
    while chunk := source.read(CHUNK_BYTES):
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        target.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    return {"sha256": digest.hexdigest(), "size": size}


def digest_file(path: str | Path) -> dict[str, Any]:
    """Compute SHA-256 and size for a local file."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        while chunk := file.read(CHUNK_BYTES):
            digest.update(chunk)
    return {"sha256": digest.hexdigest(), "size": Path(path).stat().st_size}


def verify_multiset(
    inputs: Iterable[str | Path],
    outputs: Iterable[str | Path],
    scratch: str | Path,
) -> None:
    """EXCEPT ALL retains duplicates; SQL compares the unchanged columns by name."""
    scratch_path = Path(scratch)
    scratch_path.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute("SET memory_limit='1GB'")
        con.execute("SET threads=2")
        con.execute("SET temp_directory=?", [str(scratch_path)])
        before = con.read_parquet(
            [str(p) for p in inputs], hive_partitioning=False, union_by_name=True
        )
        after = con.read_parquet(
            [str(p) for p in outputs], hive_partitioning=False, union_by_name=True
        )
        before.create_view("before_rows")
        after.create_view("after_rows")
        if set(before.columns) != set(after.columns):
            raise ValueError("Compacted columns changed")
        columns = ", ".join(
            '"' + c.replace('"', '""') + '"' for c in before.columns if c != "run_id"
        )
        for left, right in (("before_rows", "after_rows"), ("after_rows", "before_rows")):
            result = con.execute(
                f"SELECT 1 FROM (SELECT {columns} FROM {left} EXCEPT ALL "
                f"SELECT {columns} FROM {right}) AS difference LIMIT 1"
            ).fetchone()
            if result is not None:
                raise ValueError("Compacted row multiset changed")


def verify_schema(paths: Iterable[str | Path]) -> pa.Schema:
    """Validate and unify schemas across paths."""
    schemas = [pq.ParquetFile(path).schema_arrow for path in paths]
    schema = pa.unify_schemas(schemas)
    for field in schema:
        if not field.nullable and any(field.name not in item.names for item in schemas):
            raise ValueError(f"Missing non-nullable column: {field.name}")
    for name in (
        "run_id",
        "source_date",
        "source_id",
        "source_version",
        "ingested_at",
        "content_hash",
        "payload",
    ):
        if name not in schema.names:
            raise ValueError(f"Missing Bronze column: {name}")
    if not pa.types.is_string(schema.field("run_id").type):
        raise ValueError("Bronze run_id must be a string")
    return schema
