"""Bounded Parquet rewriting and exact multiset verification for daily compaction."""

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from procurement.ingestion.engine.metadata import calculate_content_hash

CHUNK_BYTES = 1024 * 1024
BATCH_ROWS = 1024


def copy_file(source, target):
    digest, size = hashlib.sha256(), 0
    while chunk := source.read(CHUNK_BYTES):
        target.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    return {"sha256": digest.hexdigest(), "size": size}


def digest_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        while chunk := file.read(CHUNK_BYTES):
            digest.update(chunk)
    return {"sha256": digest.hexdigest(), "size": Path(path).stat().st_size}


def _schema(paths):
    schemas = [pq.ParquetFile(path).schema_arrow for path in paths]
    # Default promotion only permits null -> concrete type, never lossy numeric/text casts.
    schema = pa.unify_schemas(schemas)
    for field in schema:
        if not field.nullable and any(field.name not in item.names for item in schemas):
            raise ValueError(f"Missing non-nullable column: {field.name}")
    for name in ("run_id", "source_date", "source_id", "source_version", "ingested_at",
                 "content_hash", "payload"):
        if name not in schema.names:
            raise ValueError(f"Missing Bronze column: {name}")
    if not pa.types.is_string(schema.field("run_id").type):
        raise ValueError("Bronze run_id must be a string")
    return schema


def rewrite_table(paths, directory, *, baseline_run_id, run_id, source_date, target_bytes):
    """Preserve every cell except run_id. Rotate after a compressed row group reaches target."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    schema = _schema(paths)
    writer = sink = None
    outputs, rows = [], 0
    try:
        for path in paths:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
                for record in batch.to_pylist():
                    if (record["run_id"] != baseline_run_id
                            or str(record["source_date"])[:10] != source_date.isoformat()):
                        raise ValueError("Source Bronze lineage mismatch")
                    payload = record["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if calculate_content_hash(payload) != record["content_hash"]:
                        raise ValueError("Source Bronze content hash mismatch")
                arrays = []
                for field in schema:
                    if field.name == "run_id":
                        column = pa.array([run_id] * batch.num_rows, type=field.type)
                    elif field.name in batch.schema.names:
                        column = batch.column(batch.schema.get_field_index(field.name)).cast(field.type, safe=True)
                    else:
                        column = pa.nulls(batch.num_rows, type=field.type)
                    arrays.append(column)
                if writer is None:
                    output = directory / f"compact-{len(outputs):06d}.parquet"
                    outputs.append(output)
                    sink = output.open("wb")
                    writer = pq.ParquetWriter(sink, schema, compression="zstd", write_statistics=True)
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
                rows += batch.num_rows
                if sink.tell() >= target_bytes:
                    writer.close()
                    sink.close()
                    writer = sink = None
    finally:
        if writer is not None:
            writer.close()
        if sink is not None:
            sink.close()
    if not rows and not outputs:
        # Preserve an existing empty table's schema if other tables make the day worthwhile.
        output = directory / "compact-000000.parquet"
        pq.write_table(pa.Table.from_batches([], schema=schema), output, compression="zstd")
        outputs.append(output)
    return outputs, rows


def verify_multiset(inputs, outputs, scratch):
    """EXCEPT ALL retains duplicates; SQL compares the unchanged columns by name."""
    scratch = Path(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute("SET memory_limit='1GB'")
        con.execute("SET threads=2")
        con.execute("SET temp_directory=?", [str(scratch)])
        before = con.read_parquet([str(p) for p in inputs], hive_partitioning=False, union_by_name=True)
        after = con.read_parquet([str(p) for p in outputs], hive_partitioning=False, union_by_name=True)
        before.create_view("before_rows")
        after.create_view("after_rows")
        if set(before.columns) != set(after.columns):
            raise ValueError("Compacted columns changed")
        columns = ", ".join('"' + c.replace('"', '""') + '"' for c in before.columns if c != "run_id")
        for left, right in (("before_rows", "after_rows"), ("after_rows", "before_rows")):
            result = con.execute(
                f"SELECT 1 FROM (SELECT {columns} FROM {left} EXCEPT ALL "
                f"SELECT {columns} FROM {right}) AS difference LIMIT 1"
            ).fetchone()
            if result is not None:
                raise ValueError("Compacted row multiset changed")
