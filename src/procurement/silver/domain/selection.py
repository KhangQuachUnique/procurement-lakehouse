"""Domain hashing and partition selection utilities."""

import hashlib
import json
from datetime import date
from typing import Any

from procurement.common.catalog import get_resource
from procurement.storage.committed import select_committed_days
from procurement.storage.control import read_day_manifest


def digest(value: Any) -> str:
    """Deterministic, sorted SHA-256 digest of any JSON-serializable structure."""
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
        ).encode()
    ).hexdigest()


def file_hash(fs: Any, key: str) -> str:
    """Calculate SHA-256 hash of a storage object."""
    result = hashlib.sha256()
    with fs.open(key, "rb") as stream:
        while block := stream.read(1024 * 1024):
            result.update(block)
    return result.hexdigest()


def freeze(fs: Any, resources: list[str], start: date, end: date) -> list[dict[str, Any]]:
    """Freeze committed Bronze partitions for the requested resource and date range."""
    if start > end or not resources or len(set(resources)) != len(resources):
        raise ValueError("Select a nonempty unique resource set and an ordered date range")
    partitions = []
    for resource in sorted(resources):
        definition = get_resource(resource)
        for day in select_committed_days(fs, definition, start, end):
            manifest = read_day_manifest(fs, definition.identity, day.run_id, day.source_date)
            if manifest is None or manifest.status.value != "success":
                raise ValueError("Selected commit disappeared")
            partitions.append(
                {
                    "resource": resource,
                    "source_date": day.source_date.isoformat(),
                    "run_id": day.run_id,
                    "expected_records": day.expected_records,
                    "manifest": manifest.model_dump(mode="json"),
                    "files": [
                        {"table": table, "key": key, "sha256": file_hash(fs, key)}
                        for table, key in day.files
                    ],
                }
            )
    return partitions


def records(fs: Any, partition: dict[str, Any]):
    """Stream verified records from committed partition files."""
    import json

    import pyarrow.parquet as pq

    from procurement.ingestion.engine.metadata import calculate_content_hash

    count = 0
    for file in partition["files"]:
        if file_hash(fs, file["key"]) != file["sha256"]:
            raise ValueError("Frozen Bronze file changed")
        with fs.open(file["key"], "rb") as stream:
            ordinal = 0
            for batch in pq.ParquetFile(stream).iter_batches(batch_size=1024):
                for row in batch.to_pylist():
                    if (
                        row["run_id"] != partition["run_id"]
                        or str(row["source_date"])[:10] != partition["source_date"]
                    ):
                        raise ValueError("Bronze lineage mismatch")
                    payload = (
                        json.loads(row["payload"])
                        if isinstance(row["payload"], str)
                        else row["payload"]
                    )
                    if calculate_content_hash(payload) != row["content_hash"]:
                        raise ValueError("Bronze content hash mismatch")
                    yield file, ordinal, row
                    ordinal += 1
                    count += 1
    if count != partition["expected_records"]:
        raise ValueError(
            f"Bronze count mismatch: expected {partition['expected_records']}, got {count}"
        )


__all__ = ["digest", "file_hash", "freeze", "records"]
