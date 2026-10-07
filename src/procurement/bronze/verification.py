"""Schema, count, lineage, and hash verification for Bronze records and files."""

import hashlib
import json
from datetime import date
from typing import Any

from procurement.bronze.hashing import calculate_content_hash


def calculate_file_sha256(data_bytes: bytes) -> str:
    """Calculate SHA256 hex digest for file content."""
    return hashlib.sha256(data_bytes).hexdigest()


def verify_record_lineage(
    record: dict[str, Any],
    *,
    expected_source_date: date,
    expected_run_id: str,
) -> None:
    """Verify record belongs to expected source date and attempt run ID."""
    record_date = str(record.get("source_date", ""))[:10]
    if record_date != expected_source_date.isoformat():
        raise ValueError(
            f"Lineage error: record source_date '{record_date}' != expected '{expected_source_date}'"
        )
    if record.get("run_id") != expected_run_id:
        raise ValueError(
            f"Lineage error: record run_id '{record.get('run_id')}' != expected '{expected_run_id}'"
        )


def verify_record_content_hash(record: dict[str, Any]) -> None:
    """Verify that stored content_hash matches calculate_content_hash(payload)."""
    payload = record.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload)
    computed = calculate_content_hash(payload)
    if computed != record.get("content_hash"):
        raise ValueError(
            f"Hash mismatch: computed '{computed}' != record '{record.get('content_hash')}'"
        )
