import json
from collections.abc import Iterable
from datetime import date
from typing import Any, BinaryIO, cast

import s3fs

from procurement.common.errors import build_error_record  # noqa: F401 -- compatibility export
from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.models.errors import ErrorRecord


def save_error_records(
    *,
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
    page_number: int,
    records: Iterable[ErrorRecord],
) -> str:
    key = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_errors/{identity.source}/{identity.resource}/"
        f"run_id={run_id}/source_date={source_date.isoformat()}/"
        f"page-{page_number:06d}.jsonl"
    )
    content = "".join(
        json.dumps(record.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    ).encode("utf-8")
    with fs.open(key, "wb") as raw_file:
        cast(BinaryIO, raw_file).write(content)
    return f"s3://{key}"


def _normalize_error_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Read schema-v1 error files without keeping the removed error taxonomy."""

    normalized = dict(payload)
    normalized.pop("code", None)
    normalized.pop("retryable", None)
    normalized.pop("retry_input", None)
    normalized["schema_version"] = 2
    return normalized


def list_error_records(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    source_date: date | None = None,
    run_id: str | None = None,
) -> list[ErrorRecord]:
    run_part = f"run_id={run_id}" if run_id else "run_id=*"
    date_part = f"source_date={source_date.isoformat()}" if source_date else "source_date=*"
    pattern = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_errors/{identity.source}/{identity.resource}/"
        f"{run_part}/{date_part}/page-*.jsonl"
    )
    records: list[ErrorRecord] = []
    for key in fs.glob(pattern):
        with fs.open(key, "rb") as raw_file:
            content = cast(BinaryIO, raw_file).read().decode("utf-8")
        for line in content.splitlines():
            if line.strip():
                payload = _normalize_error_payload(json.loads(line))
                records.append(ErrorRecord.model_validate(payload))
    return sorted(records, key=lambda item: item.occurred_at, reverse=True)
