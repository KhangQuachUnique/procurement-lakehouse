import hashlib
import json
import uuid
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from typing import Any, BinaryIO, cast

import s3fs

from procurement.common.errors import ErrorClassification, ErrorStage, safe_error_message
from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings

ErrorRecord = dict[str, Any]


def build_error_record(
    *,
    identity: ResourceIdentity,
    run_id: str,
    stage: ErrorStage,
    source_date: date,
    search_page: int,
    exc: Exception,
    classification: ErrorClassification,
    retry_input: Mapping[str, Any],
    source_id: str | None = None,
    parent_source_id: str | None = None,
    source_version: str | None = None,
) -> ErrorRecord:
    return {
        "schema_version": 2,
        "error_id": uuid.uuid4().hex,
        "run_id": run_id,
        "source": identity.source,
        "resource": identity.resource,
        "stage": stage.value,
        "source_date": source_date.isoformat(),
        "search_page": search_page,
        "source_id": source_id,
        "parent_source_id": parent_source_id,
        "source_version": source_version,
        "error_code": classification.code.value,
        "error_type": type(exc).__name__,
        "error_message": safe_error_message(exc, classification),
        "http_status": classification.http_status,
        "retryable": classification.retryable,
        "failed_at": datetime.now(UTC).isoformat(),
        "retry_input": dict(retry_input),
    }


def save_error_records(
    *,
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
    stage: ErrorStage,
    page_number: int,
    records: Iterable[ErrorRecord],
) -> str:
    key = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_errors/{identity.source}/{identity.resource}/"
        f"source_date={source_date.isoformat()}/run_id={run_id}/"
        f"stage={stage.value}/page-{page_number:06d}.jsonl"
    )
    content = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    ).encode("utf-8")
    with fs.open(key, "wb") as raw_file:
        cast(BinaryIO, raw_file).write(content)
    return f"s3://{key}"


def _legacy_error_id(record: Mapping[str, Any], key: str, index: int) -> str:
    canonical = json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return "legacy_" + hashlib.sha256(f"{key}:{index}:{canonical}".encode()).hexdigest()[:32]


def list_error_records(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    source_date: date | None = None,
    run_id: str | None = None,
    retryable: bool | None = None,
) -> list[ErrorRecord]:
    date_part = f"source_date={source_date.isoformat()}" if source_date else "source_date=*"
    run_part = f"run_id={run_id}" if run_id else "run_id=*"
    pattern = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_errors/{identity.source}/{identity.resource}/"
        f"{date_part}/{run_part}/stage=*/page-*.jsonl"
    )
    records: list[ErrorRecord] = []
    for key in fs.glob(pattern):
        with fs.open(key, "rb") as raw_file:
            content = cast(BinaryIO, raw_file).read().decode("utf-8")
        for index, line in enumerate(content.splitlines()):
            if not line.strip():
                continue
            record = cast(ErrorRecord, json.loads(line))
            record.setdefault("error_id", _legacy_error_id(record, key, index))
            if retryable is not None and bool(record.get("retryable")) is not retryable:
                continue
            records.append(record)
    return sorted(records, key=lambda item: str(item.get("failed_at", "")), reverse=True)
