import json
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
        "schema_version": 1,
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
        "attempts": None,
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
        f"{settings.OBJECT_STORAGE_BUCKET}/_errors/"
        f"{identity.source}/{identity.resource}/"
        f"source_date={source_date.isoformat()}/run_id={run_id}/"
        f"stage={stage.value}/page-{page_number:06d}.jsonl"
    )
    content = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    ).encode("utf-8")

    with fs.open(key, "wb") as raw_file:
        file = cast(BinaryIO, raw_file)
        file.write(content)

    return f"s3://{key}"
