import json
import uuid
from collections.abc import Iterable
from datetime import UTC, date, datetime
from typing import BinaryIO, cast

import s3fs

from procurement.common.errors import ErrorClassification, ErrorStage, safe_error_message
from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.models.errors import ErrorRecord


def build_error_record(
    *,
    identity: ResourceIdentity,
    run_id: str,
    stage: ErrorStage,
    source_date: date,
    page_number: int | None,
    exc: Exception,
    classification: ErrorClassification,
    source_id: str | None = None,
) -> ErrorRecord:
    return ErrorRecord(
        error_id=uuid.uuid4().hex,
        run_id=run_id,
        source=identity.source,
        resource=identity.resource,
        source_date=source_date,
        page_number=page_number,
        stage=stage,
        code=classification.code,
        source_id=source_id,
        error_type=type(exc).__name__,
        message=safe_error_message(exc, classification),
        http_status=classification.http_status,
        occurred_at=datetime.now(UTC),
    )


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
        json.dumps(record.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":"))
        + "\n"
        for record in records
    ).encode("utf-8")
    with fs.open(key, "wb") as raw_file:
        cast(BinaryIO, raw_file).write(content)
    return f"s3://{key}"


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
                records.append(ErrorRecord.model_validate(json.loads(line)))
    return sorted(records, key=lambda item: item.occurred_at, reverse=True)
