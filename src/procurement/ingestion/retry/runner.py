import uuid
from datetime import UTC, date, datetime
from typing import Any

import dlt
import s3fs

from procurement.common.errors import classify_exception, safe_error_message
from procurement.ingestion.engine.models import ResourceSpec
from procurement.storage.bronze import create_bronze_destination, create_bronze_resource
from procurement.storage.error_resolutions import (
    ResolutionStatus,
    read_error_resolution,
    write_error_resolution,
)
from procurement.storage.errors import ErrorRecord, list_error_records
from procurement.storage.retry_manifests import write_retry_manifest


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _base_state(error: ErrorRecord) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "error_id": error["error_id"],
        "source": error["source"],
        "resource": error["resource"],
        "source_date": error["source_date"],
        "original_run_id": error["run_id"],
        "status": ResolutionStatus.PENDING.value,
        "attempts": 0,
        "last_retry_run_id": None,
        "last_attempt_at": None,
        "recovered_at": None,
    }


def retry_resource_errors(
    *,
    fs: s3fs.S3FileSystem,
    spec: ResourceSpec,
    source_date: date,
    original_run_id: str | None = None,
    max_attempts: int = 3,
) -> str:
    """Retry unresolved record-level errors without mutating the original run/error events."""
    if spec.retry_error is None:
        raise RuntimeError(f"Resource {spec.identity.resource} does not support retries")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    retry_run_id = uuid.uuid4().hex
    started_at = _now()
    errors = list_error_records(
        fs,
        spec.identity,
        source_date=source_date,
        run_id=original_run_id,
        retryable=True,
    )
    pipeline = dlt.pipeline(
        pipeline_name=spec.pipeline_name,
        destination=create_bronze_destination(source_partition_date=source_date),
        dataset_name=spec.dataset_name,
    )

    selected = recovered = failed = dead_letter = skipped = 0
    for error in errors:
        error_id = str(error["error_id"])
        state = read_error_resolution(fs, spec.identity, source_date, error_id) or _base_state(error)
        status = ResolutionStatus(str(state.get("status", ResolutionStatus.PENDING.value)))
        attempts = int(state.get("attempts", 0))

        if status in {ResolutionStatus.RECOVERED, ResolutionStatus.DEAD_LETTER}:
            skipped += 1
            continue
        if attempts >= max_attempts:
            state["status"] = ResolutionStatus.DEAD_LETTER.value
            write_error_resolution(fs, spec.identity, source_date, error_id, state)
            dead_letter += 1
            continue

        selected += 1
        attempt_number = attempts + 1
        state.update(
            {
                "status": ResolutionStatus.RETRYING.value,
                "last_retry_run_id": retry_run_id,
                "last_attempt_at": _now(),
            }
        )
        write_error_resolution(fs, spec.identity, source_date, error_id, state)

        try:
            records = spec.retry_records(
                error=error,
                retry_run_id=retry_run_id,
                source_date=source_date,
            )
            resource = create_bronze_resource(records, name=f"{spec.identity.resource}_retry")
            load_info = pipeline.run(resource)
            state.update(
                {
                    "status": ResolutionStatus.RECOVERED.value,
                    "attempts": attempt_number,
                    "recovered_at": _now(),
                    "bronze_load_ids": list(load_info.loads_ids),
                    "last_error_code": None,
                    "last_error_message": None,
                }
            )
            recovered += 1
        except Exception as exc:
            classification = classify_exception(exc)
            final = attempt_number >= max_attempts or not classification.retryable
            state.update(
                {
                    "status": (
                        ResolutionStatus.DEAD_LETTER.value
                        if final
                        else ResolutionStatus.PENDING.value
                    ),
                    "attempts": attempt_number,
                    "last_error_code": classification.code.value,
                    "last_error_message": safe_error_message(exc, classification),
                }
            )
            failed += 1
            if final:
                dead_letter += 1
        write_error_resolution(fs, spec.identity, source_date, error_id, state)

    status = "completed" if failed == 0 else "completed_with_errors"
    write_retry_manifest(
        fs,
        spec.identity,
        source_date,
        retry_run_id,
        {
            "schema_version": 1,
            "retry_run_id": retry_run_id,
            "source": spec.identity.source,
            "resource": spec.identity.resource,
            "source_date": source_date.isoformat(),
            "original_run_id": original_run_id,
            "status": status,
            "started_at": started_at,
            "completed_at": _now(),
            "selected_errors": selected,
            "recovered_errors": recovered,
            "failed_attempts": failed,
            "dead_letter_errors": dead_letter,
            "skipped_resolved_errors": skipped,
        },
    )
    return retry_run_id
