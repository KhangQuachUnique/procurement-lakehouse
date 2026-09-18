"""Decide whether another day may run from persisted evidence of a failed attempt."""

from datetime import date

from procurement.common.resources import ResourceIdentity
from procurement.models.control import DayStatus
from procurement.storage.control import read_day_manifest
from procurement.storage.errors import list_error_records


def classify_day_failure(fs, identity: ResourceIdentity, run_id: str, source_date: date) -> str:
    day = read_day_manifest(fs, identity, run_id, source_date)
    if day is None or day.status is not DayStatus.FAILED:
        return "unconfirmed_failure"
    errors = list_error_records(fs, identity, run_id=run_id, source_date=source_date)
    if any(error.http_status in {401, 403} for error in errors):
        return "authentication_failure"
    if any(error.stage in {"bronze_load", "internal"} for error in errors):
        return "storage_or_internal_failure"
    if not errors or len(errors) != day.error_count:
        return "unconfirmed_failure"
    return "source_failure"
