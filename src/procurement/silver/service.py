"""Sequential committed-Bronze to immutable Silver candidate runs; no publication.

Application Service facade delegating to the Hexagonal Silver Application Service.
"""

import sys
from collections.abc import Callable
from datetime import date
from typing import Any

from procurement.silver.adapters.driven.bronze.arrow_adapter import ArrowBronzeRepositoryAdapter
from procurement.silver.adapters.driven.storage.s3_candidate_adapter import S3SilverCandidateAdapter
from procurement.silver.application.process_silver_service import ProcessSilverService
from procurement.silver.domain.selection import (
    freeze as default_freeze,
)
from procurement.silver.domain.selection import (
    records as default_records,
)

freeze = default_freeze
records = default_records


def date_range(day: str | None, start: str | None, end: str | None) -> tuple[date, date]:
    if day and (start or end):
        raise ValueError("Use --date OR --start-date/--end-date, not both")
    if not day and not (start and end):
        raise ValueError("Supply --date or both --start-date and --end-date")
    s_val = day or start
    e_val = day or end
    if s_val is None or e_val is None:
        raise ValueError("Supply --date or both --start-date and --end-date")
    try:
        first, last = date.fromisoformat(s_val), date.fromisoformat(e_val)
    except (ValueError, TypeError) as exc:
        raise ValueError("Dates must use YYYY-MM-DD") from exc
    if first > last:
        raise ValueError("Start date must not be after end date")
    return first, last


def split_date_range_by_year(start: date, end: date) -> list[tuple[date, date]]:
    """Split an inclusive date range into year-bounded sub-ranges."""
    if start > end:
        raise ValueError("Start date must not be after end date")
    if start.year == end.year:
        return [(start, end)]

    ranges = []
    curr_start = start
    for y in range(start.year, end.year + 1):
        year_end = min(date(y, 12, 31), end)
        ranges.append((curr_start, year_end))
        curr_start = date(y + 1, 1, 1)
    return ranges


def merge_run_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple chunk reports into a unified multi-period report."""
    if not reports:
        raise ValueError("Cannot merge empty reports list")
    if len(reports) == 1:
        return reports[0]

    first_rep = reports[0]
    all_days = []
    for r in reports:
        all_days.extend(r.get("days", []))
    all_days.sort(key=lambda d: d.get("date", ""))

    statuses = {d.get("status") for d in all_days}
    if "failed" in statuses:
        overall_status = "failed"
    elif "needs_review" in statuses:
        overall_status = "needs_review"
    else:
        overall_status = "completed"

    return {
        "run_id": first_rep.get("run_id"),
        "resource": first_rep.get("resource"),
        "family": first_rep.get("family", "all"),
        "start_date": str(reports[0].get("start_date")),
        "end_date": str(reports[-1].get("end_date")),
        "status": overall_status,
        "mapping_version": first_rep.get("mapping_version"),
        "days": all_days,
        "output_prefix": first_rep.get("output_prefix"),
        "data_prefix": first_rep.get("data_prefix"),
        "iceberg_enabled": any(r.get("iceberg_enabled") for r in reports),
    }


def run_resource(
    fs,
    bucket: str,
    resource: str,
    start: date,
    end: date,
    *,
    family: str | None = None,
    continue_on_error: bool = False,
    skip_existing: bool = False,
    on_progress: Callable[[dict], None] | None = None,
    db_engine: Any = None,
    iceberg: bool = False,
    iceberg_config: Any = None,
) -> dict:
    """Verify all source families before applying a family filter; report every day."""
    current_module = sys.modules[__name__]
    active_freeze = getattr(current_module, "freeze", default_freeze)
    active_records = getattr(current_module, "records", default_records)

    bronze_repo = ArrowBronzeRepositoryAdapter(
        fs=fs,
        db_engine=db_engine,
        freeze_fn=active_freeze if active_freeze is not default_freeze else None,
        records_fn=active_records if active_records is not default_records else None,
    )
    silver_repo = S3SilverCandidateAdapter(fs=fs)

    resolved_iceberg_config = None
    if iceberg:
        from procurement.common.settings import settings

        resolved_iceberg_config = iceberg_config or settings.iceberg_config()
    elif iceberg_config is not None:
        resolved_iceberg_config = iceberg_config

    use_case = ProcessSilverService(
        bronze_repo=bronze_repo,
        silver_repo=silver_repo,
        bucket=bucket,
        resource_name=resource,
        iceberg_config=resolved_iceberg_config,
    )

    return use_case.execute(
        start_date=start,
        end_date=end,
        family=family,
        continue_on_error=continue_on_error,
        skip_existing=skip_existing,
        on_progress=on_progress,
    )


__all__ = [
    "date_range",
    "freeze",
    "merge_run_reports",
    "records",
    "run_resource",
    "split_date_range_by_year",
]
