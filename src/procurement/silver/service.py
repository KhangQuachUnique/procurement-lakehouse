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


__all__ = ["date_range", "freeze", "records", "run_resource"]
