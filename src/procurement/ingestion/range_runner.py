import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from procurement.ingestion.engine.models import DailyResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    @property
    def partition_date(self) -> date:
        return self.start


def split_by_day(start: date, end: date) -> list[DateWindow]:
    """Split an inclusive date range into one source partition per day."""

    if start > end:
        raise ValueError("start must be before or equal to end")
    return [
        DateWindow(
            start=start + timedelta(days=offset),
            end=start + timedelta(days=offset),
        )
        for offset in range((end - start).days + 1)
    ]


def to_api_window(window: DateWindow) -> tuple[str, str]:
    source_date = window.partition_date.isoformat()
    return (
        f"{source_date}T00:00:00.000Z",
        f"{source_date}T23:59:59.999Z",
    )


def run_daily_range(
    start: date,
    end: date,
    *,
    run_day: Callable[[str, date], DailyResult],
    resource: str,
) -> str:
    """Run any daily ingestion resource over an inclusive date range."""

    run_id = uuid.uuid4().hex
    totals = {"days": 0, "skipped_days": 0, "pages": 0, "items": 0, "errors": 0}
    logger.info(
        "crawl_started run_id=%s resource=%s start_date=%s end_date=%s",
        run_id, resource, start, end,
    )
    try:
        for window in split_by_day(start, end):
            result = run_day(run_id, window.partition_date)
            if result["status"] == "skipped":
                totals["skipped_days"] += 1
                continue
            totals["days"] += 1
            totals["pages"] += result["pages"]
            totals["items"] += result["search_items"]
            totals["errors"] += result["errors"]
    except Exception:
        logger.exception(
            "crawl_failed run_id=%s resource=%s completed_days=%s "
            "skipped_days=%s pages=%s search_items=%s errors=%s",
            run_id, resource, totals["days"], totals["skipped_days"],
            totals["pages"], totals["items"], totals["errors"],
        )
        raise
    logger.info(
        "crawl_completed run_id=%s resource=%s days=%s skipped_days=%s "
        "pages=%s search_items=%s errors=%s",
        run_id, resource, totals["days"], totals["skipped_days"],
        totals["pages"], totals["items"], totals["errors"],
    )
    return run_id
