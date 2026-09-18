"""Shared date validation; API window semantics stay compatible with existing runs."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def today_vn() -> date:
    return datetime.now(VIETNAM_TZ).date()


def validate_closed_range(start: date, end: date, *, today: date) -> None:
    if start > end:
        raise ValueError("start must be before or equal to end")
    if end >= today:
        raise ValueError("Only closed source dates can be crawled")


def validate_page_size(page_size: int) -> None:
    if page_size <= 0:
        raise ValueError("page_size must be greater than zero")


def api_day_window(source_date: date) -> tuple[str, str]:
    # Preserve the existing wire contract until the upstream timezone is verified.
    return (
        f"{source_date.isoformat()}T00:00:00.000Z",
        f"{source_date.isoformat()}T23:59:59.999Z",
    )
