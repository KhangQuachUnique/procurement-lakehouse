from datetime import date

import pytest

from procurement.jobs.crawl_contractor_result import _validate_closed_range

TODAY_VN = date(2026, 9, 12)


def test_closed_range_accepts_completed_dates() -> None:
    _validate_closed_range(
        date(2026, 9, 1),
        date(2026, 9, 11),
        today=TODAY_VN,
    )


@pytest.mark.parametrize("end", [date(2026, 9, 12), date(2026, 9, 13)])
def test_closed_range_rejects_today_and_future(end: date) -> None:
    with pytest.raises(ValueError, match="closed source dates"):
        _validate_closed_range(date(2026, 9, 1), end, today=TODAY_VN)


def test_closed_range_rejects_reverse_range() -> None:
    with pytest.raises(ValueError, match="start must be"):
        _validate_closed_range(date(2026, 9, 11), date(2026, 9, 10), today=TODAY_VN)
