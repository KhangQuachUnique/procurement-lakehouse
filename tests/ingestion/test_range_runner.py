from datetime import date

import pytest

from procurement.ingestion.range_runner import split_by_day, to_api_window


def test_split_by_day_across_month_boundary() -> None:
    windows = split_by_day(date(2026, 8, 31), date(2026, 9, 2))

    assert [(window.start, window.end) for window in windows] == [
        (date(2026, 8, 31), date(2026, 8, 31)),
        (date(2026, 9, 1), date(2026, 9, 1)),
        (date(2026, 9, 2), date(2026, 9, 2)),
    ]


def test_split_by_day_rejects_reverse_range() -> None:
    with pytest.raises(ValueError, match="start must be"):
        split_by_day(date(2026, 9, 2), date(2026, 9, 1))


def test_to_api_window_uses_full_utc_day() -> None:
    window = split_by_day(date(2026, 9, 11), date(2026, 9, 11))[0]

    assert to_api_window(window) == (
        "2026-09-11T00:00:00.000Z",
        "2026-09-11T23:59:59.999Z",
    )
