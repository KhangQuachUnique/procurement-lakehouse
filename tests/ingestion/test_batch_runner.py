from datetime import date
from typing import Any

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion import batch_runner
from procurement.ingestion.batch_runner import split_by_day, to_api_window
from procurement.models.control import RunStatus

IDENTITY = ResourceIdentity("muasamcong", "khlcnt")


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


def test_to_api_window_uses_closed_utc_day() -> None:
    window = split_by_day(date(2026, 9, 11), date(2026, 9, 11))[0]
    assert to_api_window(window) == (
        "2026-09-11T00:00:00.000Z",
        "2026-09-11T23:59:59.999Z",
    )


def test_range_continues_after_failed_day_and_finishes_partial(monkeypatch) -> None:
    manifests = []
    calls = []
    monkeypatch.setattr(
        batch_runner,
        "write_run_manifest",
        lambda _fs, _identity, manifest: manifests.append(manifest) or "s3://run",
    )

    def run_day(_run_id: str, source_date: date) -> dict[str, Any]:
        calls.append(source_date)
        if source_date == date(2026, 9, 2):
            raise RuntimeError("boom")
        return {
            "status": "success",
            "pages": 1,
            "search_items": 1,
            "bronze_records": 1,
            "errors": 0,
        }

    batch_runner.run_batch_range(
        date(2026, 9, 1),
        date(2026, 9, 3),
        fs=object(),  # type: ignore[arg-type]
        identity=IDENTITY,
        run_day=run_day,
    )

    assert calls == [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)]
    assert manifests[-1].status is RunStatus.PARTIAL_FAILED
    assert manifests[-1].success_dates == 2
    assert manifests[-1].failed_dates == 1
