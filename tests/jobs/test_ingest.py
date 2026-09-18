import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from unittest.mock import Mock

import pytest

from procurement.common.settings import settings
from procurement.ingestion.coverage import CoverageDay
from procurement.jobs import ingest
from procurement.jobs.lock import execution_lock


def args(*extra):
    return ingest._parser().parse_args(
        [
            "repair",
            "--resource",
            "project",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-01",
            *extra,
        ]
    )


def test_daily_range_uses_vietnam_calendar_and_validates_budget():
    options = ingest._parser().parse_args(["daily", "--lookback-days", "3"])
    assert ingest.resolve_dates(options, today=date(2026, 1, 2)) == (
        date(2025, 12, 30),
        date(2026, 1, 1),
    )
    options.max_days = 2
    with pytest.raises(ValueError, match="max-days"):
        ingest.resolve_dates(options, today=date(2026, 1, 2))


@pytest.mark.parametrize(
    "state,age,expected",
    [
        ("running", 0, False),
        ("running", 20, True),
        ("interrupted", 0, True),
    ],
)
def test_running_day_requires_explicit_stale_recovery(monkeypatch, state, age, expected):
    day = CoverageDay(date(2025, 1, 1), None, None, ("active",))
    monkeypatch.setattr(ingest, "read_coverage", lambda *_: [day])
    monkeypatch.setattr(
        ingest,
        "read_execution",
        lambda *_: {
            "state": state,
            "heartbeat_at": (datetime.now(UTC) - timedelta(minutes=age)).isoformat(),
        },
    )
    options = args("--dry-run")
    report, _ = ingest.execute_flow(options, fs=object())
    assert report["resources"][0]["dates"][0]["planned"] is False
    options.retry_stale = True
    report, _ = ingest.execute_flow(options, fs=object())
    assert report["resources"][0]["dates"][0]["planned"] is expected


def test_preflight_failure_makes_no_ingestion_calls(monkeypatch):
    monkeypatch.setattr(ingest, "read_coverage", Mock(side_effect=OSError("storage offline")))
    crawl = Mock()
    with pytest.raises(OSError):
        ingest.execute_flow(args(), fs=object(), run_day=crawl)
    crawl.assert_not_called()


def test_configured_token_is_not_rejected_based_on_its_spelling(monkeypatch):
    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "fake_token")
    monkeypatch.setattr(
        ingest, "read_coverage", lambda *_: [CoverageDay(date(2025, 1, 1), None, None, ())]
    )
    crawl = Mock(side_effect=RuntimeError("stopped after credential gate"))
    report, code = ingest.execute_flow(args(), fs=object(), run_day=crawl)
    crawl.assert_called_once()
    assert code == 1
    assert report["execution_errors"][0]["error"] == "stopped after credential gate"


def test_status_reports_missing_day_as_nonzero(monkeypatch):
    monkeypatch.setattr(
        ingest, "read_coverage", lambda *_: [CoverageDay(date(2025, 1, 1), None, None, ())]
    )
    options = args()
    options.mode = "status"
    report, code = ingest.execute_flow(options, fs=object())
    assert code == 1
    assert report["resources"][0]["dates"][0]["status"] == "no_attempt"


def test_host_lock_blocks_overlap_and_releases_after_exit(tmp_path):
    script = """
import sys
from pathlib import Path
from procurement.jobs.lock import execution_lock
try:
    with execution_lock(Path(sys.argv[1])):
        pass
except RuntimeError:
    raise SystemExit(9)
"""
    with execution_lock(tmp_path):
        result = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path)], timeout=15, check=False
        )
        assert result.returncode == 9
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path)], timeout=15, check=False)
    assert result.returncode == 0
