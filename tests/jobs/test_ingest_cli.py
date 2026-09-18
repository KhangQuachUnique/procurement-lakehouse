from datetime import date
from unittest.mock import Mock

import pytest

from procurement.jobs import ingest, runner


@pytest.mark.parametrize("year,days", [(2025, 365), (2024, 366)])
@pytest.mark.parametrize("mode", ["backfill", "repair", "status", "verify"])
def test_year_selects_whole_closed_year_without_redundant_budget_flag(mode, year, days):
    options = ingest._parser().parse_args([mode, "--year", str(year)])
    start, end = ingest.resolve_dates(options, today=date(2026, 9, 18))
    assert start == date(year, 1, 1)
    assert end == date(year, 12, 31)
    assert (end - start).days + 1 == days


@pytest.mark.parametrize(
    "arguments",
    [
        ["backfill", "--year", "2026"],
        ["backfill", "--year", "2027"],
        ["backfill", "--year", "0"],
        ["backfill", "--year", "2025", "--start-date", "2025-01-01"],
        ["backfill", "--year", "2025", "--end-date", "2025-12-31"],
        ["backfill", "--year", "2025", "--max-days", "31"],
        ["daily", "--year", "2025"],
        ["backfill", "--start-date", "2025-01-01"],
        ["backfill", "--start-date", "2025-02-01", "--end-date", "2025-01-01"],
        ["backfill", "--start-date", "2025-01-01", "--end-date", "2025-12-31"],
        ["backfill", "--start-date", "2026-09-18", "--end-date", "2026-09-18"],
        ["status", "--year", "2025", "--continue-on-error"],
        ["verify", "--year", "2025", "--continue-on-error"],
        ["backfill", "--year", "2025", "--page-size", "0"],
    ],
)
def test_invalid_cli_input_fails_before_touching_storage(monkeypatch, arguments):
    filesystem = Mock(side_effect=AssertionError("must not access storage"))
    monkeypatch.setattr(ingest, "create_s3_filesystem", filesystem)
    monkeypatch.setattr(ingest, "today_vn", lambda: date(2026, 9, 18))
    monkeypatch.setattr("sys.argv", ["ingest", *arguments])
    with pytest.raises(SystemExit) as exc:
        ingest.main()
    assert exc.value.code == 2
    filesystem.assert_not_called()


@pytest.mark.parametrize("code", [0, 1])
def test_cli_preserves_flow_exit_code(monkeypatch, code, capsys):
    monkeypatch.setattr("sys.argv", ["ingest", "status", "--year", "2025"])
    monkeypatch.setattr(ingest, "execute_flow", lambda _: ({"mode": "status"}, code))
    with pytest.raises(SystemExit) as exc:
        ingest.main()
    assert exc.value.code == code
    assert '"mode": "status"' in capsys.readouterr().out


def test_day_runner_validates_before_storage(monkeypatch):
    filesystem = Mock()
    monkeypatch.setattr(runner, "create_s3_filesystem", filesystem)
    with pytest.raises(ValueError, match="page_size"):
        runner.run_resource_day("project", date(2025, 1, 1), page_size=0)
    filesystem.assert_not_called()
