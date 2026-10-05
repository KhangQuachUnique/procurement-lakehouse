from datetime import date
from unittest.mock import Mock

import pytest

from procurement.jobs import ingest, runner


@pytest.mark.parametrize("mode", ["daily", "backfill", "repair"])
@pytest.mark.parametrize("resource", [[], ["--resource", "all"]])
def test_ingestion_requires_one_explicit_resource_before_storage(monkeypatch, mode, resource):
    fs = Mock(side_effect=AssertionError("must not touch storage"))
    monkeypatch.setattr(ingest, "create_s3_filesystem", fs)
    dates = [] if mode == "daily" else ["--year", "2025"]
    monkeypatch.setattr("sys.argv", ["ingest", mode, *dates, *resource])
    with pytest.raises(SystemExit) as exc:
        ingest.main()
    assert exc.value.code == 2
    fs.assert_not_called()


@pytest.mark.parametrize("mode", ["status", "verify"])
def test_read_only_commands_still_accept_all(mode):
    args = ingest._parser().parse_args([mode, "--resource", "all", "--year", "2025"])
    assert ingest.resolve_dates(args, today=date(2026, 10, 4)) == (date(2025, 1, 1), date(2025, 12, 31))


@pytest.mark.parametrize("year,days", [(2025, 365), (2024, 366)])
@pytest.mark.parametrize("mode", ["backfill", "repair", "status", "verify"])
def test_year_selects_whole_closed_year_without_redundant_budget_flag(mode, year, days):
    options = ingest._parser().parse_args([mode, "--resource", "project", "--year", str(year)])
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
        ["backfill", "--year", "2025", "--resource-workers", "0"],
        ["backfill", "--year", "2025", "--resource-workers", "5"],
        ["backfill", "--year", "2025", "--khlcnt-package-workers", "0"],
        ["backfill", "--year", "2025", "--khlcnt-package-workers", "33"],
        ["backfill", "--year", "2025", "--source-max-inflight", "0"],
        ["backfill", "--year", "2025", "--source-max-inflight", "33"],
        ["backfill", "--year", "2025", "--source-request-interval", "-1"],
        ["backfill", "--year", "2025", "--source-request-interval", "nan"],
        ["backfill", "--year", "2025", "--source-request-interval", "inf"],
    ],
)
def test_invalid_cli_input_fails_before_touching_storage(monkeypatch, arguments):
    filesystem = Mock(side_effect=AssertionError("must not access storage"))
    monkeypatch.setattr(ingest, "create_s3_filesystem", filesystem)
    monkeypatch.setattr(ingest, "today_vn", lambda: date(2026, 9, 18))
    monkeypatch.setattr("sys.argv", ["ingest", *arguments, "--resource", "project"])
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


@pytest.mark.parametrize("shared", [False, True])
def test_day_runner_uses_optional_pacing_and_original_retry_defaults(monkeypatch, shared):
    from contextlib import contextmanager

    from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget

    captured = {}
    monkeypatch.setattr(runner.settings, "MUASAMCONG_TOKEN", "fixture")
    monkeypatch.setattr(runner.settings, "MUASAMCONG_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(runner.settings, "MUASAMCONG_REQUEST_INTERVAL_SECONDS", 1.5)
    monkeypatch.setattr(runner, "create_s3_filesystem", object)
    monkeypatch.setattr(runner, "run_batch_range", lambda *_, **__: "run")
    @contextmanager
    def client(**kwargs):
        captured.update(kwargs)
        yield object()
    monkeypatch.setattr(runner, "MuasamcongClient", client)
    budget = RequestBudget(1, min_interval=2) if shared else None
    assert runner.run_resource_day("project", date(2025, 1, 1), request_budget=budget) == "run"
    assert captured["max_attempts"] == 3
    assert "retry_base_delay" not in captured
    assert "shared_retry_cooldown" not in captured
    if shared:
        assert captured["request_budget"] is budget
    else:
        assert captured["request_budget"]._interval == 1.5
