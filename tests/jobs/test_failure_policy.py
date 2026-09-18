from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import CoverageDay
from procurement.jobs import failures, ingest
from procurement.models.control import DayStatus, RunStatus


@pytest.mark.parametrize(
    "day_status,stage,http_status,count,expected",
    [
        (DayStatus.FAILED, "plan_detail", 404, 1, "source_failure"),
        (DayStatus.FAILED, "search_page", 503, 1, "source_failure"),
        (DayStatus.FAILED, "pagination", None, 1, "source_failure"),
        (DayStatus.FAILED, "plan_detail", 401, 1, "authentication_failure"),
        (DayStatus.FAILED, "search_page", 403, 1, "authentication_failure"),
        (DayStatus.FAILED, "bronze_load", None, 1, "storage_or_internal_failure"),
        (DayStatus.FAILED, "internal", None, 1, "storage_or_internal_failure"),
        (DayStatus.FAILED, None, None, 1, "unconfirmed_failure"),
        (DayStatus.FAILED, "plan_detail", 404, 2, "unconfirmed_failure"),
        (DayStatus.SUCCESS, None, None, 0, "unconfirmed_failure"),
        (DayStatus.RUNNING, None, None, 0, "unconfirmed_failure"),
        (None, None, None, 0, "unconfirmed_failure"),
    ],
)
def test_continue_requires_confirmed_source_failure(
    monkeypatch, day_status, stage, http_status, count, expected
):
    day = SimpleNamespace(status=day_status, error_count=count) if day_status else None
    monkeypatch.setattr(failures, "read_day_manifest", lambda *_: day)
    errors = [SimpleNamespace(stage=stage, http_status=http_status)] if stage else []
    monkeypatch.setattr(failures, "list_error_records", lambda *_, **__: errors)
    assert (
        failures.classify_day_failure(
            object(), get_resource("project").identity, "run", date(2025, 1, 1)
        )
        == expected
    )


@pytest.mark.parametrize(
    "outcome", ["exception", "missing_manifest", "running", "missing_errors", "load_error"]
)
def test_flow_stops_on_uncertain_or_storage_failure_even_with_continue(monkeypatch, outcome):
    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "test")
    monkeypatch.setattr(
        ingest,
        "read_coverage",
        lambda *_: [CoverageDay(date(2025, 1, n), None, None, ()) for n in (1, 2)],
    )
    run_day = Mock(return_value="run")
    if outcome == "exception":
        run_day.side_effect = OSError("storage offline")
    status = RunStatus.RUNNING if outcome == "running" else RunStatus.FAILED
    monkeypatch.setattr(
        ingest,
        "read_run_manifest",
        lambda *_: None if outcome == "missing_manifest" else SimpleNamespace(status=status),
    )
    if outcome == "missing_errors":
        monkeypatch.setattr(
            ingest, "classify_day_failure", Mock(side_effect=OSError("cannot read errors"))
        )
    else:
        monkeypatch.setattr(
            ingest, "classify_day_failure", lambda *_: "storage_or_internal_failure"
        )
    options = ingest._parser().parse_args(
        [
            "backfill",
            "--resource",
            "project",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-02",
            "--continue-on-error",
        ]
    )
    report, code = ingest.execute_flow(options, fs=object(), run_day=run_day)
    assert code == 1
    assert report["stopped_early"]
    assert report["attempted_days"] == 1
    run_day.assert_called_once()
