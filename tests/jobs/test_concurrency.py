from datetime import date
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from procurement.jobs import ingest
from procurement.models.control import RunStatus

PLAN = [("bid_opening", date(2025, 1, day)) for day in (1, 2)]


def options(*extra):
    return ingest._parser().parse_args(["backfill", "--resource", "bid_opening", "--year", "2025", *extra])


def report():
    return {"attempted_days": 0, "execution_errors": [], "stopped_early": False}


def test_single_resource_preserves_day_order_and_worker_budget(monkeypatch):
    monkeypatch.setattr(ingest, "read_run_manifest", lambda *_: SimpleNamespace(status=RunStatus.SUCCESS))
    calls, budgets = [], []
    def run_day(resource, day, **kwargs):
        calls.append((resource, day))
        budgets.append(kwargs["request_budget"])
        assert kwargs["bid_opening_detail_workers"] == 5
        return "run"
    result = report()
    ingest._execute_plan(options("--bid-opening-detail-workers", "5"), object(), PLAN, result, run_day)
    assert calls == PLAN
    assert len({id(b) for b in budgets}) == 1
    assert result == {"attempted_days": 2, "execution_errors": [], "stopped_early": False}


def test_mixed_resource_plan_rejected_before_execution():
    run_day = Mock()
    with pytest.raises(ValueError, match="only the selected resource"):
        ingest._execute_plan(options(), object(), [*PLAN, ("project", PLAN[0][1])], report(), run_day)
    run_day.assert_not_called()


@pytest.mark.parametrize("reason", ["authentication_failure", "storage_or_internal_failure",
                                    "unconfirmed_failure", "execution_exception", "source_failure"])
@pytest.mark.parametrize("keep_going", [False, True])
def test_failure_policy_applies_within_selected_resource(monkeypatch, reason, keep_going):
    monkeypatch.setattr(ingest, "read_run_manifest", lambda *_: SimpleNamespace(status=RunStatus.FAILED))
    monkeypatch.setattr(ingest, "classify_day_failure", lambda *_: reason)
    def run_day(*_, **__):
        if reason == "execution_exception":
            raise OSError("unavailable")
        return "failed"
    result = report()
    ingest._execute_plan(options(*(["--continue-on-error"] if keep_going else [])), object(), PLAN, result, run_day)
    expected = 2 if keep_going and reason == "source_failure" else 1
    assert result["attempted_days"] == expected
    assert len(result["execution_errors"]) == expected
    assert result["stopped_early"] is (expected == 1)


def test_interrupt_cancels_requests_before_waiting_for_workers(monkeypatch):
    started = Event()
    cancelled = Event()
    real_cancel = ingest.RequestBudget.cancel

    def cancel(budget):
        real_cancel(budget)
        cancelled.set()

    monkeypatch.setattr(ingest.RequestBudget, "cancel", cancel)

    def interrupted_wait(*_, **__):
        assert started.wait(5)
        raise KeyboardInterrupt

    monkeypatch.setattr(ingest, "wait", interrupted_wait)

    def run_day(*_, request_budget, **__):
        started.set()
        assert cancelled.wait(5)
        with request_budget.request():
            pytest.fail("cancelled attempt must not issue another request")

    with pytest.raises(KeyboardInterrupt):
        ingest._execute_plan(options(), object(), PLAN, report(), run_day)
    assert cancelled.is_set()
