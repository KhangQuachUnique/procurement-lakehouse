from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier, Event, Lock
from types import SimpleNamespace

import pytest

from procurement.jobs import ingest
from procurement.models.control import RunStatus

RESOURCES = ("project", "khlcnt", "notify_contractor", "contractor_result")
PLAN = [(resource, date(2025, 1, day)) for resource in RESOURCES for day in (1, 2)]


def options(*extra):
    return ingest._parser().parse_args(["backfill", "--year", "2025", *extra])


def report():
    return {"attempted_days": 0, "execution_errors": [], "stopped_early": False}


def test_resource_overlap_is_bounded_and_days_of_each_resource_never_overlap(monkeypatch):
    gate = Barrier(2)
    lock = Lock()
    active = set()
    calls, budgets = [], []
    peak = 0
    monkeypatch.setattr(
        ingest, "read_run_manifest", lambda *_: SimpleNamespace(status=RunStatus.SUCCESS),
    )

    def run_day(resource, source_date, **kwargs):
        nonlocal peak
        with lock:
            assert resource not in active
            active.add(resource)
            peak = max(peak, len(active))
            calls.append((resource, source_date))
            budgets.append(kwargs["request_budget"])
            assert kwargs["khlcnt_package_workers"] == 3
        try:
            if resource in RESOURCES[:2] and source_date.day == 1:
                gate.wait(timeout=5)
            return "run"
        finally:
            with lock:
                active.remove(resource)

    result = report()
    ingest._execute_plan(options(), object(), PLAN, result, run_day)
    assert peak == 2
    assert {resource for resource, _ in calls[:2]} == {"project", "khlcnt"}
    assert len({id(budget) for budget in budgets}) == 1
    for resource in RESOURCES:
        assert [day.day for name, day in calls if name == resource] == [1, 2]
    assert result == {"attempted_days": 8, "execution_errors": [], "stopped_early": False}


@pytest.mark.parametrize("reason", [
    "authentication_failure", "storage_or_internal_failure", "unconfirmed_failure",
    "execution_exception", "source_failure",
])
def test_fatal_failure_stops_new_work_and_drains_other_started_day(monkeypatch, reason):
    both_started = Barrier(2)
    reported = Event()
    release_other = Event()
    calls = []
    monkeypatch.setattr(
        ingest, "read_run_manifest",
        lambda _, identity, __: SimpleNamespace(
            status=RunStatus.FAILED if identity.resource == "project" else RunStatus.SUCCESS,
        ),
    )
    monkeypatch.setattr(ingest, "classify_day_failure", lambda *_: reason)

    class Errors(list):
        def append(self, value):
            super().append(value)
            reported.set()

    def run_day(resource, source_date, **_):
        calls.append((resource, source_date))
        both_started.wait(timeout=5)
        if resource == "project":
            if reason == "execution_exception":
                raise OSError("storage offline")
            return "failed"
        assert release_other.wait(5)
        return "success"

    result = report()
    result["execution_errors"] = Errors()
    args = options(*([] if reason == "source_failure" else ["--continue-on-error"]))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(ingest._execute_plan, args, object(), PLAN, result, run_day)
        try:
            assert reported.wait(5)
            assert len(calls) == 2
            assert not future.done(), "must drain the active attempt before releasing the flow lock"
        finally:
            release_other.set()
        future.result(timeout=5)
    assert result["attempted_days"] == 2
    assert result["stopped_early"]
    assert [error["reason"] for error in result["execution_errors"]] == [reason]


def test_continue_on_source_failure_runs_remaining_days_and_resources(monkeypatch):
    monkeypatch.setattr(
        ingest, "read_run_manifest", lambda *_: SimpleNamespace(status=RunStatus.FAILED),
    )
    monkeypatch.setattr(ingest, "classify_day_failure", lambda *_: "source_failure")
    result = report()
    ingest._execute_plan(
        options("--continue-on-error"), object(), PLAN, result, lambda *_, **__: "run",
    )
    assert result["attempted_days"] == 8
    assert len(result["execution_errors"]) == 8
    assert not result["stopped_early"]


def test_one_worker_rotates_resources_and_never_overlaps(monkeypatch):
    monkeypatch.setattr(
        ingest, "read_run_manifest", lambda *_: SimpleNamespace(status=RunStatus.SUCCESS),
    )
    calls = []

    def run_day(resource, day, **_):
        calls.append((resource, day))
        return "run"

    result = report()
    ingest._execute_plan(options("--resource-workers", "1"), object(), PLAN, result, run_day)
    assert calls == [(resource, date(2025, 1, day)) for day in (1, 2) for resource in RESOURCES]


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
        ingest._execute_plan(options("--resource-workers", "1"), object(), PLAN, report(), run_day)
    assert cancelled.is_set()
