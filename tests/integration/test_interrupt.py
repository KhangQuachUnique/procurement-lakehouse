from datetime import date
from threading import Barrier, Event

import httpx
import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.jobs import ingest, runner
from procurement.models.control import DayStatus, RunStatus
from procurement.storage.control import list_day_manifests, list_run_manifests
from procurement.storage.errors import list_error_records
from procurement.storage.execution import read_execution

pytestmark = pytest.mark.integration


def test_interrupt_drains_parallel_workers_and_persists_terminal_manifests(store, monkeypatch):
    fs, _ = store
    started = Barrier(3)
    cancelled = Event()
    real_cancel = ingest.RequestBudget.cancel

    def cancel(budget):
        real_cancel(budget)
        cancelled.set()

    def interrupt(*_, **__):
        started.wait(timeout=10)
        raise KeyboardInterrupt

    def source(request):
        started.wait(timeout=10)
        assert cancelled.wait(10)
        return httpx.Response(200, json={"page": {
            "content": [{"id": "one"}], "totalElements": 1,
        }})

    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "fixture")
    monkeypatch.setattr(ingest.RequestBudget, "cancel", cancel)
    monkeypatch.setattr(ingest, "wait", interrupt)
    monkeypatch.setattr(runner, "create_s3_filesystem", lambda: fs)
    monkeypatch.setattr(runner, "MuasamcongClient", lambda **kwargs: MuasamcongClient(
        **kwargs, transport=httpx.MockTransport(source),
    ))
    args = ingest._parser().parse_args(["backfill", "--year", "2025", "--resource-workers", "2"])
    plan = [(name, date(2025, 1, 1)) for name in ("project", "khlcnt")]
    report = {"attempted_days": 0, "execution_errors": []}
    with pytest.raises(KeyboardInterrupt):
        ingest._execute_plan(args, fs, plan, report, runner.run_resource_day)
    for name, _ in plan:
        identity = get_resource(name).identity
        runs = list_run_manifests(fs, identity)
        assert len(runs) == 1
        assert runs[0].status is RunStatus.FAILED
        days = list_day_manifests(fs, identity, run_id=runs[0].run_id)
        assert days[0].status is DayStatus.FAILED
        assert days[0].completed_at is not None
        assert read_execution(fs, identity, runs[0].run_id)["state"] == "interrupted"
        assert list_error_records(fs, identity, run_id=runs[0].run_id)[0].stage == "interrupted"
