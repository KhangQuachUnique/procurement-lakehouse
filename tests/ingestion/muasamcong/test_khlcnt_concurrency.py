from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier, Event, Lock, get_ident

import httpx
import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.khlcnt import extractor


def collect(client, *, workers=3, plans=("plan-1",), errors=None, stats=None):
    return list(extractor.iter_khlcnt_records(
        client, identity=ResourceIdentity("muasamcong", "khlcnt"),
        search_items=[{"id": plan, "planVersion": "01"} for plan in plans],
        run_id="run", source_date=date(2025, 1, 1), search_page=0,
        errors=errors if errors is not None else [],
        stats=stats if stats is not None else PageStats(), package_workers=workers,
    ))


def test_three_packages_overlap_and_refill_before_slow_package_finishes():
    first_wave = Barrier(3)
    refilled = Event()
    release = Event()
    lock = Lock()
    active = peak = 0
    completed = []
    plan_threads = []
    stats_threads = []

    class Stats(PageStats):
        def record(self, kind):
            stats_threads.append(get_ident())
            super().record(kind)

    class Client:
        def get_plan_detail(self, plan_id):
            plan_threads.append(get_ident())
            if plan_id == "plan-2":
                assert len(completed) == 7
                return {"bidpPlanDetailToProjectList": []}
            return {"bidpPlanDetailToProjectList": [{"id": str(n)} for n in range(7)]}

        def get_bid_package_detail(self, package_id):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                if int(package_id) < 3:
                    first_wave.wait(timeout=5)
                if package_id == "0":
                    assert release.wait(5)
                if package_id == "3":
                    refilled.set()
                return {"id": package_id}
            finally:
                with lock:
                    active -= 1
                    completed.append(package_id)

    stats = Stats()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(collect, Client(), plans=("plan-1", "plan-2"), stats=stats)
        try:
            assert refilled.wait(5), "package 3 should start without waiting for package 0"
            assert not future.done()
        finally:
            release.set()
        records = future.result(timeout=5)
    assert peak == 3
    assert len(records) == 9
    assert len(set(plan_threads + stats_threads)) == 1
    assert stats.record_counts == {"plan": 2, "bid_package": 7}
    assert {item.record.source_id for item in records} == {"plan-1", "plan-2", *map(str, range(7))}
    assert all(item.record.run_id == "run" for item in records)


@pytest.mark.parametrize("workers,count", [(1, 5), (3, 0), (3, 1)])
def test_small_or_disabled_package_pool_runs_inline(monkeypatch, workers, count):
    owner = get_ident()

    def unexpected_pool(**_):
        pytest.fail("no pool needed")

    monkeypatch.setattr(extractor, "ThreadPoolExecutor", unexpected_pool)

    class Client:
        def get_plan_detail(self, _):
            return {"bidpPlanDetailToProjectList": [{"id": str(n)} for n in range(count)]}

        def get_bid_package_detail(self, package_id):
            assert get_ident() == owner
            return {"id": package_id}

    assert len(collect(Client(), workers=workers)) == count + 1


def test_parallel_package_errors_keep_stage_identity_and_counts():
    class Client:
        def get_plan_detail(self, _):
            return {"bidpPlanDetailToProjectList": [
                {"id": "slow"}, {}, {"id": "ok"}, {"id": "missing"},
            ]}

        def get_bid_package_detail(self, package_id):
            if package_id == "slow":
                raise httpx.ReadTimeout("timeout")
            if package_id == "missing":
                raise httpx.HTTPStatusError(
                    "not found", request=httpx.Request("POST", "https://test/detail"),
                    response=httpx.Response(404),
                )
            return {"id": package_id}

    errors, stats = [], PageStats()
    records = collect(Client(), errors=errors, stats=stats)
    assert {item.record.source_id for item in records} == {"plan-1", "ok"}
    assert {error.source_id for error in errors} == {None, "slow", "missing"}
    assert all(error.stage == "bid_package_detail" for error in errors)
    assert stats.record_counts == {"plan": 1, "bid_package": 1}
    assert stats.error_counts == {"bid_package": 3}
