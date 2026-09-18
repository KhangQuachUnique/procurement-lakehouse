from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock

import httpx
import pytest

from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget


def test_request_limit_is_shared_across_clients_and_releases_after_transport_failure():
    budget = RequestBudget(3)
    first_wave = Barrier(3)
    release = Event()
    full = Event()
    lock = Lock()
    active = peak = calls = 0

    def handler(request):
        nonlocal active, peak, calls
        with lock:
            active += 1
            calls += 1
            number = calls
            peak = max(peak, active)
        try:
            if number <= 3:
                first_wave.wait(timeout=5)
                full.set()
                assert release.wait(5)
            if number == 1:
                raise httpx.ReadTimeout("fixture timeout", request=request)
            return httpx.Response(200, json={"ok": True})
        finally:
            with lock:
                active -= 1

    with (
        MuasamcongClient(
            token="test", request_budget=budget, transport=httpx.MockTransport(handler),
            sleep=lambda _: None,
        ) as first,
        MuasamcongClient(
            token="test", request_budget=budget, transport=httpx.MockTransport(handler),
            sleep=lambda _: None,
        ) as second,
        ThreadPoolExecutor(max_workers=8) as pool,
    ):
        futures = [pool.submit((first if n % 2 else second).post, "/detail", {}) for n in range(8)]
        try:
            assert full.wait(5)
            with lock:
                assert active == calls == 3
        finally:
            release.set()
        assert all(future.result(timeout=5) == {"ok": True} for future in futures)
    assert peak == 3
    assert calls == 9  # one retry also went through the shared budget


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failure_blocks_other_clients_and_queued_requests(status):
    budget = RequestBudget(1)
    entered = Event()
    release = Event()
    calls = []

    def handler(request):
        calls.append(request)
        entered.set()
        assert release.wait(5)
        return httpx.Response(status)

    with (
        MuasamcongClient(
            token="test", request_budget=budget, transport=httpx.MockTransport(handler),
        ) as first,
        MuasamcongClient(
            token="test", request_budget=budget, transport=httpx.MockTransport(handler),
        ) as second,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        a = pool.submit(first.post, "/detail", {})
        try:
            assert entered.wait(5)
            b = pool.submit(second.post, "/search", {})
        finally:
            release.set()
        for future in (a, b):
            with pytest.raises(httpx.HTTPStatusError) as exc:
                future.result(timeout=5)
            assert exc.value.response.status_code == status
    assert len(calls) == 1


def test_backoff_releases_request_slot_for_another_resource():
    budget = RequestBudget(1)
    calls = 0
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        MuasamcongClient(
            token="test", request_budget=budget,
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json={})),
        ) as other,
    ):
        def handler(request):
            nonlocal calls
            calls += 1
            return httpx.Response(503) if calls == 1 else httpx.Response(200, json={})

        def sleep(_):
            # A slot held during backoff would prevent this other resource from progressing.
            assert pool.submit(other.post, "/detail", {}).result(timeout=3) == {}

        with MuasamcongClient(
            token="test", request_budget=budget, transport=httpx.MockTransport(handler), sleep=sleep,
        ) as client:
            assert client.post("/search", {}) == {}
    assert calls == 2


def test_cancelled_budget_prevents_new_network_calls():
    budget = RequestBudget(3)
    budget.cancel()

    def handler(_):
        pytest.fail("cancelled flow must not call the source")

    with MuasamcongClient(
        token="test", request_budget=budget, transport=httpx.MockTransport(handler),
    ) as client, pytest.raises(RuntimeError, match="cancelled"):
        client.post("/detail", {})
