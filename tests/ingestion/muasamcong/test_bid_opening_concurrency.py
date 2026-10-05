import copy
import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from threading import Barrier, Event, Lock

import httpx
import pytest

from procurement.common.cancellation import IngestionInterrupted
from procurement.ingestion.engine.page_runner import run_page
from procurement.ingestion.sources.muasamcong.bid_opening.resource import (
    ENDPOINTS,
    create_bid_opening_spec,
)
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget


def samples(count):
    fixture = json.loads((Path(__file__).with_name("fixtures") / "bid_opening.json").read_text(encoding="utf-8"))
    contexts, payloads = [], {}
    for index in range(count):
        ctx = {"id": str(index), "notifyNo": f"IB{index}", "notifyVersion": "01", "isInternet": 1}
        payload = copy.deepcopy(fixture)
        payload["notify"]["bidNoContractorResponse"]["bidNotification"].update(ctx)
        payload["roundmng"]["bidoBidroundMngViewDTO"].update(ctx)
        for lot in payload["lot_open_detail"]:
            lot["notifyId"] = ctx["id"]
        contexts.append(ctx)
        payloads[ctx["id"]] = payload
    return contexts, payloads


@pytest.mark.parametrize("fail_one", [False, True])
def test_parallel_details_share_budget_and_merge_without_partial_page_write(fail_one):
    contexts, payloads = samples(7)
    gate, lock = Barrier(2), Lock()
    calls = defaultdict(list)
    active = peak = first_requests = 0

    def handle(request):
        nonlocal active, peak, first_requests
        body = json.loads(request.content)
        part = next(k for k, v in ENDPOINTS.items() if request.url.path == v)
        with lock:
            active += 1
            peak = max(peak, active)
            calls[body["notifyId"]].append(part)
            rendezvous = part == "notify" and first_requests < 2
            if part == "notify":
                first_requests += 1
        try:
            if rendezvous:
                gate.wait(timeout=5)
            data = payloads[body["notifyId"]][part]
            if fail_one and body["notifyId"] == "2" and part == "bid_open":
                data = {"bidSubmissionByContractorViewResponse": None}
            return httpx.Response(200, json=data)
        finally:
            with lock:
                active -= 1

    class Writer:
        tables = None
        def write_page(self, tables):
            self.tables = tables
            return sum(map(len, tables.values()))
    writer = Writer()
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle),
                         request_budget=RequestBudget(2)) as client:
        result = run_page(spec=create_bid_opening_spec(client, detail_workers=3), writer=writer,
            search_items=contexts, run_id="parallel", source_date=date(2025, 1, 1), page_number=0)
    assert peak == 2 and active == 0
    assert len(calls) == 7 and all(parts == list(ENDPOINTS) for parts in calls.values())
    assert result.stats.search_items == 7
    assert result.stats.total_records == 7 - int(fail_one)
    assert result.stats.total_errors == int(fail_one)
    assert [o["context"]["id"] for o in result.stats.quality_observations] == [str(i) for i in range(7)]
    if fail_one:
        assert writer.tables is None and result.bronze_records == 0
        assert len(result.errors) == 1 and result.errors[0].source_id == "IB2"
    else:
        rows = writer.tables["bid_opening_detail"]
        assert [r.source_id for r in rows] == [f"IB{i}" for i in range(7)]
        assert [r.payload for r in rows] == list(payloads.values())
        assert not result.errors and result.bronze_records == 7


@pytest.mark.parametrize("interruption", [IngestionInterrupted, KeyboardInterrupt])
def test_interruption_cancels_queued_requests_and_drains_workers(interruption):
    contexts, payloads = samples(8)
    other_started, cancelled = Event(), Event()
    calls = []
    def handle(request):
        body = json.loads(request.content)
        calls.append(body["notifyId"])
        if body["notifyId"] == "0":
            assert other_started.wait(5)
            raise interruption("stop")
        other_started.set()
        assert cancelled.wait(5)
        return httpx.Response(200, json=payloads[body["notifyId"]]["notify"])
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle),
                         request_budget=RequestBudget(2)) as client:
        original_cancel = client.cancel_requests
        def cancel():
            original_cancel()
            cancelled.set()
        client.cancel_requests = cancel
        from procurement.ingestion.engine.stats import PageStats
        with pytest.raises(interruption):
            list(create_bid_opening_spec(client, detail_workers=2).records(
                search_items=contexts, run_id="interrupted", source_date=date(2025, 1, 1),
                search_page=0, errors=[], stats=PageStats()))
    assert cancelled.is_set() and sorted(calls) == ["0", "1"]
