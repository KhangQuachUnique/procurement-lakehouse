import copy
import json
from datetime import date
from itertools import pairwise
from pathlib import Path

import httpx
import pytest

from procurement.bootstrap import create_spec_factory
from procurement.ingestion.engine.pagination import PaginationInvariantError, iter_search_pages
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.contract_performance.resource import (
    CONTRACT_PATH,
    LINKED_PATH,
    LOOKUP_PATH,
    SEARCH_PATH,
    ContractPerformanceApi,
    create_contract_performance_spec,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contract_performance"


class FixtureClient:
    def __init__(self, year=2025):
        self.data = json.loads((FIXTURES / f"{year}.json").read_text())
        self.calls = []

    def post(self, path, body):
        self.calls.append((path, copy.deepcopy(body)))
        if path == SEARCH_PATH:
            day = body["publicDate"]["greaterThan"][:10]
            items = [r for r in self.data["search"] if r["publicDate"][:10] == day]
            n, size = body["pageNumber"], body["pageSize"]
            return {
                "page": {
                    "content": items[n * size : (n + 1) * size],
                    "currentPage": n + 1,
                    "pageSize": size,
                    "totalPages": (len(items) + size - 1) // size,
                    "totalElements": len(items),
                }
            }
        if path == LINKED_PATH:
            return copy.deepcopy(
                next(
                    r
                    for r in self.data["linked"]
                    if r["resultList"][0]["contractCode"] == body["contractCode"]
                )
            )
        if path == LOOKUP_PATH:
            code = body[0]["query"][0]["keyWord"]
            return copy.deepcopy(
                next(
                    r
                    for r in self.data["lookups"]
                    if r["page"]["content"][0]["contractCode"] == code
                )
            )
        if path == CONTRACT_PATH:
            return copy.deepcopy(
                next(r for r in self.data["contracts"] if r["contract"]["id"] == body["id"])
            )
        raise AssertionError(path)


def extract(spec, items, day, page=0):
    stats = PageStats()
    result = list(
        spec.records(
            search_items=items,
            run_id="fixture-run",
            source_date=day,
            search_page=page,
            errors=[],
            stats=stats,
        )
    )
    return result, stats


@pytest.mark.parametrize("year", [2024, 2025])
def test_profiled_fixtures_and_cache_across_pages(year):
    client = FixtureClient(year)
    spec = create_contract_performance_spec(client)
    all_records = []
    for item in client.data["search"]:
        rows, stats = extract(spec, [item], date.fromisoformat(item["publicDate"][:10]))
        all_records.extend(rows)
        assert not stats.quality_observations
    reports = [r for r in all_records if r.table == "contract_performance_detail"]
    contracts = [r for r in all_records if r.table == "contract_detail"]
    assert len(reports) == 12
    assert len(contracts) == len({r["contractCode"] for r in client.data["search"]})
    for r in reports:
        assert r.record.payload["resolution"]["status"] == "resolved"
    assert spec.strict_pagination


def test_request_filters_and_empty_normalized_page():
    client = FixtureClient()
    api = ContractPerformanceApi(client)
    result = api.search(
        page_number=0,
        page_size=50,
        window_from="2025-01-01T00:00:00.000Z",
        window_to="2025-01-01T23:59:59.999Z",
    )
    assert result["page"]["currentPage"] == 0
    body = client.calls[0][1]
    assert body["contractSignDate"] == {"greaterThan": None, "lessThan": None}
    assert body["publicDate"]["greaterThan"].startswith("2025-01-01")
    assert (
        len(
            list(
                iter_search_pages(
                    api.search, window_from="2025-01-01", window_to="2025-01-01", page_size=50
                )
            )
        )
        == 1
    )


def test_actual_pagination_and_duplicate_identity_rejected():
    client = FixtureClient()
    day = client.data["search"][0]["publicDate"][:10]
    api = ContractPerformanceApi(client)
    pages = list(
        iter_search_pages(
            api.search,
            window_from=day,
            window_to=day,
            page_size=1,
            search_key=lambda r: (r["id"], r["version"]),
        )
    )
    assert sum(len(p["page"]["content"]) for _, p in pages) > 1
    item = client.data["search"][0]

    def repeated(**kw):
        return {
            "page": {
                "content": [item],
                "totalElements": 2,
                "totalPages": 2,
                "currentPage": kw["page_number"],
                "pageSize": 1,
            }
        }

    with pytest.raises(PaginationInvariantError, match="Repeated"):
        list(
            iter_search_pages(
                repeated,
                window_from=day,
                window_to=day,
                page_size=1,
                search_key=lambda r: (r["id"], r["version"]),
            )
        )


def test_select_report_by_id_and_version_not_position():
    client = FixtureClient()
    item = client.data["search"][0]
    linked = next(
        r
        for r in client.data["linked"]
        if r["resultList"][0]["contractCode"] == item["contractCode"]
    )
    wrong = {**linked["resultList"][0], "id": "another-report"}
    linked["resultList"].insert(0, wrong)
    rows, _ = extract(
        create_contract_performance_spec(client),
        [item],
        date.fromisoformat(item["publicDate"][:10]),
    )
    report = next(
        r.record.payload["performance"] for r in rows if r.table == "contract_performance_detail"
    )
    assert report["id"] == item["id"]


def test_missing_referenced_version_retains_report_and_current_contract():
    client = FixtureClient()
    item = client.data["search"][0]
    item["contractVersion"] = "99"
    linked = next(
        r
        for r in client.data["linked"]
        if r["resultList"][0]["contractCode"] == item["contractCode"]
    )
    linked["resultList"][0]["contractVersion"] = "99"
    rows, stats = extract(
        create_contract_performance_spec(client),
        [item],
        date.fromisoformat(item["publicDate"][:10]),
    )
    report = next(r for r in rows if r.table == "contract_performance_detail")
    assert report.record.payload["resolution"]["status"] == "missing_contract_version"
    assert len(stats.quality_observations) == 1
    assert any(r.table == "contract_detail" for r in rows)


def test_fetch_exact_referenced_version():
    client = FixtureClient()
    item = client.data["search"][0]
    current = next(
        r for r in client.data["contracts"] if r["contract"]["contractCode"] == item["contractCode"]
    )
    old = copy.deepcopy(current)
    current["contract"]["id"] = "latest-contract-id"
    current["contract"]["contractVersion"] = "01"
    current["versionsList"] = [
        {"id": "latest-contract-id", "version": "01"},
        {"id": old["contract"]["id"], "version": "00"},
    ]
    lookup = next(
        r
        for r in client.data["lookups"]
        if r["page"]["content"][0]["contractCode"] == item["contractCode"]
    )
    lookup["page"]["content"][0]["id"] = "latest-contract-id"
    client.data["contracts"].append(old)
    rows, _ = extract(
        create_contract_performance_spec(client),
        [item],
        date.fromisoformat(item["publicDate"][:10]),
    )
    assert {r.record.source_version for r in rows if r.table == "contract_detail"} == {"00", "01"}
    assert (
        next(
            r.record.payload["resolution"]["status"]
            for r in rows
            if r.table == "contract_performance_detail"
        )
        == "resolved"
    )


def test_missing_requested_report_and_wrong_day_fail():
    client = FixtureClient()
    item = copy.deepcopy(client.data["search"][0])
    item["id"] = "nonexistent-report"
    with pytest.raises(ValueError, match="exactly"):
        extract(
            create_contract_performance_spec(client),
            [item],
            date.fromisoformat(item["publicDate"][:10]),
        )
    with pytest.raises(ValueError, match="outside"):
        extract(create_contract_performance_spec(client), [item], date(2025, 1, 1))


@pytest.mark.parametrize("status", [401, 403, 429])
def test_resource_circuit_stops_on_blocked_response_without_retry(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"error": "blocked"}, headers={"Retry-After": "60"})

    client = MuasamcongClient(token="fixture-only", transport=httpx.MockTransport(handler))
    scoped = client.contract_performance_reader()
    try:
        for _ in range(2):
            with pytest.raises(httpx.HTTPStatusError):
                scoped.post(SEARCH_PATH, {})
        assert len(calls) == 1
    finally:
        scoped.close()
        client.close()


def test_bootstrap_factory_recognizes_resource():
    spec = create_spec_factory(FixtureClient())("contract_performance")
    assert spec.identity.resource == "contract_performance"


def test_two_reports_for_one_contract_share_discovery_and_emit_contract_once():
    client = FixtureClient()
    first = client.data["search"][0]
    linked = next(
        r
        for r in client.data["linked"]
        if r["resultList"][0]["contractCode"] == first["contractCode"]
    )
    second = {**first, "id": "second-public-report"}
    linked["resultList"].append({**linked["resultList"][0], "id": second["id"]})
    spec = create_contract_performance_spec(client)
    day = date.fromisoformat(first["publicDate"][:10])
    initial, _ = extract(spec, [first], day, page=0)
    following, _ = extract(spec, [second], day, page=1)
    assert [r.table for r in following] == ["contract_performance_detail"]
    assert sum(r.table == "contract_detail" for r in initial + following) == 1
    for path in [LINKED_PATH, LOOKUP_PATH, CONTRACT_PATH]:
        assert sum(p == path for p, _ in client.calls) == 1


def test_zero_contract_lookup_is_warning_not_report_loss(monkeypatch):
    client = FixtureClient()
    original = client.post
    monkeypatch.setattr(
        client,
        "post",
        lambda path, body: (
            {"page": {"content": [], "totalElements": 0}}
            if path == LOOKUP_PATH
            else original(path, body)
        ),
    )
    item = client.data["search"][0]
    records, stats = extract(
        create_contract_performance_spec(client),
        [item],
        date.fromisoformat(item["publicDate"][:10]),
    )
    assert len(records) == 1
    assert records[0].table == "contract_performance_detail"
    assert records[0].record.payload["resolution"]["status"] == "missing_contract"
    assert stats.quality_observations[0]["code"] == "missing_contract"


def test_html_challenge_is_not_retried():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            200, text="<html>challenge</html>", headers={"Content-Type": "text/html"}
        )

    parent = MuasamcongClient(token="fixture-only", transport=httpx.MockTransport(handler))
    reader = parent.contract_performance_reader()
    try:
        with pytest.raises(httpx.HTTPError, match="invalid JSON"):
            reader.post(SEARCH_PATH, {})
        assert len(calls) == 1
    finally:
        reader.close()
        parent.close()


def test_retry_and_all_endpoint_reads_share_spacing():
    from time import monotonic

    starts = []

    def handler(request):
        starts.append(monotonic())
        return httpx.Response(503 if len(starts) == 1 else 200, json={"ok": True})

    parent = MuasamcongClient(
        token="fixture-only",
        transport=httpx.MockTransport(handler),
        max_retry_delay=0,
        sleep=lambda _: None,
    )
    reader = parent.contract_performance_reader()
    try:
        reader.post(SEARCH_PATH, {})
        reader.post(LINKED_PATH, {})
        reader.post(CONTRACT_PATH, {})
        assert len(starts) == 4
        assert all(b - a >= 0.045 for a, b in pairwise(starts))
    finally:
        reader.close()
        parent.close()


def test_lower_operator_pacing_is_respected():
    from time import monotonic

    from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget

    starts = []

    def handler(request):
        starts.append(monotonic())
        return httpx.Response(200, json={"ok": True})

    parent = MuasamcongClient(
        token="fixture-only",
        transport=httpx.MockTransport(handler),
        request_budget=RequestBudget(1, min_interval=0.09),
    )
    reader = parent.contract_performance_reader()
    try:
        reader.post(SEARCH_PATH, {})
        reader.post(LINKED_PATH, {})
        assert starts[1] - starts[0] >= 0.085
    finally:
        reader.close()
        parent.close()


def test_429_stops_before_retry_after_wait():
    waits = []
    parent = MuasamcongClient(
        token="fixture-only",
        sleep=waits.append,
        transport=httpx.MockTransport(
            lambda r: httpx.Response(429, json={"error": "limited"}, headers={"Retry-After": "1"})
        ),
    )
    reader = parent.contract_performance_reader()
    try:
        with pytest.raises(httpx.HTTPStatusError):
            reader.post(SEARCH_PATH, {})
        assert waits == []
    finally:
        reader.close()
        parent.close()


def test_wrapped_429_as_http_400_stops_the_shared_circuit():
    from procurement.ingestion.sources.muasamcong.client import is_source_rate_limit

    calls, waits = [], []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            400, text='429 Too Many Requests: [{"message":"API rate limit exceeded"}]'
        )

    parent = MuasamcongClient(
        token="fixture-only", transport=httpx.MockTransport(handler), sleep=waits.append
    )
    reader = parent.contract_performance_reader()
    try:
        for _ in range(2):
            with pytest.raises(httpx.HTTPStatusError) as error:
                reader.post(LOOKUP_PATH, [])
            assert error.value.response.status_code == 400
            assert is_source_rate_limit(error.value.response)
        assert len(calls) == 1
        assert waits == []
    finally:
        reader.close()
        parent.close()
