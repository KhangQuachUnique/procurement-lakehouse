import copy
import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.bid_opening.resource import (
    ENDPOINTS,
    BidOpeningApi,
    create_bid_opening_spec,
)
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.quality.contracts import load_config, validate_detail
from procurement.quality.repair import fetch_replacement

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.fixture
def dual_payload():
    return json.loads((FIXTURES / "bid_opening_dual.json").read_text(encoding="utf-8"))


def phase_client(payload, calls):
    class Client:
        def post(self, path, body):
            calls.append((path, dict(body)))
            part = next(k for k, v in ENDPOINTS.items() if path == v)
            if part in {"bid_open", "lot_open_detail"}:
                assert body["packType"] in {1, 2} and body["viewType"] == 0
                part += "_technical" if body["packType"] == 1 else "_financial"
            return copy.deepcopy(payload[part])
        post_array = post
    return Client()


@pytest.mark.parametrize("published", [True, False])
def test_dual_phase_requests_and_raw_preservation(dual_payload, published):
    if not published:
        dual_payload["roundmng"]["bidoBidroundMngViewDTO"]["successBidOpenDateTc"] = None
        del dual_payload["bid_open_financial"], dual_payload["lot_open_detail_financial"]
    calls, evidence = [], {}
    result = BidOpeningApi(phase_client(dual_payload, calls)).fetch(context(dual_payload), evidence)
    assert result == dual_payload
    assert len(calls) == (6 if published else 4)
    checked = validate(result)
    assert checked["status"] == "pass"
    assert checked["collection_status"] == ("complete" if published else "awaiting_financial")
    assert evidence["parts"]["bid_open_technical"]["request"]["packType"] == 1
    if published:
        assert evidence["parts"]["bid_open_financial"]["request"]["packType"] == 2
        tech = result["bid_open_technical"]["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"][0]
        fin = result["bid_open_financial"]["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"][0]
        assert tech["id"] != fin["id"]
        assert tech["bidPrice"] == 0 and fin["bidPrice"] == 912050000


@pytest.mark.parametrize("change", ["financial_null", "financial_missing", "financial_error", "lot_identity",
                                     "unknown_mode", "conflicting_mode", "missing_marker", "invalid_marker"])
def test_dual_phase_invalid_data_is_not_treated_as_unpublished(dual_payload, change):
    root = dual_payload["roundmng"]["bidoBidroundMngViewDTO"]
    if change == "financial_null":
        dual_payload["bid_open_financial"]["bidSubmissionByContractorViewResponse"] = None
    elif change == "financial_missing":
        del dual_payload["bid_open_financial"]
    elif change == "financial_error":
        dual_payload["bid_open_financial"]["success"] = False
    elif change == "lot_identity":
        dual_payload["lot_open_detail_financial"] = [{"notifyId": "other"}]
    elif change == "unknown_mode":
        root["bidMode"] = "unknown"
        dual_payload["notify"]["bidNoContractorResponse"]["bidNotification"]["bidMode"] = "unknown"
    elif change == "conflicting_mode":
        root["bidMode"] = "1_MTHS"
    elif change == "missing_marker":
        del root["successBidOpenDateTc"]
    else:
        root["successBidOpenDateTc"] = "not-a-date"
    assert validate(dual_payload)["status"] in {"fail", "unresolved"}


def test_failed_financial_request_emits_no_record(dual_payload):
    base = phase_client(dual_payload, [])
    class Client:
        def post(self, path, body):
            if path == ENDPOINTS["bid_open"] and body["packType"] == 2:
                raise httpx.ReadTimeout("financial timed out")
            return base.post(path, body)
        post_array = post
    stats, errors = PageStats(), []
    rows = list(create_bid_opening_spec(Client()).records(search_items=[context(dual_payload)],
        run_id="dual", source_date=date(2022, 9, 19), search_page=0, errors=errors, stats=stats))
    assert not rows and len(errors) == 1
    assert stats.quality_observations[0]["parts"]["bid_open_financial"]["status"] == "failed"


def test_dual_repair_fetches_both_phases(dual_payload, tmp_path):
    calls = []
    row = {"context": context(dual_payload), "source_id": context(dual_payload)["notifyNo"], "source_version": "00"}
    result = fetch_replacement(phase_client(dual_payload, calls), row,
                               load_config(resource="bid_opening"), tmp_path)
    assert result[0] == dual_payload and len(calls) == 6


@pytest.fixture
def payload():
    return json.loads((FIXTURES / "bid_opening.json").read_text(encoding="utf-8"))


def context(payload):
    root = payload["notify"]["bidNoContractorResponse"]["bidNotification"]
    return {key: root[key] for key in ("id", "notifyNo", "notifyVersion", "isInternet")}


def validate(payload):
    return validate_detail(payload, context(payload), load_config(resource="bid_opening").contracts["bid_opening"])


def test_all_four_preserved_and_evidenced(payload):
    calls = []
    original = copy.deepcopy(payload)
    def handle(request):
        calls.append(request.url.path)
        assert json.loads(request.content) == {"notifyId": context(payload)["id"],
            "notifyNo": context(payload)["notifyNo"], "type": "TBMT", "packType": 0}
        part = next(k for k, v in ENDPOINTS.items() if v == request.url.path)
        return httpx.Response(200, json=payload[part])
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle)) as client:
        spec = create_bid_opening_spec(client)
        errors, stats = [], PageStats()
        records = list(spec.records(search_items=[context(payload)], run_id="run",
            source_date=date(2026, 9, 27), search_page=0, errors=errors, stats=stats))
    assert not errors
    assert calls == list(ENDPOINTS.values())
    assert records[0].record.payload == original
    assert records[0].record.source_version == "01"
    observation = stats.quality_observations[0]
    assert set(observation["parts"]) == set(ENDPOINTS)
    assert "test" not in json.dumps(observation)
    assert all("completed_at" in part for part in observation["parts"].values())


def test_empty_lots_allowed(payload):
    payload["lot_open_detail"] = []
    assert validate(payload)["status"] == "pass"


def test_missing_version_cannot_form_complete_assembly(payload):
    ctx = context(payload)
    ctx.pop("notifyVersion")
    payload["notify"]["bidNoContractorResponse"]["bidNotification"].pop("notifyVersion")
    payload["roundmng"]["bidoBidroundMngViewDTO"].pop("notifyVersion")
    result = validate_detail(payload, ctx, load_config(resource="bid_opening").contracts["bid_opening"])
    assert result["status"] == "fail"


@pytest.mark.parametrize("change", ["missing", "error", "wrong_uuid", "wrong_version", "lot_identity", "lot_null", "bidders_null"])
def test_invalid_assembly_rejected(payload, change):
    if change == "missing":
        del payload["roundmng"]
    elif change == "error":
        payload["bid_open"]["error"] = "source failed"
    elif change == "wrong_uuid":
        payload["roundmng"]["bidoBidroundMngViewDTO"]["id"] = "other"
    elif change == "wrong_version":
        payload["roundmng"]["bidoBidroundMngViewDTO"]["notifyVersion"] = "02"
    elif change == "lot_identity":
        payload["lot_open_detail"][0]["notifyId"] = "other"
    elif change == "lot_null":
        payload["lot_open_detail"] = None
    else:
        payload["bid_open"]["bidSubmissionByContractorViewResponse"] = None
    assert validate(payload)["status"] == "fail"


def test_array_transport_retry_and_object_contract():
    attempts = []
    def handle(request):
        attempts.append(request)
        return httpx.Response(503) if len(attempts) == 1 else httpx.Response(200, json=[])
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle), sleep=lambda _: None) as client:
        assert client.post_array("/lots", {}) == []
        with pytest.raises(TypeError, match="object"):
            client.post("/object", {})
    assert len(attempts) == 3


def test_one_failed_endpoint_emits_no_record(payload):
    def handle(request):
        if request.url.path == ENDPOINTS["lot_open_detail"]:
            return httpx.Response(500)
        return httpx.Response(200, json=payload[next(k for k, v in ENDPOINTS.items() if v == request.url.path)])
    with MuasamcongClient(token="test", max_attempts=1, transport=httpx.MockTransport(handle)) as client:
        errors, stats = [], PageStats()
        rows = list(create_bid_opening_spec(client).records(search_items=[context(payload)],
            run_id="r", source_date=date(2026, 9, 27), search_page=0, errors=errors, stats=stats))
    assert rows == [] and len(errors) == 1
    assert len(stats.quality_observations[0]["parts"]) == 4


def test_search_matches_supplied_filters():
    class Client:
        def post(self, path, body):
            filters = {f["fieldName"]: f for f in body[0]["query"][0]["filters"]}
            assert set(filters["stepCode"]["fieldValues"]) == {
                "notify-contractor-step-2-kqmt", "notify-contractor-step-3-dsntdkt", "notify-contractor-step-4-kqlcnt"}
            assert filters["publicDateKqmt"]["searchType"] == "not_null"
            assert filters["isInternet"]["fieldValues"] == [1]
            assert filters["publicDate"]["to"].endswith("23:59:59.999Z")
            return json.loads((FIXTURES / "bid_opening_search.json").read_text(encoding="utf-8"))
    response = BidOpeningApi(Client()).search(page_number=0, page_size=50,
        window_from="2026-08-03T00:00:00.000Z", window_to="2026-08-03T23:59:59.999Z")
    assert response["page"]["totalElements"] == 465


def test_selective_refetch_uses_four_calls_and_cache(payload, tmp_path):
    calls = []
    class Client:
        def post(self, path, body):
            calls.append(path)
            return payload[next(k for k, v in ENDPOINTS.items() if v == path)]
        post_array = post
    row = {"context": context(payload), "source_id": context(payload)["notifyNo"], "source_version": "01"}
    config = load_config(resource="bid_opening")
    first = fetch_replacement(Client(), row, config, tmp_path)
    second = fetch_replacement(Client(), row, config, tmp_path)
    assert first[0] == second[0] == payload
    assert len(calls) == 4
    assert len(second[1]["collection"]["parts"]) == 4
