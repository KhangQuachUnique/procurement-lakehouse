import copy
import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from procurement.ingestion.engine.stats import PageStats

from procurement.ingestion.sources.muasamcong.bid_opening.resource import (
    ADB_WB_OPENING_PATH,
    ENDPOINTS,
    KHAC_OPENING_PATH,
    STANDARD_NOTICE_PATH,
    BidOpeningApi,
    create_bid_opening_spec,
)

from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.quality.contracts import (
    DetailValidationError,
    load_config,
    validate_detail,
)
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
        dual_payload["roundmng"]["bidoBidroundMngViewDTO"][
            "successBidOpenDateTc"
        ] = None
        del (
            dual_payload["bid_open_financial"],
            dual_payload["lot_open_detail_financial"],
        )
    calls, evidence = [], {}
    result = BidOpeningApi(phase_client(dual_payload, calls)).fetch(
        context(dual_payload), evidence
    )
    assert result == dual_payload
    assert len(calls) == (6 if published else 4)
    checked = validate(result)
    assert checked["status"] == "pass"
    assert checked["collection_status"] == (
        "complete" if published else "awaiting_financial"
    )
    assert evidence["parts"]["bid_open_technical"]["request"]["packType"] == 1
    if published:
        assert evidence["parts"]["bid_open_financial"]["request"]["packType"] == 2
        tech = result["bid_open_technical"]["bidSubmissionByContractorViewResponse"][
            "bidSubmissionDTOList"
        ][0]
        fin = result["bid_open_financial"]["bidSubmissionByContractorViewResponse"][
            "bidSubmissionDTOList"
        ][0]
        assert tech["id"] != fin["id"]
        assert tech["bidPrice"] == 0 and fin["bidPrice"] == 912050000


@pytest.mark.parametrize(
    "change",
    [
        "financial_null",
        "financial_missing",
        "financial_error",
        "lot_identity",
        "unknown_mode",
        "conflicting_mode",
        "missing_marker",
        "invalid_marker",
    ],
)
def test_dual_phase_invalid_data_is_not_treated_as_unpublished(dual_payload, change):
    root = dual_payload["roundmng"]["bidoBidroundMngViewDTO"]
    if change == "financial_null":
        dual_payload["bid_open_financial"][
            "bidSubmissionByContractorViewResponse"
        ] = None
    elif change == "financial_missing":
        del dual_payload["bid_open_financial"]
    elif change == "financial_error":
        dual_payload["bid_open_financial"]["success"] = False
    elif change == "lot_identity":
        dual_payload["lot_open_detail_financial"] = [{"notifyId": "other"}]
    elif change == "unknown_mode":
        root["bidMode"] = "unknown"
        dual_payload["notify"]["bidNoContractorResponse"]["bidNotification"][
            "bidMode"
        ] = "unknown"
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
    rows = list(
        create_bid_opening_spec(Client()).records(
            search_items=[context(dual_payload)],
            run_id="dual",
            source_date=date(2022, 9, 19),
            search_page=0,
            errors=errors,
            stats=stats,
        )
    )
    assert not rows and len(errors) == 1
    assert (
        stats.quality_observations[0]["parts"]["bid_open_financial"]["status"]
        == "failed"
    )


def test_dual_repair_fetches_both_phases(dual_payload, tmp_path):
    calls = []
    row = {
        "context": context(dual_payload),
        "source_id": context(dual_payload)["notifyNo"],
        "source_version": "00",
    }
    result = fetch_replacement(
        phase_client(dual_payload, calls),
        row,
        load_config(resource="bid_opening"),
        tmp_path,
    )
    assert result[0] == dual_payload and len(calls) == 6


@pytest.fixture
def payload():
    return json.loads((FIXTURES / "bid_opening.json").read_text(encoding="utf-8"))


def context(payload):
    root = payload["notify"]["bidNoContractorResponse"]["bidNotification"]
    return {key: root[key] for key in ("id", "notifyNo", "notifyVersion", "isInternet")}


def validate(payload):
    return validate_detail(
        payload,
        context(payload),
        load_config(resource="bid_opening").contracts["bid_opening"],
    )


def test_all_four_preserved_and_evidenced(payload):
    calls = []
    original = copy.deepcopy(payload)

    def handle(request):
        calls.append(request.url.path)
        assert json.loads(request.content) == {
            "notifyId": context(payload)["id"],
            "notifyNo": context(payload)["notifyNo"],
            "type": "TBMT",
            "packType": 0,
        }
        part = next(k for k, v in ENDPOINTS.items() if v == request.url.path)
        return httpx.Response(200, json=payload[part])

    with MuasamcongClient(
        token="test", transport=httpx.MockTransport(handle)
    ) as client:
        spec = create_bid_opening_spec(client)
        errors, stats = [], PageStats()
        records = list(
            spec.records(
                search_items=[context(payload)],
                run_id="run",
                source_date=date(2026, 9, 27),
                search_page=0,
                errors=errors,
                stats=stats,
            )
        )
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
    result = validate_detail(
        payload, ctx, load_config(resource="bid_opening").contracts["bid_opening"]
    )
    assert result["status"] == "fail"


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "error",
        "wrong_uuid",
        "wrong_version",
        "lot_identity",
        "lot_null",
        "bidders_null",
    ],
)
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
        return (
            httpx.Response(503) if len(attempts) == 1 else httpx.Response(200, json=[])
        )

    with MuasamcongClient(
        token="test", transport=httpx.MockTransport(handle), sleep=lambda _: None
    ) as client:
        assert client.post_array("/lots", {}) == []
        with pytest.raises(TypeError, match="object"):
            client.post("/object", {})
    assert len(attempts) == 3


def test_one_failed_endpoint_emits_no_record(payload):
    def handle(request):
        if request.url.path == ENDPOINTS["lot_open_detail"]:
            return httpx.Response(500)
        return httpx.Response(
            200,
            json=payload[
                next(k for k, v in ENDPOINTS.items() if v == request.url.path)
            ],
        )

    with MuasamcongClient(
        token="test", max_attempts=1, transport=httpx.MockTransport(handle)
    ) as client:
        errors, stats = [], PageStats()
        rows = list(
            create_bid_opening_spec(client).records(
                search_items=[context(payload)],
                run_id="r",
                source_date=date(2026, 9, 27),
                search_page=0,
                errors=errors,
                stats=stats,
            )
        )
    assert rows == [] and len(errors) == 1
    assert len(stats.quality_observations[0]["parts"]) == 4


def test_search_matches_supplied_filters():
    class Client:
        def post(self, path, body):
            filters = {f["fieldName"]: f for f in body[0]["query"][0]["filters"]}
            assert set(filters["stepCode"]["fieldValues"]) == {
                "notify-contractor-step-2-kqmt",
                "notify-contractor-step-3-dsntdkt",
                "notify-contractor-step-4-kqlcnt",
            }
            assert filters["publicDateKqmt"]["searchType"] == "not_null"
            assert filters["isInternet"]["fieldValues"] == [1]
            assert filters["publicDate"]["to"].endswith("23:59:59.999Z")
            return json.loads(
                (FIXTURES / "bid_opening_search.json").read_text(encoding="utf-8")
            )

    response = BidOpeningApi(Client()).search(
        page_number=0,
        page_size=50,
        window_from="2026-08-03T00:00:00.000Z",
        window_to="2026-08-03T23:59:59.999Z",
    )
    assert response["page"]["totalElements"] == 465


def test_selective_refetch_uses_four_calls_and_cache(payload, tmp_path):
    calls = []

    class Client:
        def post(self, path, body):
            calls.append(path)
            return payload[next(k for k, v in ENDPOINTS.items() if v == path)]

        post_array = post

    row = {
        "context": context(payload),
        "source_id": context(payload)["notifyNo"],
        "source_version": "01",
    }
    config = load_config(resource="bid_opening")
    first = fetch_replacement(Client(), row, config, tmp_path)
    second = fetch_replacement(Client(), row, config, tmp_path)
    assert first[0] == second[0] == payload
    assert len(calls) == 4
    assert len(second[1]["collection"]["parts"]) == 4


def khac_example():
    uid = "e505fb3f-a288-407c-9213-905414f96da4"

    context = {
        "id": uid,
        "notifyId": uid,
        "notifyNo": "IB2200001476",
        "notifyVersion": "00",
        "processApply": "KHAC",
        "bidMode": "1_MTHS",
        "isInternet": 1,
    }

    common = {
        "id": uid,
        "notifyNo": context["notifyNo"],
        "notifyVersion": "00",
        "processApply": "KHAC",
        "bidMode": "1_MTHS",
    }

    response = {
        "bidoNotifyContractorP": dict(common),
        "bidoBidroundOutMngViewDTO": {
            **common,
            "countContractor": 2,
            "successBidOpenDate": "2022-10-04T10:01:12",
        },
        "bidSubmissionByContractorViewResponse": {
            "bidSubmissionDTOList": [
                {"id": "submission-1", "contractorCode": "A"},
                {"id": "submission-2", "contractorCode": "B"},
            ]
        },
    }

    return context, response


def test_khac_opening_fetch_and_validation():
    ctx, response = khac_example()
    calls = []

    class Client:
        def post(self, path, body):
            calls.append((path, dict(body)))
            return copy.deepcopy(response)

    evidence = {}
    payload = BidOpeningApi(Client()).fetch(ctx, evidence)

    assert calls == [(KHAC_OPENING_PATH, {"id": ctx["id"]})]

    assert payload == {"opening_out": response}

    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "complete"
    assert result["identity"]["notifyNo"] == "IB2200001476"
    assert result["identity"]["notifyVersion"] == "00"


@pytest.mark.parametrize(
    "failure",
    ["missing_round", "null_bidders", "wrong_identity"],
)
def test_khac_opening_rejects_invalid_data(failure):
    ctx, response = khac_example()

    if failure == "missing_round":
        response["bidoBidroundOutMngViewDTO"] = None

    elif failure == "null_bidders":
        response["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"] = None

    else:
        response["bidoBidroundOutMngViewDTO"]["notifyNo"] = "IB_WRONG"

    config = load_config(resource="bid_opening")

    result = validate_detail(
        {"opening_out": response},
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "fail"


def cancelled_dual_example(dual_payload):
    payload = copy.deepcopy(dual_payload)

    original = payload["notify"]["bidNoContractorResponse"]["bidNotification"]

    payload["roundmng"]["bidoBidroundMngViewDTO"][
        "successBidOpenDateTc"
    ] = "2022-11-29T15:26:36"

    payload["bid_open_financial"]["bidSubmissionByContractorViewResponse"] = None

    replacement_id = "replacement-uuid"
    replacement_no = "IB2200034695"

    cancellation_notice = {
        "bidNoContractorResponse": {
            "bidNotification": copy.deepcopy(original),
        },
        "bidCancelingResponse": {
            "id": original["id"],
            "notifyNo": original["notifyNo"],
            "bidNo": original["bidNo"],
            "cancelDate": "2022-11-01T08:50:50",
            "cancelDecisionNo": "437/QD-TEST",
        },
        "bidpPlanDetail": {
            "id": original["bidId"],
            "linkNotifyInfo": json.dumps(
                {
                    "notifyId": replacement_id,
                    "notifyNo": replacement_no,
                    "bidId": original["bidId"],
                }
            ),
        },
    }

    payload["cancellation_notice"] = cancellation_notice

    ctx = {
        **context(dual_payload),
        "processApply": "LDT",
        "bidMode": "1_HTHS",
    }

    return ctx, payload


def test_cancelled_after_technical_is_accepted(dual_payload):
    ctx, payload = cancelled_dual_example(dual_payload)

    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "cancelled_after_technical"
    assert result["identity"]["notifyNo"] == ctx["notifyNo"]

    # Preserve the actual null financial root.
    assert (
        payload["bid_open_financial"]["bidSubmissionByContractorViewResponse"] is None
    )


@pytest.mark.parametrize(
    "problem",
    [
        "wrong_cancel_id",
        "missing_cancel_date",
        "wrong_link",
        "technical_after_cancel",
        "financial_missing_field",
    ],
)
def test_invalid_cancellation_is_rejected(dual_payload, problem):
    ctx, payload = cancelled_dual_example(dual_payload)

    cancellation = payload["cancellation_notice"]["bidCancelingResponse"]

    if problem == "wrong_cancel_id":
        cancellation["id"] = "wrong-uuid"

    elif problem == "missing_cancel_date":
        cancellation["cancelDate"] = None

    elif problem == "wrong_link":
        payload["cancellation_notice"]["bidpPlanDetail"]["linkNotifyInfo"] = json.dumps(
            {
                "notifyId": "another-uuid",
                "notifyNo": "IB_OTHER",
                "bidId": "wrong-package",
            }
        )

    elif problem == "technical_after_cancel":
        payload["bid_open_technical"]["bidSubmissionByContractorViewResponse"][
            "bidSubmissionDTOList"
        ][0]["createdDateBidOpen"] = "2022-12-01T10:00:00"

    else:
        del payload["bid_open_financial"]["bidSubmissionByContractorViewResponse"]

    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "fail"


def test_cancelled_opening_fetches_cancellation_evidence(dual_payload):
    ctx, expected = cancelled_dual_example(dual_payload)
    calls = []

    class Client:
        def post(self, path, body):
            calls.append((path, dict(body)))

            if path == STANDARD_NOTICE_PATH:
                return copy.deepcopy(expected["cancellation_notice"])

            component = next(key for key, value in ENDPOINTS.items() if value == path)

            if component in {"bid_open", "lot_open_detail"}:
                phase = "_technical" if body["packType"] == 1 else "_financial"
                component += phase

            return copy.deepcopy(expected[component])

        post_array = post

    evidence = {}
    actual = BidOpeningApi(Client()).fetch(ctx, evidence)

    assert actual == expected
    assert len(calls) == 7
    assert calls[-1] == (
        STANDARD_NOTICE_PATH,
        {"id": ctx["id"]},
    )

    result = validate_detail(
        actual,
        ctx,
        load_config(resource="bid_opening").contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "cancelled_after_technical"


def khac_dual_example():
    uid = "8ebc7466-1e8f-4548-b558-fe165f037978"

    context = {
        "id": uid,
        "notifyId": uid,
        "notifyNo": "IB2200011849",
        "notifyVersion": "00",
        "processApply": "KHAC",
        "bidMode": "1_HTHS",
        "isInternet": 1,
    }

    common = {
        "id": uid,
        "notifyNo": context["notifyNo"],
        "notifyVersion": "00",
        "processApply": "KHAC",
        "bidMode": "1_HTHS",
    }

    technical = {
        "bidoNotifyContractorP": dict(common),
        "bidoBidroundOutMngViewDTO": {
            **common,
            "packType": 1,
            "countContractor": 1,
            "successBidOpenDate": "2022-10-21T09:19:23",
            "successBidOpenDateTc": "2022-10-24T16:43:30",
        },
        "bidSubmissionByContractorViewResponse": {
            "bidSubmissionDTOList": [
                {
                    "id": "technical-submission",
                    "contractorCode": "vn0303476197",
                    "bidPrice": 0,
                    "lotNoValueDTOList": [],
                }
            ]
        },
    }

    financial = copy.deepcopy(technical)

    financial["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"] = [
        {
            "id": "financial-submission",
            "contractorCode": "vn0303476197",
            "bidPrice": 3280000000,
            "lotNoValueDTOList": [],
        }
    ]

    return context, technical, financial


def test_khac_dual_fetch_and_validation():
    ctx, technical, financial = khac_dual_example()

    calls = []

    class Client:
        def post(self, path, body):
            calls.append((path, dict(body)))

            if body["packType"] == 1:
                return copy.deepcopy(technical)

            if body["packType"] == 2:
                return copy.deepcopy(financial)

            raise AssertionError("Unexpected packType")

    evidence = {}

    payload = BidOpeningApi(Client()).fetch(
        ctx,
        evidence,
    )

    assert len(calls) == 2

    assert calls[0][1] == {
        "id": ctx["id"],
        "packType": 1,
    }

    assert calls[1][1] == {
        "id": ctx["id"],
        "packType": 2,
    }

    assert payload == {
        "opening_out_technical": technical,
        "opening_out_financial": financial,
    }

    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "complete"
    assert result["identity"]["notifyNo"] == "IB2200011849"


def test_khac_dual_rejects_missing_financial():
    ctx, technical, _ = khac_dual_example()

    payload = {
        "opening_out_technical": technical,
    }

    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "fail"


def test_khac_dual_awaiting_financial():
    ctx, technical, _ = khac_dual_example()

    technical["bidoBidroundOutMngViewDTO"]["successBidOpenDateTc"] = None

    payload = {
        "opening_out_technical": technical,
    }

    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "awaiting_financial"


def cancelled_single_example(payload):
    data = copy.deepcopy(payload)

    notice = data["notify"]["bidNoContractorResponse"]["bidNotification"]

    notice["bidOpenDate"] = "2023-02-10T11:00:00"

    # Round metadata is missing in the actual source.
    data["roundmng"]["bidoBidroundMngViewDTO"] = None

    for row in data["bid_open"]["bidSubmissionByContractorViewResponse"][
        "bidSubmissionDTOList"
    ]:
        row["createdDateBidOpen"] = "2023-02-10T11:08:33"

    data["cancellation_notice"] = {
        "bidNoContractorResponse": {
            "bidNotification": copy.deepcopy(notice),
        },
        "bidCancelingResponse": {
            "id": notice["id"],
            "notifyNo": notice["notifyNo"],
            "bidNo": notice["bidNo"],
            "cancelDecisionNo": "48/QD-TEST",
            "cancelDate": "2023-04-11T11:03:54",
        },
    }

    ctx = {
        **context(data),
        "processApply": "LDT",
        "bidMode": "1_MTHS",
    }

    return ctx, data


def test_cancelled_single_opening_accepted(payload):
    ctx, data = cancelled_single_example(payload)

    result = validate_detail(
        data,
        ctx,
        load_config(resource="bid_opening").contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "cancelled_after_opening"


@pytest.mark.parametrize(
    "problem",
    [
        "missing_cancellation",
        "wrong_identity",
        "cancel_before_opening",
        "missing_submissions",
    ],
)
def test_invalid_cancelled_single_rejected(payload, problem):
    ctx, data = cancelled_single_example(payload)

    if problem == "missing_cancellation":
        data.pop("cancellation_notice")

    elif problem == "wrong_identity":
        data["cancellation_notice"]["bidCancelingResponse"]["notifyNo"] = "IB_WRONG"

    elif problem == "cancel_before_opening":
        data["cancellation_notice"]["bidCancelingResponse"][
            "cancelDate"
        ] = "2023-02-01T00:00:00"

    elif problem == "missing_submissions":
        data["bid_open"]["bidSubmissionByContractorViewResponse"][
            "bidSubmissionDTOList"
        ] = None

    result = validate_detail(
        data,
        ctx,
        load_config(resource="bid_opening").contracts["bid_opening"],
    )

    assert result["status"] == "fail"


def adb_single_example():
    uid = "15654f6d-90c8-4f96-a35b-a93c3a526c9f"

    ctx = {
        "id": uid,
        "notifyId": uid,
        "notifyNo": "IB2300169768",
        "notifyVersion": "00",
        "processApply": "ADB",
        "bidMode": "1_MTHS",
        "bidForm": "DTRR",
        "isInternet": 1,
    }

    common = {
        "id": uid,
        "notifyNo": ctx["notifyNo"],
        "notifyVersion": "00",
        "processApply": "ADB",
        "bidMode": "1_MTHS",
        "bidNo": "BP2200270934",
    }

    response = {
        "bidNoContractorResponse": {
            "bidNotification": {
                **common,
                "bidOpenDate": "2023-08-22T08:00:00",
            },
        },
        "bidoBidroundMngViewDTO": {
            **common,
            "countContractor": 2,
            "successBidOpenDate": "2023-08-22T08:01:47",
        },
        "bidSubmissionByContractorViewResponse": {
            "bidSubmissionDTOList": [
                {
                    "id": "3a8e0e19-d6b0-4f1b-ba0e-82b414238c20",
                    "contractorCode": "vn0101819282",
                    "contractorName": ("CÔNG TY CỔ PHẦN XÂY DỰNG " "CÔNG TRÌNH 568"),
                    "bidPrice": 158229576198.904,
                    "bidPriceUnit": "VND",
                    "lotNoValueDTOList": [
                        {
                            "lotNo": "BP2200270934",
                            "lotPrice": 158229576198.904,
                            "lotPriceUnit": "VND",
                        }
                    ],
                },
                {
                    "id": "9a8e4781-4273-4bdd-8aad-dc6243257138",
                    "contractorCode": "vn4101046551",
                    "contractorName": ("CÔNG TY TNHH XÂY DỰNG THỦY DƯƠNG"),
                    "bidPrice": 176833941484.409,
                    "bidPriceUnit": "VND",
                    "lotNoValueDTOList": [
                        {
                            "lotNo": "BP2200270934",
                            "lotPrice": 176833941484.409,
                            "lotPriceUnit": "VND",
                        }
                    ],
                },
            ],
            "bidoLotOpenDetailDTOS": [],
            "bidOpenAdditionList": None,
        },
    }

    return ctx, response


def test_adb_single_fetch_and_validation():
    ctx, response = adb_single_example()

    calls = []

    class Client:
        def post(self, path, body):
            calls.append((path, dict(body)))
            return copy.deepcopy(response)

    evidence = {}

    payload = BidOpeningApi(Client()).fetch(
        ctx,
        evidence,
    )

    # 1. Must use the verified ADB/WB endpoint.
    assert calls == [
        (
            ADB_WB_OPENING_PATH,
            {
                "notifyNo": "IB2300169768",
                "type": "TBMT",
                "packType": 0,
            },
        )
    ]

    # 2. Original API response must be preserved.
    assert payload == {
        "opening_adb_wb": response,
    }

    # 3. Evidence must record the request.
    assert evidence["parts"]["opening_adb_wb"]["request"] == calls[0][1]

    # 4. Quality validation must pass.
    config = load_config(resource="bid_opening")

    result = validate_detail(
        payload,
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "complete"

    assert result["identity"]["id"] == ctx["id"]
    assert result["identity"]["notifyNo"] == ctx["notifyNo"]
    assert result["identity"]["notifyVersion"] == "00"

    # 5. Submission count must be preserved.
    submissions = response["bidSubmissionByContractorViewResponse"][
        "bidSubmissionDTOList"
    ]

    assert len(submissions) == 2

    # 6. Decimal prices must not be rounded.
    assert submissions[0]["bidPrice"] == 158229576198.904
    assert submissions[1]["bidPrice"] == 176833941484.409

    # 7. Empty lot detail list is valid for this case.
    assert (
        response["bidSubmissionByContractorViewResponse"]["bidoLotOpenDetailDTOS"] == []
    )


@pytest.mark.parametrize(
    "problem",
    [
        "missing_notice",
        "missing_round",
        "wrong_uuid",
        "wrong_notify_no",
        "wrong_version",
        "wrong_process",
        "null_submissions",
        "count_mismatch",
        "invalid_lot_list",
        "missing_lot_details",
        "missing_opening_time",
    ],
)
def test_invalid_adb_single_rejected(problem):
    ctx, response = adb_single_example()

    if problem == "missing_notice":
        response["bidNoContractorResponse"]["bidNotification"] = None

    elif problem == "missing_round":
        response["bidoBidroundMngViewDTO"] = None

    elif problem == "wrong_uuid":
        response["bidoBidroundMngViewDTO"]["id"] = "wrong-uuid"

    elif problem == "wrong_notify_no":
        response["bidoBidroundMngViewDTO"]["notifyNo"] = "IB_WRONG"

    elif problem == "wrong_version":
        response["bidoBidroundMngViewDTO"]["notifyVersion"] = "01"

    elif problem == "wrong_process":
        response["bidoBidroundMngViewDTO"]["processApply"] = "LDT"

    elif problem == "null_submissions":
        response["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"] = None

    elif problem == "count_mismatch":
        response["bidoBidroundMngViewDTO"]["countContractor"] = 3

    elif problem == "invalid_lot_list":
        response["bidSubmissionByContractorViewResponse"]["bidSubmissionDTOList"][0][
            "lotNoValueDTOList"
        ] = None

    elif problem == "missing_lot_details":
        response["bidSubmissionByContractorViewResponse"].pop("bidoLotOpenDetailDTOS")

    elif problem == "missing_opening_time":
        response["bidoBidroundMngViewDTO"]["successBidOpenDate"] = None

    config = load_config(resource="bid_opening")

    result = validate_detail(
        {"opening_adb_wb": response},
        ctx,
        config.contracts["bid_opening"],
    )

    assert result["status"] == "fail", f"Invalid ADB payload was accepted: {problem}"


def test_adb_unsupported_mode_rejected():
    ctx, _ = adb_single_example()

    ctx["bidMode"] = "1_HTHS"

    class Client:
        def post(self, path, body):
            raise AssertionError("Unsupported ADB mode must not call API")

    with pytest.raises(
        DetailValidationError,
        match="unsupported_adb_opening_mode",
    ):
        BidOpeningApi(Client()).fetch(ctx, {})


def no_qualified_technical_example(dual_payload):
    data = copy.deepcopy(dual_payload)

    notice = data["notify"]["bidNoContractorResponse"]["bidNotification"]

    round_info = data["roundmng"]["bidoBidroundMngViewDTO"]

    notice["isMultiLot"] = 1

    round_info.update(
        {
            "isMultiLot": True,
            "countContractor": 2,
            "successBidOpenDateTc": "2023-10-04T10:50:32",
            "bidStatus": "PUB_KQLCNT",
            "evalStatus": "HT_HSDXTC",
        }
    )

    submissions = data["bid_open_technical"]["bidSubmissionByContractorViewResponse"][
        "bidSubmissionDTOList"
    ]

    first = submissions[0]

    first["contractorCode"] = "vn0301275262"
    first["lotNoValueDTOList"] = None

    second = copy.deepcopy(first)
    second["id"] = "technical-submission-2"
    second["contractorCode"] = "vn1702170861"

    submissions.append(second)

    lot_rows = [
        ("PP2300100187", "vn0301275262"),
        ("PP2300100187", "vn1702170861"),
        ("PP2300100188", "vn0301275262"),
        ("PP2300100189", "vn0301275262"),
        ("PP2300100190", "vn0301275262"),
    ]

    data["lot_open_detail_technical"] = [
        {
            "notifyId": notice["id"],
            "lotNo": lot_no,
            "contractorCode": contractor,
            "passedYn": "N",
            "eligibilityYn": "Y",
            "experienceYn": "Y",
        }
        for lot_no, contractor in lot_rows
    ]

    # Preserve explicit nulls returned by financial API.
    data["bid_open_financial"] = {
        "bidNoContractorResponse": None,
        "bidoBidroundMngViewDTO": None,
        "bidSubmissionByContractorViewResponse": None,
    }

    data["lot_open_detail_financial"] = []

    data["cancellation_notice"] = {
        "bidNoContractorResponse": {
            "bidNotification": copy.deepcopy(notice),
        },
        "bidCancelingResponse": {
            "id": notice["id"],
            "notifyNo": notice["notifyNo"],
            "bidNo": notice["bidNo"],
            "cancelDate": None,
            "cancelDecisionNo": None,
        },
        "bidpPlanDetail": {
            "id": notice["bidId"],
            "linkNotifyInfo": json.dumps(
                {
                    "notifyId": notice["id"],
                    "notifyNo": notice["notifyNo"],
                    "bidId": notice["bidId"],
                }
            ),
        },
    }

    ctx = {
        **context(data),
        "processApply": "LDT",
        "bidMode": "1_HTHS",
    }

    return ctx, data


def test_no_qualified_technical_bidders_accepted(dual_payload):
    ctx, data = no_qualified_technical_example(dual_payload)

    result = validate_detail(
        data,
        ctx,
        load_config(resource="bid_opening").contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "no_qualified_technical_bidders"

    assert result["identity"]["id"] == ctx["id"]
    assert result["identity"]["notifyNo"] == ctx["notifyNo"]

    technical_lots = data["lot_open_detail_technical"]

    assert len(technical_lots) == 5
    assert all(row["passedYn"] == "N" for row in technical_lots)

    financial = data["bid_open_financial"]

    assert financial["bidSubmissionByContractorViewResponse"] is None
    assert data["lot_open_detail_financial"] == []

    # Raw payload must remain unchanged by validation.
    assert (
        len(
            data["bid_open_technical"]["bidSubmissionByContractorViewResponse"][
                "bidSubmissionDTOList"
            ]
        )
        == 2
    )


@pytest.mark.parametrize(
    "problem",
    [
        "qualified_bidder",
        "missing_passed_flag",
        "missing_bidder_evaluation",
        "unknown_contractor",
        "wrong_notify_id",
        "duplicate_pair",
        "missing_financial_root",
        "unexpected_financial_lot",
        "actual_cancellation",
        "different_linked_notice",
        "partial_lot_declarations",
    ],
)
def test_invalid_no_qualified_technical_rejected(dual_payload, problem):
    ctx, data = no_qualified_technical_example(dual_payload)

    lots = data["lot_open_detail_technical"]

    financial = data["bid_open_financial"]

    cancel = data["cancellation_notice"]["bidCancelingResponse"]

    if problem == "qualified_bidder":
        lots[0]["passedYn"] = "Y"

    elif problem == "missing_passed_flag":
        lots[0].pop("passedYn")

    elif problem == "missing_bidder_evaluation":
        lots[:] = [row for row in lots if row["contractorCode"] != "vn1702170861"]

    elif problem == "unknown_contractor":
        lots[0]["contractorCode"] = "vnUNKNOWN"

    elif problem == "wrong_notify_id":
        lots[0]["notifyId"] = "wrong-uuid"

    elif problem == "duplicate_pair":
        lots.append(copy.deepcopy(lots[0]))

    elif problem == "missing_financial_root":
        financial.pop("bidSubmissionByContractorViewResponse")

    elif problem == "unexpected_financial_lot":
        data["lot_open_detail_financial"] = [
            {"notifyId": ctx["id"], "lotNo": "PP_OTHER"}
        ]

    elif problem == "actual_cancellation":
        cancel["cancelDate"] = "2023-10-05T10:00:00"
        cancel["cancelDecisionNo"] = "TEST-CANCEL"

    elif problem == "different_linked_notice":
        data["cancellation_notice"]["bidpPlanDetail"]["linkNotifyInfo"] = json.dumps(
            {
                "notifyId": "another-uuid",
                "notifyNo": "IB_OTHER",
                "bidId": data["cancellation_notice"]["bidpPlanDetail"]["id"],
            }
        )

    elif problem == "partial_lot_declarations":
        submissions = data["bid_open_technical"][
            "bidSubmissionByContractorViewResponse"
        ]["bidSubmissionDTOList"]

        submissions[0]["lotNoValueDTOList"] = [{"lotNo": "PP2300100187"}]

    result = validate_detail(
        data,
        ctx,
        load_config(resource="bid_opening").contracts["bid_opening"],
    )

    assert result["status"] == "fail", problem

    assert result.get("collection_status") != "no_qualified_technical_bidders"


def test_no_qualified_fetch_preserves_source(
    dual_payload,
):
    ctx, expected = no_qualified_technical_example(dual_payload)

    calls = []

    class Client:
        def post(self, path, body):
            calls.append((path, dict(body)))

            if path == STANDARD_NOTICE_PATH:
                return copy.deepcopy(expected["cancellation_notice"])

            component = next(key for key, value in ENDPOINTS.items() if value == path)

            if component in {
                "bid_open",
                "lot_open_detail",
            }:
                phase = "_technical" if body["packType"] == 1 else "_financial"

                component += phase

            return copy.deepcopy(expected[component])

        post_array = post

    evidence = {}

    actual = BidOpeningApi(Client()).fetch(
        ctx,
        evidence,
    )

    assert actual == expected
    assert len(calls) == 7

    assert calls[-1] == (
        STANDARD_NOTICE_PATH,
        {"id": ctx["id"]},
    )

    assert evidence["parts"]["bid_open_financial"]["request"]["packType"] == 2

    assert evidence["parts"]["cancellation_notice"]["status"] == "received"

    result = validate_detail(
        actual,
        ctx,
        load_config(resource="bid_opening").contracts["bid_opening"],
    )

    assert result["status"] == "pass"
    assert result["collection_status"] == "no_qualified_technical_bidders"
