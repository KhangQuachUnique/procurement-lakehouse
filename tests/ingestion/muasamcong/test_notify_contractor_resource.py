from typing import Any

from procurement.ingestion.sources.muasamcong.notify_contractor.resource import (
    CASE_KHKQ_EXCLUDED,
    REOFFER_DETAIL_PATH,
    SEARCH_INDEX,
    SEARCH_PATH,
    STANDARD_DETAIL_PATH,
    TYPE_FILTER,
    NotifyContractorApi,
    create_notify_contractor_spec,
)


class StubTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def post(self, path: str, body: Any) -> dict[str, Any]:
        self.calls.append((path, body))
        return {"ok": True}


def test_notify_contractor_api_owns_search_and_both_detail_endpoints() -> None:
    transport = StubTransport()
    api = NotifyContractorApi(transport)

    api.search(page_number=3, page_size=50, window_from="from", window_to="to")
    api.get_standard_detail("standard-1")
    api.get_reoffer_detail("reoffer-1")

    search_path, search_body = transport.calls[0]
    query = search_body[0]["query"][0]

    assert search_path == SEARCH_PATH
    assert query["index"] == SEARCH_INDEX
    assert query["filters"] == [
        {
            "fieldName": "publicDate",
            "searchType": "range",
            "from": "from",
            "to": "to",
        },
        {
            "fieldName": "type",
            "searchType": "in",
            "fieldValues": [TYPE_FILTER],
        },
        {
            "fieldName": "caseKHKQ",
            "searchType": "not_in",
            "fieldValues": [CASE_KHKQ_EXCLUDED],
        },
    ]
    assert transport.calls[1] == (STANDARD_DETAIL_PATH, {"id": "standard-1"})
    assert transport.calls[2] == (REOFFER_DETAIL_PATH, {"id": "reoffer-1"})


def test_notify_contractor_spec_plugs_into_shared_engine_without_engine_changes() -> None:
    spec = create_notify_contractor_spec(StubTransport())

    assert spec.identity.source == "muasamcong"
    assert spec.identity.resource == "notify_contractor"
    assert spec.pipeline_name == "muasamcong_bronze"
    assert spec.dataset_name == "muasamcong"
