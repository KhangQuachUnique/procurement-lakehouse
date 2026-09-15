from typing import Any

from procurement.ingestion.sources.muasamcong.contractor_result.resource import (
    CONTRACTOR_RESULT_DETAIL_PATH,
    SEARCH_INDEX,
    SEARCH_PATH,
    STEP_CODE_FILTER,
    TYPE_FILTER,
    ContractorResultApi,
    create_contractor_result_spec,
)


class StubTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def post(self, path: str, body: Any) -> dict[str, Any]:
        self.calls.append((path, body))
        return {"ok": True}


def test_contractor_result_api_owns_search_and_detail_contract() -> None:
    transport = StubTransport()
    api = ContractorResultApi(transport)

    api.search(page_number=2, page_size=50, window_from="from", window_to="to")
    api.get_result_detail("result-1")

    search_path, search_body = transport.calls[0]
    query = search_body[0]["query"][0]
    filters = query["filters"]

    assert search_path == SEARCH_PATH
    assert query["index"] == SEARCH_INDEX
    assert filters == [
        {
            "fieldName": "publicDateKqlcnt",
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
            "fieldName": "stepCode",
            "searchType": "in",
            "fieldValues": [STEP_CODE_FILTER],
        },
    ]
    assert transport.calls[1] == (CONTRACTOR_RESULT_DETAIL_PATH, {"id": "result-1"})


def test_contractor_result_spec_exposes_resource_hooks() -> None:
    spec = create_contractor_result_spec(StubTransport())

    assert spec.identity.source == "muasamcong"
    assert spec.identity.resource == "contractor_result"
    assert spec.pipeline_name == "muasamcong_bronze"
    assert spec.dataset_name == "muasamcong"
