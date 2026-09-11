from typing import Any

from procurement.ingestion.muasamcong.resources.khlcnt import (
    BID_PACKAGE_DETAIL_PATH,
    KHLCNT_INDEX,
    KHLCNT_TYPE_FILTER,
    PLAN_DETAIL_PATH,
    SEARCH_PATH,
    KhlcntApi,
    create_khlcnt_spec,
)


class StubTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def post(self, path: str, body: Any) -> dict[str, Any]:
        self.calls.append((path, body))
        return {"ok": True}


def test_khlcnt_api_owns_resource_endpoints_and_search_payload() -> None:
    transport = StubTransport()
    api = KhlcntApi(transport)

    api.search(
        page_number=2,
        page_size=50,
        window_from="from",
        window_to="to",
    )
    api.get_plan_detail("plan-1")
    api.get_bid_package_detail("package-1")

    search_path, search_body = transport.calls[0]
    assert search_path == SEARCH_PATH
    assert search_body[0]["query"][0]["index"] == KHLCNT_INDEX
    assert search_body[0]["query"][0]["filters"][1]["fieldValues"] == [
        KHLCNT_TYPE_FILTER
    ]
    assert transport.calls[1] == (PLAN_DETAIL_PATH, {"id": "plan-1"})
    assert transport.calls[2] == (BID_PACKAGE_DETAIL_PATH, {"id": "package-1"})


def test_khlcnt_spec_contains_only_resource_configuration() -> None:
    spec = create_khlcnt_spec(StubTransport())

    assert spec.identity.source == "muasamcong"
    assert spec.identity.resource == "khlcnt"
    assert spec.pipeline_name == "muasamcong_bronze"
