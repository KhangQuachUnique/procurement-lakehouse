from functools import partial
from typing import Any

from procurement.common.catalog import get_resource
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.sources.muasamcong.khlcnt.extractor import iter_khlcnt_records
from procurement.ingestion.sources.muasamcong.search import (
    JsonPostClient,
    build_search_payload,
    search_document_key,
)

KHLCNT_IDENTITY = get_resource("khlcnt").identity
KHLCNT_INDEX = "es-contractor-selection"
KHLCNT_TYPE_FILTER = "es-plan-project-p"

SEARCH_PATH = "/o/egp-portal-contractor-selection-v2/services/smart/search"
PLAN_DETAIL_PATH = (
    "/o/egp-portal-contractor-selection-v2/services/expose/lcnt/"
    "bid-po-bidp-plan-project-view/get-by-id"
)
BID_PACKAGE_DETAIL_PATH = (
    "/o/egp-portal-contractor-selection-v2/services/lcnt/"
    "bid-po-bidp-plan-project-view/get-bidp-plan-detail-by-id"
)


class KhlcntApi:
    def __init__(self, client: JsonPostClient) -> None:
        self._client = client

    def search(
        self, *, page_number: int, page_size: int, window_from: str, window_to: str
    ) -> dict[str, Any]:
        return self._client.post(
            SEARCH_PATH,
            _build_search_payload(
                page_number=page_number,
                page_size=page_size,
                window_from=window_from,
                window_to=window_to,
            ),
        )

    def get_plan_detail(self, plan_id: str) -> dict[str, Any]:
        return self._client.post(PLAN_DETAIL_PATH, {"id": plan_id})

    def get_bid_package_detail(self, bid_package_id: str) -> dict[str, Any]:
        return self._client.post(BID_PACKAGE_DETAIL_PATH, {"id": bid_package_id})


def _build_search_payload(
    *, page_number: int, page_size: int, window_from: str, window_to: str
) -> list[dict[str, Any]]:
    return build_search_payload(
        page_number=page_number,
        page_size=page_size,
        index=KHLCNT_INDEX,
        filters=[
            {
                "fieldName": "publicDate",
                "searchType": "range",
                "from": window_from,
                "to": window_to,
            },
            {"fieldName": "type", "searchType": "in", "fieldValues": [KHLCNT_TYPE_FILTER]},
        ],
    )


def create_khlcnt_spec(client: JsonPostClient) -> ResourceSpec:
    api = KhlcntApi(client)
    return ResourceSpec(
        identity=KHLCNT_IDENTITY,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        search_key=search_document_key,
        iter_records=partial(iter_khlcnt_records, api, identity=KHLCNT_IDENTITY),
    )
