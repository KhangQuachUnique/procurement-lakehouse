from functools import partial
from typing import Any, Protocol

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.sources.muasamcong.contractor_result.extractor import (
    iter_contractor_result_records,
)

CONTRACTOR_RESULT_IDENTITY = ResourceIdentity(
    source="muasamcong",
    resource="contractor_result",
)

SEARCH_INDEX = "es-contractor-selection"
TYPE_FILTER = "es-notify-contractor"
STEP_CODE_FILTER = "notify-contractor-step-4-kqlcnt"

SEARCH_PATH = "/o/egp-portal-contractor-selection-v2/services/smart/search"
CONTRACTOR_RESULT_DETAIL_PATH = (
    "/o/egp-portal-contractor-selection-v2/services/expose/"
    "contractor-input-result/get"
)


class JsonPostClient(Protocol):
    def post(self, path: str, body: Any) -> dict[str, Any]: ...


class ContractorResultApi:
    def __init__(self, client: JsonPostClient) -> None:
        self._client = client

    def search(
        self,
        *,
        page_number: int,
        page_size: int,
        window_from: str,
        window_to: str,
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

    def get_result_detail(self, result_id: str) -> dict[str, Any]:
        return self._client.post(CONTRACTOR_RESULT_DETAIL_PATH, {"id": result_id})


def _build_search_payload(
    *,
    page_number: int,
    page_size: int,
    window_from: str,
    window_to: str,
) -> list[dict[str, Any]]:
    return [
        {
            "pageSize": page_size,
            "pageNumber": str(page_number),
            "query": [
                {
                    "index": SEARCH_INDEX,
                    "matchType": "all-1",
                    "matchFields": ["notifyNo", "bidName"],
                    "filters": [
                        {
                            "fieldName": "publicDateKqlcnt",
                            "searchType": "range",
                            "from": window_from,
                            "to": window_to,
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
                    ],
                }
            ],
        }
    ]


def create_contractor_result_spec(client: JsonPostClient) -> ResourceSpec:
    api = ContractorResultApi(client)
    return ResourceSpec(
        identity=CONTRACTOR_RESULT_IDENTITY,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        iter_records=partial(
            iter_contractor_result_records,
            api,
            identity=CONTRACTOR_RESULT_IDENTITY,
        ),
    )
