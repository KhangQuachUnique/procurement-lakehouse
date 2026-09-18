from functools import partial
from typing import Any

from procurement.common.catalog import get_resource
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.sources.muasamcong.notify_contractor.extractor import (
    iter_notify_contractor_records,
)
from procurement.ingestion.sources.muasamcong.search import (
    JsonPostClient,
    build_search_payload,
    search_document_key,
)

NOTIFY_CONTRACTOR_IDENTITY = get_resource("notify_contractor").identity

SEARCH_INDEX = "es-contractor-selection"
TYPE_FILTER = "es-notify-contractor"
CASE_KHKQ_EXCLUDED = "1"

SEARCH_PATH = "/o/egp-portal-contractor-selection-v2/services/smart/search"
STANDARD_DETAIL_PATH = "/o/egp-portal-contractor-selection-v2/services/lcnt_tbmt_ttc_ldt"
REOFFER_DETAIL_PATH = "/o/egp-portal-contractor-selection-v2/services/online-reoffer/detail"


class NotifyContractorApi:
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

    def get_standard_detail(self, notice_id: str) -> dict[str, Any]:
        return self._client.post(STANDARD_DETAIL_PATH, {"id": notice_id})

    def get_reoffer_detail(self, notice_id: str) -> dict[str, Any]:
        return self._client.post(REOFFER_DETAIL_PATH, {"id": notice_id})


def _build_search_payload(
    *,
    page_number: int,
    page_size: int,
    window_from: str,
    window_to: str,
) -> list[dict[str, Any]]:
    return build_search_payload(
        page_number=page_number,
        page_size=page_size,
        index=SEARCH_INDEX,
        filters=[
            {
                "fieldName": "publicDate",
                "searchType": "range",
                "from": window_from,
                "to": window_to,
            },
            {"fieldName": "type", "searchType": "in", "fieldValues": [TYPE_FILTER]},
            {"fieldName": "caseKHKQ", "searchType": "not_in", "fieldValues": [CASE_KHKQ_EXCLUDED]},
        ],
    )


def create_notify_contractor_spec(client: JsonPostClient) -> ResourceSpec:
    api = NotifyContractorApi(client)
    return ResourceSpec(
        identity=NOTIFY_CONTRACTOR_IDENTITY,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        search_key=search_document_key,
        iter_records=partial(
            iter_notify_contractor_records,
            api,
            identity=NOTIFY_CONTRACTOR_IDENTITY,
        ),
    )
