from functools import partial
from typing import Any

from procurement.common.catalog import get_resource
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.sources.muasamcong.project.extractor import iter_project_records
from procurement.ingestion.sources.muasamcong.search import (
    JsonPostClient,
    build_search_payload,
    search_document_key,
)

PROJECT_IDENTITY = get_resource("project").identity
PROJECT_INDEX = "es-contractor-selection"
PROJECT_TYPE_FILTER = "es-bidp-project-p"

SEARCH_PATH = "/o/egp-portal-contractor-selection-v2/services/smart/search"
PROJECT_DETAIL_PATH = (
    "/o/egp-portal-contractor-selection-v2/services/expose/lcnt/bid-po-bidp-project-view/get-by-id"
)


class ProjectApi:
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

    def get_project_detail(self, project_id: str) -> dict[str, Any]:
        return self._client.post(PROJECT_DETAIL_PATH, {"id": project_id})


def _build_search_payload(
    *, page_number: int, page_size: int, window_from: str, window_to: str
) -> list[dict[str, Any]]:
    return build_search_payload(
        page_number=page_number,
        page_size=page_size,
        index=PROJECT_INDEX,
        filters=[
            {
                "fieldName": "publicDate",
                "searchType": "range",
                "from": window_from,
                "to": window_to,
            },
            {"fieldName": "type", "searchType": "in", "fieldValues": [PROJECT_TYPE_FILTER]},
        ],
    )


def create_project_spec(client: JsonPostClient) -> ResourceSpec:
    api = ProjectApi(client)
    return ResourceSpec(
        identity=PROJECT_IDENTITY,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        search_key=search_document_key,
        iter_records=partial(iter_project_records, api, identity=PROJECT_IDENTITY),
    )
