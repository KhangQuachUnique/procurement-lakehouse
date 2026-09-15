from functools import partial
from typing import Any, Protocol

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.sources.muasamcong.project.extractor import iter_project_records

PROJECT_IDENTITY = ResourceIdentity(source="muasamcong", resource="project")
PROJECT_INDEX = "es-contractor-selection"
PROJECT_TYPE_FILTER = "es-bidp-project-p"

SEARCH_PATH = "/o/egp-portal-contractor-selection-v2/services/smart/search"
PROJECT_DETAIL_PATH = (
    "/o/egp-portal-contractor-selection-v2/services/expose/lcnt/"
    "bid-po-bidp-project-view/get-by-id"
)


class JsonPostClient(Protocol):
    def post(self, path: str, body: Any) -> dict[str, Any]: ...


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
    return [
        {
            "pageSize": page_size,
            "pageNumber": str(page_number),
            "query": [
                {
                    "index": PROJECT_INDEX,
                    "matchType": "all-1",
                    "matchFields": ["notifyNo", "bidName"],
                    "filters": [
                        {
                            "fieldName": "publicDate",
                            "searchType": "range",
                            "from": window_from,
                            "to": window_to,
                        },
                        {
                            "fieldName": "type",
                            "searchType": "in",
                            "fieldValues": [PROJECT_TYPE_FILTER],
                        },
                    ],
                }
            ],
        }
    ]


def create_project_spec(client: JsonPostClient) -> ResourceSpec:
    api = ProjectApi(client)
    return ResourceSpec(
        identity=PROJECT_IDENTITY,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        iter_records=partial(iter_project_records, api, identity=PROJECT_IDENTITY),
    )
