from typing import Any

from procurement.ingestion.sources.muasamcong.project.resource import (
    PROJECT_DETAIL_PATH,
    PROJECT_INDEX,
    PROJECT_TYPE_FILTER,
    SEARCH_PATH,
    ProjectApi,
    create_project_spec,
)


class StubTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def post(self, path: str, body: Any) -> dict[str, Any]:
        self.calls.append((path, body))
        return {"ok": True}


def test_project_api_owns_resource_endpoints_and_search_payload() -> None:
    transport = StubTransport()
    api = ProjectApi(transport)
    api.search(page_number=2, page_size=50, window_from="from", window_to="to")
    api.get_project_detail("project-1")

    search_path, search_body = transport.calls[0]
    query = search_body[0]["query"][0]
    filters = query["filters"]

    assert search_path == SEARCH_PATH
    assert query["index"] == PROJECT_INDEX
    assert filters == [
        {
            "fieldName": "publicDate",
            "searchType": "range",
            "from": "from",
            "to": "to",
        },
        {
            "fieldName": "type",
            "searchType": "in",
            "fieldValues": [PROJECT_TYPE_FILTER],
        },
    ]
    assert transport.calls[1] == (PROJECT_DETAIL_PATH, {"id": "project-1"})


def test_project_spec_exposes_only_source_ingestion_hooks() -> None:
    spec = create_project_spec(StubTransport())
    assert spec.identity.source == "muasamcong"
    assert spec.identity.resource == "project"
    assert spec.pipeline_name == "muasamcong_bronze"
    assert spec.dataset_name == "muasamcong"
    assert not hasattr(spec, "retry_error")
    assert not hasattr(spec, "build_query_definition")
