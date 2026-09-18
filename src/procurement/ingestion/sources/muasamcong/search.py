"""Shared transport/search envelope; filter and detail contracts stay in each resource."""

from typing import Any, Protocol


class JsonPostClient(Protocol):
    def post(self, path: str, body: Any) -> dict[str, Any]: ...


def search_document_key(item: dict[str, Any]) -> str | None:
    key = item.get("id") or item.get("inputResultId")
    return str(key) if key else None


def build_search_payload(
    *, page_number: int, page_size: int, index: str, filters: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    return [
        {
            "pageSize": page_size,
            "pageNumber": str(page_number),
            "query": [
                {
                    "index": index,
                    "matchType": "all-1",
                    "matchFields": ["notifyNo", "bidName"],
                    "filters": filters,
                }
            ],
        }
    ]
