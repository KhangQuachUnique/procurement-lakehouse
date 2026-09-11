from typing import Any

import pytest

from procurement.ingestion.engine.pagination import SearchResultLimitError, iter_search_pages


class StubClient:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.calls: list[int] = []

    def search_khlcnt(self, *, page_number: int, **_: Any) -> dict[str, Any]:
        self.calls.append(page_number)
        return self.pages[page_number]


def test_iterates_until_last_page() -> None:
    client = StubClient(
        [
            {"page": {"content": [{"id": "1"}], "totalElements": 2, "last": False}},
            {"page": {"content": [{"id": "2"}], "totalElements": 2, "last": True}},
        ]
    )

    pages = list(
        iter_search_pages(
            client.search_khlcnt,
            window_from="from",
            window_to="to",
        )
    )

    assert [page_number for page_number, _ in pages] == [0, 1]
    assert client.calls == [0, 1]


def test_rejects_daily_result_at_website_limit() -> None:
    client = StubClient(
        [{"page": {"content": [], "totalElements": 10_000, "last": False}}]
    )

    with pytest.raises(SearchResultLimitError, match="10_000|10000"):
        list(
            iter_search_pages(
                client.search_khlcnt,
                window_from="from",
                window_to="to",
            )
        )
