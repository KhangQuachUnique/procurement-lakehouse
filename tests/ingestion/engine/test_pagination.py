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


def test_iterates_using_total_elements_instead_of_last_flag() -> None:
    client = StubClient(
        [
            {"page": {"content": [{"id": "1"}], "totalElements": 2, "last": True}},
            {"page": {"content": [{"id": "2"}], "totalElements": 2, "last": False}},
        ]
    )

    pages = list(
        iter_search_pages(
            client.search_khlcnt,
            window_from="from",
            window_to="to",
            page_size=1,
        )
    )

    assert [page_number for page_number, _ in pages] == [0, 1]
    assert client.calls == [0, 1]


def test_stops_without_requesting_extra_page_when_last_flag_is_false() -> None:
    page_size = 50
    total_elements = 211
    pages_data = [
        {
            "page": {
                "content": [{"id": str(index)}],
                "totalElements": total_elements,
                "last": False,
            }
        }
        for index in range(5)
    ]
    client = StubClient(pages_data)

    pages = list(
        iter_search_pages(
            client.search_khlcnt,
            window_from="from",
            window_to="to",
            page_size=page_size,
        )
    )

    assert [page_number for page_number, _ in pages] == [0, 1, 2, 3, 4]
    assert client.calls == [0, 1, 2, 3, 4]


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
