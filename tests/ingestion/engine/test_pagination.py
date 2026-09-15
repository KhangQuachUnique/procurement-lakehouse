from typing import Any

import pytest

from procurement.ingestion.engine.pagination import (
    PaginationInvariantError,
    SearchResultLimitError,
    iter_search_pages,
)


class StubClient:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.calls: list[int] = []

    def search(self, *, page_number: int, **_: Any) -> dict[str, Any]:
        self.calls.append(page_number)
        return self.pages[page_number]


def _page(
    *,
    items: int,
    total: int,
    number: int | None = None,
    size: int | None = None,
    total_pages: int | None = None,
) -> dict[str, Any]:
    page: dict[str, Any] = {
        "content": [{"id": str(index)} for index in range(items)],
        "totalElements": total,
        "last": False,
    }
    if number is not None:
        page["number"] = number
    if size is not None:
        page["size"] = size
    if total_pages is not None:
        page["totalPages"] = total_pages
    return {"page": page}


def test_iterates_using_total_elements_instead_of_last_flag() -> None:
    client = StubClient(
        [
            _page(items=1, total=2),
            _page(items=1, total=2),
        ]
    )

    pages = list(
        iter_search_pages(
            client.search,
            window_from="from",
            window_to="to",
            page_size=1,
        )
    )

    assert [page_number for page_number, _ in pages] == [0, 1]
    assert client.calls == [0, 1]


def test_validates_server_page_metadata_when_present() -> None:
    client = StubClient(
        [
            _page(items=2, total=3, number=0, size=2, total_pages=2),
            _page(items=1, total=3, number=1, size=2, total_pages=2),
        ]
    )

    pages = list(
        iter_search_pages(
            client.search,
            window_from="from",
            window_to="to",
            page_size=2,
        )
    )

    assert [page_number for page_number, _ in pages] == [0, 1]


def test_rejects_server_page_size_clamp() -> None:
    client = StubClient(
        [_page(items=50, total=200, number=0, size=50, total_pages=4)]
    )

    with pytest.raises(PaginationInvariantError, match="page size mismatch"):
        list(
            iter_search_pages(
                client.search,
                window_from="from",
                window_to="to",
                page_size=100,
            )
        )


def test_rejects_short_non_terminal_page_even_without_size_metadata() -> None:
    client = StubClient([_page(items=25, total=100)])

    with pytest.raises(PaginationInvariantError, match="item count mismatch"):
        list(
            iter_search_pages(
                client.search,
                window_from="from",
                window_to="to",
                page_size=50,
            )
        )


def test_rejects_total_elements_change_between_pages() -> None:
    client = StubClient(
        [
            _page(items=2, total=4),
            _page(items=2, total=5),
        ]
    )

    with pytest.raises(PaginationInvariantError, match="totalElements changed"):
        list(
            iter_search_pages(
                client.search,
                window_from="from",
                window_to="to",
                page_size=2,
            )
        )


def test_rejects_wrong_returned_page_number() -> None:
    client = StubClient([_page(items=1, total=1, number=3)])

    with pytest.raises(PaginationInvariantError, match="page mismatch"):
        list(
            iter_search_pages(
                client.search,
                window_from="from",
                window_to="to",
                page_size=1,
            )
        )


def test_rejects_daily_result_at_website_limit() -> None:
    client = StubClient([_page(items=0, total=10_000)])

    with pytest.raises(SearchResultLimitError, match="10_000|10000"):
        list(
            iter_search_pages(
                client.search,
                window_from="from",
                window_to="to",
            )
        )
