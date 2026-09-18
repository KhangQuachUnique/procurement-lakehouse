import math
from collections.abc import Callable, Hashable, Iterator
from typing import Any

MAX_SEARCH_ITEMS = 10_000


class SearchResultLimitError(RuntimeError):
    """Raised when a search window reaches the website result limit."""


class PaginationInvariantError(RuntimeError):
    """Raised when search pagination metadata/content is internally inconsistent."""


def _optional_int(page: dict[str, Any], *field_names: str) -> int | None:
    for field_name in field_names:
        value = page.get(field_name)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise PaginationInvariantError(
                f"Invalid pagination field {field_name}={value!r}"
            ) from exc
    return None


def iter_search_pages(
    fetch_page: Callable[..., dict[str, Any]],
    *,
    window_from: str,
    window_to: str,
    page_size: int = 50,
    search_key: Callable[[dict[str, Any]], Hashable | None] | None = None,
) -> Iterator[tuple[int, dict[str, Any]]]:
    if page_size <= 0:
        raise ValueError("page_size must be greater than zero")

    page_number = 0
    expected_total_items: int | None = None
    seen: set[Hashable] = set()

    while True:
        response = fetch_page(
            page_number=page_number,
            page_size=page_size,
            window_from=window_from,
            window_to=window_to,
        )
        page = response["page"]
        content = page["content"]
        if not isinstance(content, list):
            raise PaginationInvariantError("Search page content must be a list")

        total_items = int(page["totalElements"])
        if total_items < 0:
            raise PaginationInvariantError(
                f"totalElements must be non-negative, received {total_items}"
            )
        if total_items >= MAX_SEARCH_ITEMS:
            raise SearchResultLimitError(
                f"Search returned {total_items} items; the website limit is {MAX_SEARCH_ITEMS}."
            )

        if expected_total_items is None:
            expected_total_items = total_items
        elif total_items != expected_total_items:
            raise PaginationInvariantError(
                "totalElements changed during crawl: "
                f"expected={expected_total_items}, received={total_items}, "
                f"page={page_number}"
            )

        returned_page = _optional_int(page, "number", "pageNumber", "currentPage")
        if returned_page is not None and returned_page != page_number:
            raise PaginationInvariantError(
                f"Search page mismatch: requested={page_number}, returned={returned_page}"
            )

        returned_size = _optional_int(page, "size", "pageSize")
        if returned_size is not None and returned_size != page_size:
            raise PaginationInvariantError(
                f"Search page size mismatch: requested={page_size}, returned={returned_size}"
            )

        total_pages = max(1, math.ceil(total_items / page_size))
        returned_total_pages = _optional_int(page, "totalPages")
        # Live MuaSamCong returns totalPages=0 for an empty first page.
        # Keep accepting the legacy one-page empty envelope as well.
        valid_total_pages = {0, 1} if total_items == 0 else {total_pages}
        if returned_total_pages is not None and returned_total_pages not in valid_total_pages:
            raise PaginationInvariantError(
                "Search totalPages mismatch: "
                f"expected={total_pages}, returned={returned_total_pages}"
            )

        remaining = max(total_items - page_number * page_size, 0)
        expected_items_on_page = min(page_size, remaining)
        if len(content) != expected_items_on_page:
            raise PaginationInvariantError(
                "Search page item count mismatch: "
                f"page={page_number}, expected={expected_items_on_page}, "
                f"received={len(content)}, totalElements={total_items}, "
                f"pageSize={page_size}"
            )

        if search_key is not None:
            for item in content:
                key = search_key(item)
                # Missing identity remains an extractor error with source context.
                if key is None:
                    continue
                if key in seen:
                    raise PaginationInvariantError(
                        f"Repeated search identity on page {page_number}: {key!r}"
                    )
                seen.add(key)

        yield page_number, response

        if page_number + 1 >= total_pages:
            break
        page_number += 1
