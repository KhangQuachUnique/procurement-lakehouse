import math
from collections.abc import Callable, Iterator
from typing import Any

MAX_SEARCH_ITEMS = 10_000


class SearchResultLimitError(RuntimeError):
    """Raised when a search window reaches the website result limit."""


def iter_search_pages(
    fetch_page: Callable[..., dict[str, Any]],
    *,
    window_from: str,
    window_to: str,
    page_size: int = 50,
) -> Iterator[tuple[int, dict[str, Any]]]:
    page_number = 0
    while True:
        response = fetch_page(
            page_number=page_number,
            page_size=page_size,
            window_from=window_from,
            window_to=window_to,
        )
        page = response["page"]
        total_items = int(page["totalElements"])
        if total_items >= MAX_SEARCH_ITEMS:
            raise SearchResultLimitError(
                f"Search returned {total_items} items; "
                f"the website limit is {MAX_SEARCH_ITEMS}."
            )

        total_pages = max(1, math.ceil(total_items / page_size))
        yield page_number, response

        # MuaSamCong's `last` flag is not reliable. Derive the terminal page from
        # totalElements and the requested page size instead so we never ingest an
        # extra empty page or stop early because of source pagination metadata.
        if page_number + 1 >= total_pages:
            break
        page_number += 1
