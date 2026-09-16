from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


def ordered_parallel_map(
    function: Callable[[InputT], OutputT],
    items: Sequence[InputT],
    *,
    max_workers: int,
) -> list[OutputT]:
    """Execute independent I/O work concurrently while preserving input order.

    Detail fetchers use this helper so source extractors can overlap network latency
    without changing the deterministic ordering of Bronze records produced from one
    search page. Request pacing remains the responsibility of the shared source client.
    """

    if max_workers <= 0:
        raise ValueError("max_workers must be greater than zero")
    if len(items) <= 1 or max_workers == 1:
        return [function(item) for item in items]

    worker_count = min(max_workers, len(items))
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="detail-fetch",
    ) as executor:
        return list(executor.map(function, items))
