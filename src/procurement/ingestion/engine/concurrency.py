import logging
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from typing import TypeVar

logger = logging.getLogger(__name__)

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

    item_count = len(items)
    started_at = perf_counter()
    if item_count <= 1 or max_workers == 1:
        results = [function(item) for item in items]
        worker_count = 1 if item_count else 0
    else:
        worker_count = min(max_workers, item_count)
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="detail-fetch",
        ) as executor:
            results = list(executor.map(function, items))

    logger.info(
        "detail_batch_completed items=%s workers=%s elapsed_ms=%.2f",
        item_count,
        worker_count,
        (perf_counter() - started_at) * 1000,
    )
    return results
