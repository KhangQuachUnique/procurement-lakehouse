import time

import pytest

from procurement.ingestion.engine.concurrency import ordered_parallel_map


def test_ordered_parallel_map_preserves_input_order() -> None:
    def delayed(value: int) -> int:
        time.sleep((4 - value) * 0.01)
        return value * 10

    result = ordered_parallel_map(delayed, [1, 2, 3], max_workers=3)

    assert result == [10, 20, 30]


def test_ordered_parallel_map_rejects_non_positive_worker_count() -> None:
    with pytest.raises(ValueError, match="max_workers must be greater than zero"):
        ordered_parallel_map(lambda value: value, [1], max_workers=0)
