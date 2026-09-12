import pytest

from procurement.ingestion.engine.fingerprint import (
    IncompatibleCheckpointError,
    calculate_page_fingerprint,
    calculate_query_fingerprint,
    ensure_compatible,
    ensure_page_compatible,
)


def test_query_fingerprint_is_stable_and_detects_change() -> None:
    first = calculate_query_fingerprint({"page_size": 50, "type": "plan"})
    same = calculate_query_fingerprint({"type": "plan", "page_size": 50})
    changed = calculate_query_fingerprint({"page_size": 100, "type": "plan"})
    assert first == same
    assert first != changed
    ensure_compatible({"query_fingerprint": first}, same)
    with pytest.raises(IncompatibleCheckpointError):
        ensure_compatible({"query_fingerprint": first}, changed)


def test_page_fingerprint_detects_order_drift() -> None:
    first = calculate_page_fingerprint([{"id": "a"}, {"id": "b"}])
    reordered = calculate_page_fingerprint([{"id": "b"}, {"id": "a"}])
    assert first != reordered
    ensure_page_compatible({"search_page_fingerprint": first}, first)
    with pytest.raises(IncompatibleCheckpointError):
        ensure_page_compatible({"search_page_fingerprint": first}, reordered)
