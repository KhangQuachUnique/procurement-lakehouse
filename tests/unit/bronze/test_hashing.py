"""Tests for Bronze content hashing determinism."""

from procurement.bronze.hashing import calculate_content_hash


def test_calculate_content_hash_deterministic():
    data1 = {"b": 2, "a": 1, "nested": {"y": "tên", "x": 10}}
    data2 = {"a": 1, "b": 2, "nested": {"x": 10, "y": "tên"}}
    assert calculate_content_hash(data1) == calculate_content_hash(data2)


def test_calculate_content_hash_preserves_unicode():
    payload = {"title": "Gói thầu xây lắp số 1", "amount": 1000000}
    hash_val = calculate_content_hash(payload)
    assert len(hash_val) == 64
    assert isinstance(hash_val, str)
