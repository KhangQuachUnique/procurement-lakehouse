"""Tests for Bronze verification functions."""

from datetime import date

import pytest

from procurement.bronze.hashing import calculate_content_hash
from procurement.bronze.verification import (
    calculate_file_sha256,
    verify_record_content_hash,
    verify_record_lineage,
)


def test_file_sha256():
    data = b"bronze parquet content"
    digest = calculate_file_sha256(data)
    assert len(digest) == 64


def test_verify_record_lineage_success():
    record = {"source_date": "2025-01-01", "run_id": "attempt-1"}
    verify_record_lineage(
        record, expected_source_date=date(2025, 1, 1), expected_run_id="attempt-1"
    )


def test_verify_record_lineage_mismatch():
    record = {"source_date": "2025-01-01", "run_id": "attempt-1"}
    with pytest.raises(ValueError, match="Lineage error"):
        verify_record_lineage(
            record, expected_source_date=date(2025, 1, 2), expected_run_id="attempt-1"
        )


def test_verify_record_content_hash():
    payload = {"k": "v"}
    record = {"payload": payload, "content_hash": calculate_content_hash(payload)}
    verify_record_content_hash(record)

    corrupted_record = {"payload": payload, "content_hash": "bad" + "0" * 61}
    with pytest.raises(ValueError, match="Hash mismatch"):
        verify_record_content_hash(corrupted_record)
