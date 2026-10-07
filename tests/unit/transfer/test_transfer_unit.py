"""Unit tests for the transfer package."""

from datetime import date
from io import BytesIO

import pytest

from procurement.transfer import (
    CHUNK_SIZE,
    MAX_INDEX_BYTES,
    MAX_JSON_BYTES,
    BundleDay,
    ObjectDigest,
    copy_digest,
    safe_key,
    year_dates,
)


def test_chunk_size_and_limits():
    assert CHUNK_SIZE == 1024 * 1024
    assert MAX_JSON_BYTES > 0
    assert MAX_INDEX_BYTES > MAX_JSON_BYTES


def test_safe_key_valid():
    safe_key("bronze/muasamcong/table/data.parquet")
    safe_key("_control/muasamcong/table/run_id=123/day.json")


@pytest.mark.parametrize("invalid_key", ["", "a\\b", "c:d", "e\x00f", "../secret", "a//b", "a/./b"])
def test_safe_key_invalid(invalid_key: str):
    with pytest.raises(ValueError, match="Unsafe archive object path"):
        safe_key(invalid_key)


def test_copy_digest_hashing():
    payload = b"test payload for checksum calculation"
    reader = BytesIO(payload)
    writer = BytesIO()
    digest = copy_digest(reader, writer)

    assert isinstance(digest, ObjectDigest)
    assert digest.size == len(payload)
    assert len(digest.sha256) == 64
    assert writer.getvalue() == payload


def test_year_dates_validation():
    # Valid closed year (e.g. 2024)
    dates = year_dates(2024)
    assert len(dates) == 366  # 2024 was a leap year
    assert dates[0] == date(2024, 1, 1)
    assert dates[-1] == date(2024, 12, 31)

    # Future or current year should raise ValueError
    with pytest.raises(ValueError, match="closed calendar year"):
        year_dates(2099)


def test_bundle_day_properties():
    day = BundleDay(
        resource="bid_opening",
        source_date=date(2024, 6, 1),
        run_id="run-123",
        objects=[
            "_control/muasamcong/bid_opening/run_id=run-123/run.json",
            "bronze/muasamcong/table1/source_date=2024-06-01/run_id=run-123/part-0.parquet",
        ],
    )
    assert "run_id=run-123" in day.control_prefix
    assert "source_date=2024-06-01" in day.day_key
    assert "run.json" in day.run_key
    assert len(day.parquet_files()) == 1
    assert day.parquet_files()[0][0] == "table1"
