"""Unit tests for direct PyArrow 3-tier schema validation and coercion."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from procurement.silver.application.arrow_typing import (
    get_pyarrow_schema,
    typed_arrow_rows_pyarrow,
)


def _base_observation(idx: int = 0, **overrides) -> dict:
    row = {
        "observation_id": f"obs-{idx}",
        "resource": "contractor_result",
        "source_date": date(2025, 5, 15),
        "run_id": "run-1",
        "table_name": "contractor_result_detail",
        "file_key": "bronze/part-0.parquet",
        "file_sha256": "a" * 64,
        "row_ordinal": idx,
        "observed_at": datetime(2025, 5, 15, 3, 30, 0, tzinfo=timezone.utc),
        "source_id": f"IB-{idx}",
        "source_version": "01",
        "content_hash": "b" * 64,
    }
    row.update(overrides)
    return row


def test_empty_rows_returns_cached_empty_table():
    t1 = typed_arrow_rows_pyarrow("observation", [])
    t2 = typed_arrow_rows_pyarrow("observation", [])
    assert t1.num_rows == 0
    assert t1.schema.equals(get_pyarrow_schema("observation"))
    assert t1 is t2


def test_tier1_direct_cpp_fast_path():
    tbl = typed_arrow_rows_pyarrow("observation", [_base_observation(1)])
    assert tbl.num_rows == 1
    assert tbl.schema.equals(get_pyarrow_schema("observation"))
    assert tbl["source_date"].to_pylist() == [date(2025, 5, 15)]


def test_tier2_vectorized_iso_string_casting():
    rows = [
        _base_observation(
            0,
            source_date="2025-05-15",
            observed_at="2025-05-15T10:30:00+07:00",
        ),
        _base_observation(
            1,
            source_date="2025-05-16",
            observed_at="2025-05-16T00:00:00Z",
        ),
    ]
    tbl = typed_arrow_rows_pyarrow("observation", rows)
    assert tbl.num_rows == 2
    assert tbl["source_date"].to_pylist() == [date(2025, 5, 15), date(2025, 5, 16)]
    assert tbl["observed_at"].to_pylist() == [
        datetime(2025, 5, 15, 3, 30, tzinfo=timezone.utc),
        datetime(2025, 5, 16, 0, 0, tzinfo=timezone.utc),
    ]


def test_tier3_mixed_types_and_decimal_quantize_fallback():
    rows = [
        {
            "revision_id": "rev-1",
            "entity_id": "ent-1",
            "entity_type": "result",
            "is_current": "yes",
            "decision_date": date(2025, 5, 14),
            "total_winning_price": Decimal("100.1234567"),
        },
        {
            "revision_id": "rev-2",
            "entity_id": "ent-2",
            "entity_type": "result",
            "is_current": False,
            "decision_date": "2025-05-15",
            "total_winning_price": "200.5",
        },
    ]
    tbl = typed_arrow_rows_pyarrow("result_revision", rows)
    assert tbl.num_rows == 2
    assert tbl["is_current"].to_pylist() == [True, False]
    assert tbl["decision_date"].to_pylist() == [date(2025, 5, 14), date(2025, 5, 15)]
    assert tbl["total_winning_price"].to_pylist() == [
        Decimal("100.123457"),
        Decimal("200.500000"),
    ]


def test_not_null_constraint_raises_clear_value_error():
    bad_row = _base_observation(0, run_id=None)
    with pytest.raises(ValueError, match="NOT NULL constraint failed for observation.run_id"):
        typed_arrow_rows_pyarrow("observation", [bad_row])


def test_invalid_date_raises_clear_value_error():
    bad_row = _base_observation(0, source_date="not-a-date")
    with pytest.raises(ValueError, match="observation.source_date"):
        typed_arrow_rows_pyarrow("observation", [bad_row])
