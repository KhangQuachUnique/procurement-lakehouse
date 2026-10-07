"""Unit tests for procurement.compaction package."""

from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.compaction.planner import validate_plan
from procurement.compaction.verification import (
    copy_file,
    digest_file,
    verify_multiset,
    verify_schema,
)


def test_copy_file_and_digest(tmp_path: Path):
    content = b"hello bronze compaction"
    source = BytesIO(content)
    target = BytesIO()

    res = copy_file(source, target)
    assert res["size"] == len(content)
    assert len(res["sha256"]) == 64
    assert target.getvalue() == content

    local_file = tmp_path / "sample.bin"
    local_file.write_bytes(content)
    digest = digest_file(local_file)
    assert digest["size"] == len(content)
    assert digest["sha256"] == res["sha256"]


def test_verify_schema_requires_bronze_columns(tmp_path: Path):
    table = pa.Table.from_pydict({"col1": [1, 2]})
    file_path = tmp_path / "invalid.parquet"
    pq.write_table(table, file_path)

    with pytest.raises(ValueError, match="Missing Bronze column"):
        verify_schema([file_path])


def test_verify_multiset_matches_identical_tables(tmp_path: Path):
    schema = pa.schema(
        [
            ("run_id", pa.string()),
            ("source_date", pa.string()),
            ("source_id", pa.string()),
            ("source_version", pa.string()),
            ("ingested_at", pa.timestamp("us")),
            ("content_hash", pa.string()),
            ("payload", pa.string()),
        ]
    )
    data = {
        "run_id": ["r1"],
        "source_date": ["2022-01-01"],
        "source_id": ["1"],
        "source_version": ["01"],
        "ingested_at": [1640995200000000],
        "content_hash": ["h1"],
        "payload": ["{}"],
    }
    t1 = pa.Table.from_pydict(data, schema=schema)
    p1 = tmp_path / "p1.parquet"
    pq.write_table(t1, p1)

    t2 = pa.Table.from_pydict({**data, "run_id": ["r2"]}, schema=schema)
    p2 = tmp_path / "p2.parquet"
    pq.write_table(t2, p2)

    # multiset ignores run_id
    verify_multiset([p1], [p2], tmp_path / "scratch")


def test_validate_plan_rejects_corrupted_plan():
    plan = {
        "format_version": 1,
        "plan_id": "0123456789abcdef0123456789abcdef",
        "storage_namespace": "ns",
        "created_at": "2026-10-07T00:00:00Z",
        "start": "2022-01-01",
        "end": "2022-01-01",
        "target_bytes": 1024,
        "compression": "zstd",
        "days": [],
        "skipped": [],
        "plan_hash": "invalid-hash",
    }
    with pytest.raises(ValueError, match="Plan changed or belongs to another storage namespace"):
        validate_plan(plan)
