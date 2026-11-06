"""End-to-end Bid Opening Silver pipeline tests using in-memory object storage.

These tests do not connect to S3, SeaweedFS, PostgreSQL, or Iceberg.
"""

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.silver import service
from procurement.silver.adapters.driven.bronze.arrow_adapter import file_sha256
from procurement.silver.application.resources import get_silver_resource


@pytest.fixture
def in_memory_opening(monkeypatch):
    """Create one committed-like Bronze Parquet partition in memory."""
    fs = fsspec.filesystem("memory")
    bucket = f"silver-bid-opening-e2e-{uuid4().hex}"
    day = date(2026, 10, 2)
    run_id = "bronze-opening-test-run"

    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "ingestion"
        / "muasamcong"
        / "fixtures"
        / "bid_opening.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    root = payload["notify"]["bidNoContractorResponse"]["bidNotification"]

    def run_one(input_payload):
        key = f"{bucket}/bronze/bid_opening_detail/source_date={day}/data.parquet"
        row = {
            "payload": json.dumps(input_payload, ensure_ascii=False),
            "source_id": root["notifyNo"],
            "source_version": root["notifyVersion"],
            "run_id": run_id,
            "source_date": str(day),
            "ingested_at": datetime(2026, 10, 2, 3, 30, tzinfo=UTC),
            "content_hash": calculate_content_hash(input_payload),
        }
        with fs.open(key, "wb") as stream:
            pq.write_table(pa.Table.from_pylist([row]), stream)
        sha256 = file_sha256(fs, key)

        def frozen_partition(_fs, resources, start, end):
            assert resources == ["bid_opening"]
            assert start == end == day
            return [
                {
                    "resource": "bid_opening",
                    "source_date": str(day),
                    "run_id": run_id,
                    "expected_records": 1,
                    "manifest": {},
                    "files": [
                        {
                            "table": "bid_opening_detail",
                            "key": key,
                            "sha256": sha256,
                        }
                    ],
                }
            ]

        monkeypatch.setattr(service, "freeze", frozen_partition)
        report = service.run_resource(
            fs,
            bucket,
            "bid_opening",
            day,
            day,
            iceberg=False,
        )
        return report, row, sha256

    def read_candidate(report, table):
        path = report["days"][0]["files"][table]
        assert fs.exists(path), f"Candidate file missing: {path}"
        with fs.open(path, "rb") as stream:
            return pq.ParquetFile(stream).read().to_pylist()

    return fs, bucket, day, payload, run_one, read_candidate


def test_bid_opening_bronze_to_candidate_in_memory(in_memory_opening):
    fs, _bucket, _day, payload, run_one, read_candidate = in_memory_opening
    report, bronze_row, sha256 = run_one(payload)

    assert report["status"] == "completed", report
    assert report["iceberg_enabled"] is False

    entry = report["days"][0]
    assert entry["status"] == "completed", entry
    assert entry["quality"]["accepted"] == 1
    assert entry["quality"]["quarantined"] == 0
    assert entry["quality"]["blocking_issues"] == 0

    expected_tables = set(get_silver_resource("bid_opening").output_tables)
    assert set(entry["files"]) == expected_tables

    observations = read_candidate(report, "observation")
    revisions = read_candidate(report, "opening_revision")
    phases = read_candidate(report, "opening_phase")
    submissions = read_candidate(report, "opening_submission")
    lots = read_candidate(report, "opening_lot_submission")
    quarantines = read_candidate(report, "quarantine")

    assert len(observations) == len(revisions) == len(phases) == 1
    assert len(submissions) == 3
    assert len(lots) == 3
    assert quarantines == []

    obs = observations[0]
    rev = revisions[0]
    assert obs["source_id"] == bronze_row["source_id"]
    assert obs["source_version"] == bronze_row["source_version"]
    assert obs["content_hash"] == bronze_row["content_hash"]
    assert obs["file_sha256"] == sha256
    assert rev["notify_no"] == bronze_row["source_id"]
    assert rev["observation_id"] == obs["observation_id"]
    assert rev["is_current"] is True

    rid = rev["revision_id"]
    assert phases[0]["revision_id"] == rid
    assert all(item["revision_id"] == rid for item in submissions)
    assert all(item["revision_id"] == rid for item in lots)

    submission_ids = {item["submission_id"] for item in submissions}
    assert all(item["submission_id"] in submission_ids for item in lots)

    expected_prices = {
        item["contractorCode"]: Decimal(str(item["lotPrice"]))
        for item in payload["lot_open_detail"]
    }
    assert {
        item["contractor_code"]: item["lot_price"]
        for item in lots
    } == expected_prices

    report_path = f"{report['output_prefix']}/report.json"
    assert fs.exists(report_path)
    with fs.open(report_path, "r") as stream:
        saved_report = json.load(stream)
    assert saved_report["status"] == "completed"


def test_bid_opening_quarantine_blocks_candidate_in_memory(in_memory_opening):
    fs, bucket, day, payload, run_one, _read_candidate = in_memory_opening
    invalid_payload = deepcopy(payload)
    del invalid_payload["notify"]["bidNoContractorResponse"]["bidNotification"]["notifyNo"]

    report, _row, _sha256 = run_one(invalid_payload)

    assert report["status"] == "needs_review", report
    entry = report["days"][0]
    assert entry["status"] == "needs_review", entry
    assert entry["quality"]["quarantined"] == 1
    assert entry["quality"]["blocking_issues"] >= 1
    assert entry["files"] == {}
    assert entry["iceberg"] is None

    for table in get_silver_resource("bid_opening").output_tables:
        candidate_path = f"{bucket}/silver/{table}/source_date={day}/data.parquet"
        assert not fs.exists(candidate_path), candidate_path

    assert fs.exists(f"{report['output_prefix']}/report.json")
