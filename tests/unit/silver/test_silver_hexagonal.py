"""Unit tests for Silver Hexagonal Architecture & SQLAlchemy 2.0 Schema."""

import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from procurement.silver.adapters.driven.bronze.arrow_adapter import ArrowBronzeRepositoryAdapter
from procurement.silver.adapters.driven.storage.s3_candidate_adapter import S3SilverCandidateAdapter
from procurement.silver.application.process_notice_service import ProcessNoticeService
from procurement.silver.domain.schema import (
    get_child_schemas,
    get_silver_schemas,
    metadata,
    to_pyarrow_schema,
)
from procurement.silver.ports.outbound.bronze_repository import (
    BronzeFileDescriptor,
    CommittedBronzePartition,
)


def test_sqlalchemy_metadata_and_ddl_generation():
    """Verify that SQLAlchemy metadata contains all 3 tiers of tables and generates valid DDL."""
    schemas = get_silver_schemas()
    assert len(schemas) >= 15

    # Verify Core Tier 1 Governance tables exist
    for t in ("observation", "quality_issue", "quarantine"):
        assert t in schemas
        assert "VARCHAR" in schemas[t]

    # Verify obsolete legacy graph tables are completely removed
    for legacy in (
        "entity",
        "entity_revision",
        "revision_observation",
        "current_entity",
        "relationship",
        "child",
    ):
        assert legacy not in schemas

    # Verify Core Tier 2 tables exist and have appropriate types
    for t in (
        "project_revision",
        "plan_revision",
        "package_revision",
        "notice_revision",
        "result_revision",
        "opening_revision",
    ):
        assert t in schemas
        assert "DECIMAL(38,6)" in schemas[t] or t == "opening_revision"

    # Verify Tier 3 Typed Sub-Entities exist
    child_schemas = get_child_schemas()
    for child in (
        "notice_lot",
        "notice_location",
        "notice_reoffer_submission",
        "project_location",
        "package_lot",
        "award_lot",
    ):
        assert child in child_schemas


def test_pyarrow_schema_generation():
    """Verify that to_pyarrow_schema produces valid PyArrow schemas with high-precision types."""
    notice_table = metadata.tables["silver.notice_revision"]
    arrow_schema = to_pyarrow_schema(notice_table)

    assert arrow_schema.field("bid_price").type == pa.decimal128(38, 6)
    assert arrow_schema.field("published_at").type == pa.timestamp("us", tz="UTC")
    assert arrow_schema.field("public_date").type == pa.date32()
    assert arrow_schema.field("is_internet").type == pa.bool_()
    assert arrow_schema.field("notify_no").type == pa.string()


def test_process_notice_service_isolated_with_mock_ports():
    """Verify ProcessNoticeService executes end-to-end using decoupled ports."""
    mock_bronze_repo = MagicMock()
    mock_silver_repo = MagicMock()

    day = date(2025, 1, 1)
    file_desc = BronzeFileDescriptor(
        table="notify_contractor_standard_detail",
        key="bucket/bronze/test.parquet",
        sha256="abc",
    )
    partition = CommittedBronzePartition(
        resource="notify_contractor",
        source_date=str(day),
        run_id="run-1",
        expected_records=1,
        files=(file_desc,),
        manifest={},
    )
    mock_bronze_repo.select_committed_partitions.return_value = [partition]

    root = {
        "id": "notice-1",
        "notifyNo": "IB-001",
        "notifyVersion": "00",
        "bidName": "Test Notice",
        "bidPrice": 5000000,
        "bidPriceUnit": "VND",
    }
    row = {
        "payload": json.dumps({"bidoNotifyContractorM": root}),
        "source_id": "IB-001",
        "source_version": "00",
        "run_id": "run-1",
        "source_date": str(day),
        "ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
        "content_hash": "hash123",
    }
    file_dict = {"table": file_desc.table, "key": file_desc.key, "sha256": file_desc.sha256}
    mock_bronze_repo.stream_partition_records.return_value = [(file_dict, 0, row)]
    mock_silver_repo.write_candidate_tables.return_value = {
        "notice_revision": "s3://bucket/test.parquet"
    }

    progress_entries = []
    service = ProcessNoticeService(
        bronze_repo=mock_bronze_repo,
        silver_repo=mock_silver_repo,
        bucket="test-bucket",
    )

    report = service.execute(
        start_date=day,
        end_date=day,
        on_progress=lambda e: progress_entries.append(e),
    )

    assert report["status"] == "completed"
    assert len(report["days"]) == 1
    assert report["days"][0]["status"] == "completed"
    assert report["days"][0]["input_counts"]["standard"] == 1
    assert len(progress_entries) == 1
    assert mock_silver_repo.save_run_report.called
    assert mock_silver_repo.write_candidate_tables.called


def test_arrow_adapters_in_memory_integration():
    """Verify ArrowBronzeRepositoryAdapter and S3SilverCandidateAdapter together in memory."""
    fs = fsspec.filesystem("memory")
    day = date(2025, 2, 1)
    bucket = "test-lakehouse"

    # 1. Prepare raw Bronze parquet in memory
    raw_payload = {
        "bidoNotifyContractorM": {
            "id": "test-id",
            "notifyNo": "TBMT-999",
            "notifyVersion": "00",
            "bidName": "Memory Test Package",
            "bidPrice": 99000,
            "bidPriceUnit": "VND",
        }
    }
    bronze_row = {
        "payload": json.dumps(raw_payload),
        "source_id": "TBMT-999",
        "source_version": "00",
        "run_id": "bronze-r1",
        "source_date": str(day),
        "ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
        "content_hash": "hash-999",
    }
    bronze_key = f"{bucket}/bronze/notify_contractor_standard_detail/{day}.parquet"
    with fs.open(bronze_key, "wb") as f:
        pq.write_table(pa.Table.from_pylist([bronze_row]), f)

    file_desc = BronzeFileDescriptor(
        table="notify_contractor_standard_detail",
        key=bronze_key,
        sha256="test-sha",
    )

    # 2. Test ArrowBronzeRepositoryAdapter
    bronze_adapter = ArrowBronzeRepositoryAdapter(
        fs=fs,
        freeze_fn=lambda _fs, _res, _s, _e: [
            {
                "resource": "notify_contractor",
                "source_date": str(day),
                "run_id": "bronze-r1",
                "expected_records": 1,
                "files": [
                    {"table": file_desc.table, "key": file_desc.key, "sha256": file_desc.sha256}
                ],
            }
        ],
    )
    partitions = bronze_adapter.select_committed_partitions(["notify_contractor"], day, day)
    assert len(partitions) == 1
    assert partitions[0].expected_records == 1

    records_streamed = list(bronze_adapter.stream_partition_records(partitions[0]))
    assert len(records_streamed) == 1
    assert records_streamed[0][2]["source_id"] == "TBMT-999"

    # 3. Test S3SilverCandidateAdapter
    silver_adapter = S3SilverCandidateAdapter(fs=fs)
    prefix = f"{bucket}/silver_candidates/test-run"
    dummy_table = pa.Table.from_pydict({"test_col": ["val1"]})
    written = silver_adapter.write_candidate_tables(prefix, day, {"notice_revision": dummy_table})
    assert "notice_revision" in written
    assert fs.exists(written["notice_revision"])

    report_path = silver_adapter.save_run_report(prefix, {"status": "completed"})
    assert fs.exists(report_path)
