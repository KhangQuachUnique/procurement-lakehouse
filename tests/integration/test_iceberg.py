"""Real SeaweedFS + DuckDB contract, opt in with ICEBERG_TEST=1."""

import os
from decimal import Decimal
from uuid import uuid4

import pytest

from procurement.storage.iceberg import IcebergConfig, RestCatalog, connect

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv("ICEBERG_TEST") != "1", reason="requires isolated Iceberg test stack")]


def get_test_iceberg_config() -> IcebergConfig:
    from procurement.common.settings import settings

    return settings.iceberg_config()


def test_v2_crud_merge_evolution_pinned_time_travel_and_restart():
    config = get_test_iceberg_config()
    catalog = RestCatalog(config)
    name = "proof_" + uuid4().hex[:12]
    table = "lake.poc." + name
    with connect(config, install=True) as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS lake.poc")
        db.execute(f"CREATE TABLE {table} (id INTEGER, value VARCHAR)")
        db.execute(f"INSERT INTO {table} VALUES (1,'one'),(2,'two')")
        metadata = catalog.table("poc", name)["metadata"]
        assert metadata["format-version"] == 2
        original = metadata["current-snapshot-id"]
        catalog.pin("poc", name, original, name)
        db.execute(f"""MERGE INTO {table} t USING (VALUES (2,'changed'),(3,'three')) s(id,value)
            ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value
            WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.value)""")
        db.execute(f"ALTER TABLE {table} ADD COLUMN amount DECIMAL(38,6)")
        db.execute(f"UPDATE {table} SET amount=12.345678 WHERE id=2")
        db.execute(f"DELETE FROM {table} WHERE id=1")
    # New connection proves catalog persistence and recovery independent of client state.
    with connect(config) as db:
        assert db.sql(f"SELECT * FROM {table} ORDER BY id").fetchall() == [
            (2, "changed", Decimal("12.345678")), (3, "three", None)]
        assert db.sql(f"SELECT id,value FROM {table} AT (VERSION => {original}) ORDER BY id").fetchall() == [
            (1, "one"), (2, "two")]
        db.execute("BEGIN")
        db.execute(f"INSERT INTO {table} (id,value) VALUES (4,'uncommitted')")
        db.execute("ROLLBACK")
        assert db.sql(f"SELECT count(*) FROM {table}").fetchone()[0] == 2
    assert catalog.table("poc", name)["metadata"]["refs"]["release_" + name]["snapshot-id"] == original


def test_stage_partition_lifecycle_real_iceberg():
    from datetime import date
    from procurement.silver.application.warehouse import stage_partition

    config = get_test_iceberg_config()
    catalog = RestCatalog(config)
    day1 = date(2024, 5, 1)
    day2 = date(2024, 5, 2)
    rel1 = "r" + uuid4().hex[:10]

    with connect(config) as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS lake.poc")
        db.execute("DROP TABLE IF EXISTS lake.poc.notice_revision")
        db.execute("DROP VIEW IF EXISTS poc.v_latest_notice")

        # 1. Stage Notice IB1 version 00 (Day 1)
        d1_rows = [
            {"revision_id": "rev_IB1_00", "entity_id": "IB1", "notify_version": "00", "public_date": day1, "title": "Notice 1 Original"},
        ]
        s1 = stage_partition(
            db,
            catalog,
            namespace="poc",
            tables={"notice_revision": d1_rows},
            partition_date=day1,
            release_id=rel1,
        )
        assert s1["notice_revision"]["row_count"] == 1

        # 2. Stage Notice IB1 version 01 (Day 2 amendment)
        d2_rows = [
            {"revision_id": "rev_IB1_01", "entity_id": "IB1", "notify_version": "01", "public_date": day2, "title": "Notice 1 Amended"},
            {"revision_id": "rev_IB2_00", "entity_id": "IB2", "notify_version": "00", "public_date": day2, "title": "Notice 2 New"},
        ]
        s2 = stage_partition(
            db,
            catalog,
            namespace="poc",
            tables={"notice_revision": d2_rows},
            partition_date=day2,
        )
        assert s2["notice_revision"]["row_count"] == 3

        # 3. Verify Vectorized Current View: poc.v_latest_notice returns ONLY latest revision for IB1
        latest_ib1 = db.sql("SELECT revision_id, notify_version, title FROM poc.v_latest_notice WHERE entity_id = 'IB1'").fetchone()
        assert latest_ib1 == ("rev_IB1_01", "01", "Notice 1 Amended")

        # 4. Verify Historical Table preserves BOTH rev_IB1_00 and rev_IB1_01
        all_revisions_ib1 = db.sql("SELECT revision_id, notify_version FROM lake.poc.notice_revision WHERE entity_id = 'IB1' ORDER BY notify_version").fetchall()
        assert all_revisions_ib1 == [("rev_IB1_00", "00"), ("rev_IB1_01", "01")]

        # 5. Re-stage rev_IB1_01 with corrected title (True Key-Level UPSERT: replaces only rev_IB1_01)
        d2_corrected = [
            {"revision_id": "rev_IB1_01", "entity_id": "IB1", "notify_version": "01", "public_date": day2, "title": "Notice 1 Corrected Title"},
        ]
        s3 = stage_partition(
            db,
            catalog,
            namespace="poc",
            tables={"notice_revision": d2_corrected},
            partition_date=day2,
        )
        assert s3["notice_revision"]["row_count"] == 3

        # Verify updated title in view and intact history
        updated_view = db.sql("SELECT revision_id, title FROM poc.v_latest_notice WHERE entity_id = 'IB1'").fetchone()
        assert updated_view == ("rev_IB1_01", "Notice 1 Corrected Title")
        assert db.sql("SELECT count(*) FROM lake.poc.notice_revision WHERE revision_id = 'rev_IB1_00'").fetchone()[0] == 1


def test_process_silver_service_end_to_end_real_iceberg():
    import json
    from datetime import UTC, date, datetime
    from unittest.mock import MagicMock

    from procurement.silver.application.process_silver_service import ProcessSilverService
    from procurement.silver.ports.outbound.bronze_repository import (
        BronzeFileDescriptor,
        CommittedBronzePartition,
    )

    config = get_test_iceberg_config()

    mock_bronze_repo = MagicMock()
    mock_silver_repo = MagicMock()

    day = date(2024, 6, 1)
    file_desc = BronzeFileDescriptor(
        table="notify_contractor_standard_detail",
        key="bucket/bronze/test_e2e.parquet",
        sha256="abc12345",
    )
    partition = CommittedBronzePartition(
        resource="notify_contractor",
        source_date=str(day),
        run_id="run-e2e-1",
        expected_records=1,
        files=(file_desc,),
        manifest={},
    )
    mock_bronze_repo.select_committed_partitions.return_value = [partition]

    root = {
        "id": "e2e-notice-1",
        "notifyNo": "IB-E2E-001",
        "notifyVersion": "00",
        "bidName": "End to End Iceberg Notice",
        "bidPrice": 999000000,
        "bidPriceUnit": "VND",
        "publicDate": "2024-06-01T10:00:00",
    }
    row = {
        "payload": json.dumps({"bidoNotifyContractorM": root}),
        "source_id": "IB-E2E-001",
        "source_version": "00",
        "run_id": "run-e2e-1",
        "source_date": str(day),
        "ingested_at": datetime(2024, 6, 1, tzinfo=UTC),
        "content_hash": "hash-e2e",
    }
    file_dict = {"table": file_desc.table, "key": file_desc.key, "sha256": file_desc.sha256}
    mock_bronze_repo.stream_partition_records.return_value = [(file_dict, 0, row)]
    mock_silver_repo.write_candidate_tables.return_value = {
        "notice_revision": "s3://warehouse/test_cand.parquet"
    }

    service = ProcessSilverService(
        bronze_repo=mock_bronze_repo,
        silver_repo=mock_silver_repo,
        bucket="procurement-lakehouse",
        resource_name="notice",
        iceberg_config=config,
    )

    report = service.execute(start_date=day, end_date=day)
    assert report["status"] == "completed"
    assert report["iceberg_enabled"] is True
    day_entry = report["days"][0]
    assert day_entry["status"] == "completed"
    assert "notice_revision" in day_entry["iceberg"]
    assert day_entry["iceberg"]["notice_revision"]["row_count"] >= 1

    with connect(config) as db:
        res = db.sql(
            "SELECT entity_id, notify_no, notify_version, title, bid_price FROM lake.silver.notice_revision WHERE notify_no = 'IB-E2E-001'"
        ).fetchone()
        assert res is not None
        assert res[1] == "IB-E2E-001"
        assert res[2] == "00"
        assert res[3] == "End to End Iceberg Notice"
        assert Decimal("999000000") == res[4]

        # Verify vectorized view v_latest_notice on newly connected session
        from procurement.silver.application.warehouse import ensure_views
        ensure_views(db, namespace="silver")
        view_res = db.sql(
            "SELECT entity_id, notify_no, notify_version, title FROM silver.v_latest_notice WHERE notify_no = 'IB-E2E-001'"
        ).fetchone()
        assert view_res is not None
        assert view_res[1] == "IB-E2E-001"
        assert view_res[3] == "End to End Iceberg Notice"

