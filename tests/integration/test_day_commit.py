"""Real DLT load + manifest round trip; S3 uses a disposable, uniquely named bucket."""

import json
import uuid
from datetime import date

import pyarrow.parquet as pq
import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine import daily_runner
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.engine.records import build_bronze_item
from procurement.models.control import DayStatus
from procurement.storage.control import read_day_manifest

pytestmark = pytest.mark.integration



def test_real_load_preserves_partition_payload_and_day_commit(store):
    fs, bucket = store
    identity = ResourceIdentity("muasamcong", "integration")
    source_date = date(2026, 9, 1)
    payload = {"id": "record-1", "nested": {"items": [1, 2]}, "name": "Ä‘áº¥u tháº§u"}

    def fetch(**_):
        return {"page": {"content": [{"id": "record-1"}], "totalElements": 1}}

    def records(*, run_id, source_date, stats, **_):
        for table in ("plan", "package"):
            stats.record(table)
            yield build_bronze_item(
                table=table,
                source_id="record-1",
                source_version=None,
                payload=payload,
                run_id=run_id,
                source_date=source_date,
            )

    spec = ResourceSpec(identity, "integration_bronze", "muasamcong", fetch, records)
    run_id = uuid.uuid4().hex
    result = daily_runner.run_daily_resource(
        fs=fs,
        spec=spec,
        run_id=run_id,
        source_date=source_date,
        page_size=50,
    )
    manifest = read_day_manifest(fs, identity, run_id, source_date)
    assert manifest.status is DayStatus.SUCCESS
    assert result["bronze_records"] == manifest.bronze_records == 2
    for table in ("plan", "package"):
        files = fs.glob(
            f"{bucket}/bronze/muasamcong/{table}/source_date={source_date}/run_id={run_id}/*.parquet"
        )
        assert len(files) == 1
        with fs.open(files[0], "rb") as file:
            data = pq.ParquetFile(file).read().to_pylist()
        assert len(data) == 1
        record = data[0]
        assert record["run_id"] == run_id
        assert str(record["source_date"])[:10] == source_date.isoformat()
        assert record["source_version"] is None
        actual_payload = record["payload"]
        if isinstance(actual_payload, str):
            actual_payload = json.loads(actual_payload)
        assert actual_payload == payload


def test_partial_upload_is_failed_and_retry_keeps_both_attempts(store):
    fs, bucket = store
    identity = ResourceIdentity("muasamcong", "integration")
    source_date = date(2026, 9, 1)
    fail_second_page = True
    calls = []

    def fetch(*, page_number, **_):
        calls.append(page_number)
        if fail_second_page and page_number == 1:
            raise RuntimeError("search failed after first upload")
        return {"page": {"content": [{"id": str(page_number)}], "totalElements": 2}}

    def records(*, search_items, run_id, source_date, stats, **_):
        stats.record("detail")
        yield build_bronze_item(
            table="detail", source_id=search_items[0]["id"], source_version=None,
            payload=search_items[0], run_id=run_id, source_date=source_date,
        )

    spec = ResourceSpec(identity, "integration_bronze", "muasamcong", fetch, records)
    old_run, new_run = uuid.uuid4().hex, uuid.uuid4().hex
    first = daily_runner.run_daily_resource(
        fs=fs, spec=spec, run_id=old_run, source_date=source_date, page_size=1,
    )
    assert first["status"] == "failed"
    assert first["bronze_records"] == 1
    fail_second_page = False
    calls.clear()
    second = daily_runner.run_daily_resource(
        fs=fs, spec=spec, run_id=new_run, source_date=source_date, page_size=1,
    )
    assert calls == [0, 1]
    assert second["status"] == "success"
    assert second["bronze_records"] == 2
    assert read_day_manifest(fs, identity, old_run, source_date).status is DayStatus.FAILED
    assert read_day_manifest(fs, identity, new_run, source_date).status is DayStatus.SUCCESS
    for run, expected in ((old_run, 1), (new_run, 2)):
        files = fs.glob(
            f"{bucket}/bronze/muasamcong/detail/source_date={source_date}/run_id={run}/*.parquet"
        )
        count = 0
        for key in files:
            with fs.open(key, "rb") as file:
                count += pq.ParquetFile(file).metadata.num_rows
        assert count == expected
