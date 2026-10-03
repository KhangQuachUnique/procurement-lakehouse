"""Raw source JSON must survive DLT normalization and Parquet without mutation."""

import copy
import json
from datetime import UTC, date, datetime

import dlt
import pyarrow.parquet as pq
import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.daily_runner import _create_pipeline
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.models import ResourceSpec
from procurement.models.bronze import BronzeRecord
from procurement.storage.bronze import DltBronzeWriter

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("existing_json_schema", [False, True])
def test_payload_roundtrip_keeps_private_unicode_and_hash(store, existing_json_schema):
    fs, bucket = store
    day, run_id = date(2024, 8, 30), "unicode-roundtrip"
    spec = ResourceSpec(ResourceIdentity("muasamcong", "notify_contractor"), "roundtrip",
                        "muasamcong", lambda **_: {}, lambda **_: iter(()))
    pipeline = _create_pipeline(spec, day, run_id)
    table = "notify_contractor_vk_adb_detail"
    payload = {
        "bidpPlanDetail": {"generalTasks": "\uf02b\tLiên hệ và thuê địa điểm"},
        "nested": [{"value": "\uf02612.50"}, "\uf0272024-08-30T00:00:00", "\uf02bYWJj"],
        "\uf02bkey": "quotes: \" backslash: \\ newline: \n literal: \\uf02b",
        "empty": None, "flag": False, "amount": 0, "price": 1.25,
    }
    original = copy.deepcopy(payload)
    record = BronzeRecord(source_id="IB-roundtrip", source_version="00", source_date=day,
                          run_id=run_id, ingested_at=datetime(2024, 8, 31, tzinfo=UTC),
                          payload=payload, content_hash=calculate_content_hash(payload))
    if existing_json_schema:
        seed = record.model_copy(update={"source_id": "seed", "payload": {"plain": "text"},
                                         "content_hash": calculate_content_hash({"plain": "text"})})
        receipt = pipeline.run(dlt.resource(
            [seed.model_dump(mode="json")], name=table, table_name=table,
            write_disposition="append", file_format="parquet", max_table_nesting=0,
        ))
        receipt.raise_on_failed_jobs()
    assert DltBronzeWriter(lambda: pipeline).write_page({table: [record]}) == 1
    stored = []
    for key in fs.glob(f"{bucket}/bronze/muasamcong/{table}/source_date={day}/run_id={run_id}/*.parquet"):
        with fs.open(key, "rb") as stream:
            stored.extend(row for row in pq.ParquetFile(stream).read().to_pylist()
                          if row["source_id"] == record.source_id)
    assert len(stored) == 1
    restored = json.loads(stored[0]["payload"])
    assert restored == original
    assert calculate_content_hash(restored) == stored[0]["content_hash"] == record.content_hash
    assert record.payload == original
    assert not any(key.startswith("payload__") for key in stored[0])
