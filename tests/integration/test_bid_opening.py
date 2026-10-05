import copy
import json
from datetime import date
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from procurement.common.catalog import get_resource
from procurement.ingestion.engine import daily_runner
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.engine.records import build_bronze_item
from procurement.ingestion.sources.muasamcong.bid_opening.resource import (
    ENDPOINTS,
    create_bid_opening_spec,
)
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.storage.control import read_day_manifest

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("batch_records,expected_files", [(5000, 1), (2, 3)])
def test_many_pages_batch_without_losing_records(store, monkeypatch, batch_records, expected_files):
    fs, bucket = store
    monkeypatch.setattr(daily_runner.settings, "BRONZE_BATCH_RECORDS", batch_records)
    day = date(2025, 8, 3)
    identity = get_resource("bid_opening").identity
    def fetch(*, page_number, **_):
        return {"page": {"content": [{"id": str(page_number)}], "totalElements": 5}}
    def records(*, search_items, source_date, run_id, **_):
        yield build_bronze_item(table="bid_opening_detail", source_id=search_items[0]["id"],
            source_version="00", payload={"nested": [{"unicode": "\uf02b thông báo"}]},
            source_date=source_date, run_id=run_id)
    spec = ResourceSpec(identity, "batch_test", "muasamcong", fetch, records)
    result = daily_runner.run_daily_resource(fs=fs, spec=spec, run_id="batch", source_date=day, page_size=1)
    files = fs.glob(f"{bucket}/bronze/muasamcong/bid_opening_detail/source_date={day}/run_id=batch/*.parquet")
    assert result["status"] == "success" and result["pages"] == 5
    assert len(files) == expected_files
    rows = []
    for key in files:
        with fs.open(key, "rb") as stream:
            rows.extend(pq.ParquetFile(stream).read().to_pylist())
    assert {r["source_id"] for r in rows} == {str(i) for i in range(5)}
    assert len(rows) == 5
    assert all(json.loads(r["payload"])["nested"][0]["unicode"] == "\uf02b thông báo" for r in rows)


@pytest.mark.parametrize("kind", ["single", "dual", "technical_only"])
def test_opening_record_commits_and_roundtrips(store, kind):
    fs, bucket = store
    filename = "bid_opening.json" if kind == "single" else "bid_opening_dual.json"
    fixture = Path(__file__).parents[1] / "ingestion/muasamcong/fixtures" / filename
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    if kind == "technical_only":
        payload["roundmng"]["bidoBidroundMngViewDTO"]["successBidOpenDateTc"] = None
        del payload["bid_open_financial"], payload["lot_open_detail_financial"]
    root = payload["notify"]["bidNoContractorResponse"]["bidNotification"]
    context = {k: root[k] for k in ("id", "notifyNo", "notifyVersion", "isInternet")}
    def handle(request):
        if request.url.path.endswith("smart/search"):
            return httpx.Response(200, json={"page": {"content": [context], "totalElements": 1}})
        part = next(k for k, v in ENDPOINTS.items() if request.url.path == v)
        if kind != "single" and part in {"bid_open", "lot_open_detail"}:
            part += "_technical" if json.loads(request.content)["packType"] == 1 else "_financial"
        return httpx.Response(200, json=payload[part])
    day = date(2026, 9, 27)
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle)) as client:
        spec = create_bid_opening_spec(client)
        result = daily_runner.run_daily_resource(fs=fs, spec=spec, run_id="four", source_date=day, page_size=50)
    assert result["status"] == "success" and result["bronze_records"] == 1
    assert read_day_manifest(fs, spec.identity, "four", day).completed_pages == 1
    files = fs.glob(f"{bucket}/bronze/muasamcong/bid_opening_detail/source_date={day}/run_id=four/*.parquet")
    assert len(files) == 1
    with fs.open(files[0], "rb") as stream:
        rows = pq.ParquetFile(stream).read().to_pylist()
    assert json.loads(rows[0]["payload"]) == payload


def test_parallel_openings_share_one_parquet_and_keep_identity(store):
    fs, bucket = store
    fixture = Path(__file__).parents[1] / "ingestion/muasamcong/fixtures/bid_opening.json"
    original = json.loads(fixture.read_text(encoding="utf-8"))
    contexts = [{"id": str(i), "notifyNo": f"IB{i}", "notifyVersion": "01", "isInternet": 1} for i in range(7)]
    def handle(request):
        if request.url.path.endswith("smart/search"):
            return httpx.Response(200, json={"page": {"content": contexts, "totalElements": 7}})
        body = json.loads(request.content)
        ctx = contexts[int(body["notifyId"])]
        payload = copy.deepcopy(original)
        payload["notify"]["bidNoContractorResponse"]["bidNotification"].update(ctx)
        payload["roundmng"]["bidoBidroundMngViewDTO"].update(ctx)
        for lot in payload["lot_open_detail"]:
            lot["notifyId"] = ctx["id"]
        part = next(k for k, v in ENDPOINTS.items() if request.url.path == v)
        return httpx.Response(200, json=payload[part])
    day = date(2025, 1, 1)
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle)) as client:
        result = daily_runner.run_daily_resource(fs=fs,
            spec=create_bid_opening_spec(client, detail_workers=3), run_id="parallel", source_date=day, page_size=50)
    assert result["status"] == "success" and result["bronze_records"] == 7
    files = fs.glob(f"{bucket}/bronze/muasamcong/bid_opening_detail/source_date={day}/run_id=parallel/*.parquet")
    assert len(files) == 1
    with fs.open(files[0], "rb") as stream:
        rows = pq.ParquetFile(stream).read().to_pylist()
    assert len(rows) == 7 and {r["source_id"] for r in rows} == {c["notifyNo"] for c in contexts}
    for row in rows:
        payload = json.loads(row["payload"])
        assert row["content_hash"] == calculate_content_hash(payload)
        assert row["source_id"] == payload["notify"]["bidNoContractorResponse"]["bidNotification"]["notifyNo"]
