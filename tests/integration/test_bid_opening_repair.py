"""Audit and selectively repair a composite record through actual Parquet/commits."""
import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.engine.daily_runner import _create_pipeline
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.sources.muasamcong.bid_opening.resource import ENDPOINTS
from procurement.models.bronze import BronzeRecord
from procurement.models.control import DayManifest, DayStatus, RunManifest, RunStatus
from procurement.quality.audit import audit_day, frozen_selection
from procurement.quality.contracts import load_config
from procurement.quality.files import write_json
from procurement.quality.repair import apply_plan, create_plan
from procurement.storage.bronze import DltBronzeWriter
from procurement.storage.committed import iter_committed_records, select_committed_days
from procurement.storage.control import commit_day_manifest, write_run_manifest

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("dual", [False, True])
def test_composite_audit_repair_and_resume(store, tmp_path, monkeypatch, dual):
    fs, _ = store
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    definition = get_resource("bid_opening")
    identity = definition.identity
    config = load_config(resource="bid_opening")
    day = date(2025, 1, 2)
    observed = datetime(2025, 1, 3, tzinfo=UTC)
    filename = "bid_opening_dual.json" if dual else "bid_opening.json"
    payload = json.loads((Path(__file__).parents[1] / "ingestion/muasamcong/fixtures" / filename).read_text(encoding="utf-8"))
    lot_key = "lot_open_detail_financial" if dual else "lot_open_detail"
    contexts, valid = [], []
    for key in ("good", "bad"):
        current = copy.deepcopy(payload)
        ctx = {"id": key, "notifyNo": "IB-" + key, "notifyVersion": "01", "isInternet": 1,
               "bidMode": "1_HTHS" if dual else "1_MTHS"}
        current["notify"]["bidNoContractorResponse"]["bidNotification"].update(ctx)
        current["roundmng"]["bidoBidroundMngViewDTO"].update(ctx)
        for lot in current[lot_key]:
            lot["notifyId"] = key
        contexts.append(ctx)
        valid.append(current)
    bad = copy.deepcopy(valid[1])
    bad[lot_key] = None
    records = [BronzeRecord(source_id=ctx["notifyNo"], source_version="01", source_date=day,
        run_id="baseline", ingested_at=observed, payload=data, content_hash=calculate_content_hash(data))
        for ctx, data in zip(contexts, (valid[0], bad), strict=True)]
    spec = ResourceSpec(identity, "composite_quality", identity.source, lambda **_: {}, lambda **_: iter(()))
    DltBronzeWriter(lambda: _create_pipeline(spec, day, "baseline")).write_page({"bid_opening_detail": records})
    write_run_manifest(fs, identity, RunManifest(run_id="baseline", source=identity.source,
        resource=identity.resource, start_date=day, end_date=day, total_dates=1, success_dates=1,
        status=RunStatus.SUCCESS, started_at=observed))
    commit_day_manifest(fs, identity, DayManifest(run_id="baseline", source=identity.source,
        resource=identity.resource, source_date=day, status=DayStatus.SUCCESS, bronze_records=2,
        search_items=2, started_at=observed))
    selected = frozen_selection(fs, identity.resource, day, day)[0]
    audit = audit_day(fs, selected, config, contexts)
    assert [r["result"]["status"] for r in audit["rows"]] == ["pass", "fail"]
    directory = tmp_path / "audit"
    write_json(directory / "days" / f"{day}.json", audit)
    write_json(directory / "selection.json", {"status": "complete", "resource": identity.resource,
        "year": 2025, "config_hash": config.fingerprint, "selection": [selected],
        "completed": {str(day): calculate_content_hash(audit)},
        "storage_namespace": calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])})
    plan = create_plan(directory, config, tmp_path / "plan.json")
    assert plan["estimated_detail_requests"] == (6 if dual else 4)
    calls = []
    class Client:
        def post(self, path, body):
            assert body["notifyId"] == "bad"
            calls.append(path)
            part = next(k for k, v in ENDPOINTS.items() if v == path)
            if dual and part in {"bid_open", "lot_open_detail"}:
                part += "_technical" if body["packType"] == 1 else "_financial"
            return valid[1][part]
        post_array = post
    work = tmp_path / "repair"
    first = apply_plan(fs, Client(), plan, config, work)
    assert first[0]["counts"] == {"copied": 1, "refetched": 1, "moved": 0}
    selection = select_committed_days(fs, definition, day, day)
    repaired = list(iter_committed_records(fs, selection, verify_hash=True))
    assert len(selection[0].files) == 1
    assert len(repaired) == 2
    good = next(r for _, r in repaired if r["source_id"] == "IB-good")
    assert good["ingested_at"] == observed and good["content_hash"] == records[0].content_hash
    assert apply_plan(fs, Client(), plan, config, work)[0]["status"] == "success"
    expected_calls = list(ENDPOINTS.values())
    if dual:
        expected_calls += [ENDPOINTS["bid_open"], ENDPOINTS["lot_open_detail"]]
    assert calls == expected_calls
