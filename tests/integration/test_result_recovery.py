import json
from datetime import date

import httpx
import pytest

from procurement.common.catalog import get_resource
from procurement.ingestion.batch_runner import run_batch_range
from procurement.ingestion.engine.daily_runner import run_daily_resource
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.storage.committed import select_committed_days, verify_committed
from procurement.storage.control import list_page_manifests, read_day_manifest, read_run_manifest
from procurement.tools.recover_result_2024 import recovery_spec
from procurement.transfer.archive import BundleDay, validate_day_metadata

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("approved", [True, False])
def test_recovery_real_parquet_and_manifests(store, tmp_path, approved):
    fs, bucket = store
    day = date(2024, 9, 12)
    number = "IB2400340694" if approved else "IB2400000009"
    contexts = [{"id": "bad", "notifyNo": number, "inputResultId": "bad"},
                {"id": "good", "notifyNo": "IB2400000001", "inputResultId": "good"}]
    def handle(request):
        if request.url.path.endswith("smart/search"):
            return httpx.Response(200, json={"page": {"content": contexts, "totalElements": 2}})
        if json.loads(request.content)["id"] == "bad":
            return httpx.Response(500)
        return httpx.Response(200, json={"bideContractorInputResultDTO": {
            "id": "good", "notifyNo": "IB2400000001", "resultVersion": "01"}})
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handle), max_attempts=1) as client:
        spec = recovery_spec(client, fs, tmp_path / "job", "test")
        run_id = run_batch_range(day, day, fs=fs, identity=spec.identity, run_day=lambda run, dt:
            run_daily_resource(fs=fs, spec=spec, run_id=run, source_date=dt, page_size=50))
    marker = read_day_manifest(fs, spec.identity, run_id, day)
    assert marker.status == ("success" if approved else "failed")
    assert fs.glob(f"{bucket}/_quality/**/recovery-*.json")
    if approved:
        assert marker.search_items == 2 and marker.bronze_records == 1 and marker.error_count == 0
        pages = list_page_manifests(fs, spec.identity, run_id=run_id, source_date=day)
        validate_day_metadata(BundleDay(resource="contractor_result", source_date=day, run_id=run_id, objects=[]),
                              read_run_manifest(fs, spec.identity, run_id), marker, pages)
        assert verify_committed(fs, select_committed_days(fs, get_resource("contractor_result"), day, day))["records"] == 1
