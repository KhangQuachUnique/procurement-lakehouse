"""Real runner, Parquet and manifests through Dagster; HTTP source remains a fixture."""
import json
from datetime import date
from pathlib import Path

import httpx
import pytest

pytest.importorskip('dagster')
from dagster import materialize

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.jobs import runner
from procurement.models.control import DayStatus
from procurement.orchestration.bronze import bronze_assets
from procurement.storage.committed import iter_committed_records, select_committed_days
from procurement.storage.control import list_day_manifests, list_run_manifests

pytestmark = pytest.mark.integration


def test_dagster_real_commit_retry_and_reuse(store, monkeypatch, tmp_path):
    fs, _ = store
    fail_detail = False
    calls = []

    def source(request):
        calls.append(request.url.path)
        if isinstance(json.loads(request.content), list):
            return httpx.Response(200, json={'page': {
                'content': [{'id': 'p'}], 'totalElements': 1, 'totalPages': 1,
                'number': 0, 'size': 50,
            }})
        if fail_detail:
            return httpx.Response(404)
        return httpx.Response(200, json={'id': 'p', 'projectDTO': {'version': '01'}})

    monkeypatch.setattr(settings, 'MUASAMCONG_TOKEN', 'fixture-token')
    monkeypatch.setattr(settings, 'INGESTION_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setattr(runner, 'create_s3_filesystem', lambda: fs)
    monkeypatch.setattr(runner, 'MuasamcongClient', lambda **kw: MuasamcongClient(
        **kw, transport=httpx.MockTransport(source), sleep=lambda _: None,
    ))
    project = next(a for a in bronze_assets if a.key.to_user_string() == 'bronze_project')

    def execute(refresh=False):
        return materialize(
            [project], partition_key='2025-01-01', resources={'object_storage': fs},
            run_config={'ops': {'bronze_project': {'config': {'refresh': refresh}}}},
            raise_on_error=False,
        )

    day = date(2025, 1, 1)
    definition = get_resource('project')
    assert execute().success
    initial = select_committed_days(fs, definition, day, day)
    original_records = list(iter_committed_records(fs, initial, verify_hash=True))
    assert len(original_records) == 1
    original_calls = len(calls)
    assert execute().success
    assert len(calls) == original_calls

    fail_detail = True
    assert not execute(refresh=True).success
    assert select_committed_days(fs, definition, day, day) == initial
    fail_detail = False
    assert execute(refresh=True).success
    final = select_committed_days(fs, definition, day, day)
    assert final[0].run_id != initial[0].run_id
    assert list(iter_committed_records(fs, initial, verify_hash=True)) == original_records
    manifests = [manifest for run in list_run_manifests(fs, definition.identity)
                 for manifest in list_day_manifests(fs, definition.identity, run_id=run.run_id)]
    assert sorted(m.status.value for m in manifests) == ['failed', 'success', 'success']
    assert all(m.status is not DayStatus.RUNNING for m in manifests)
    # Auditable evidence for the retention policy: local attempt state exists after commit.
    assert list(Path(settings.DLT_PIPELINES_DIR).iterdir())
