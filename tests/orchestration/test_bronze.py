from datetime import UTC, date, datetime
from unittest.mock import Mock

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip('dagster')
from dagster import (
    DefaultScheduleStatus,
    Definitions,
    build_schedule_context,
    materialize,
)

from procurement.common.catalog import RESOURCE_CATALOG, get_resource
from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.models.control import DayManifest, DayStatus, RunManifest, RunStatus
from procurement.orchestration import bronze
from procurement.orchestration.definitions import bronze_daily_schedule, defs
from procurement.storage.control import (
    DayCommitUncertainError,
    write_day_manifest,
    write_run_manifest,
)

DAY = date(2025, 1, 1)


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, 'OBJECT_STORAGE_BUCKET', tmp_path.as_posix())
    monkeypatch.setattr(settings, 'INGESTION_LOCK_DIR', str(tmp_path / 'locks'))
    return fsspec.filesystem('file', auto_mkdir=True)


def attempt(fs, *, run_id='attempt', status=DayStatus.SUCCESS, resource='project', records=0):
    identity = get_resource(resource).identity
    started = datetime(2025, 1, 2, tzinfo=UTC)
    write_run_manifest(fs, identity, RunManifest(
        run_id=run_id, source=identity.source, resource=resource, start_date=DAY, end_date=DAY,
        status=RunStatus.RUNNING if status is DayStatus.RUNNING else RunStatus.SUCCESS,
        total_dates=1, started_at=started,
    ))
    manifest = DayManifest(
        run_id=run_id, source=identity.source, resource=resource, source_date=DAY, status=status,
        started_at=started, completed_at=started, bronze_records=records,
    )
    write_day_manifest(fs, identity, manifest)
    return run_id


def execute(fs, resource='project', *, refresh=False, reconciled_run_ids=None):
    selected = next(a for a in bronze.bronze_assets if a.key.to_user_string() == f'bronze_{resource}')
    return materialize(
        [selected], resources={'object_storage': fs}, partition_key=str(DAY),
        run_config={'ops': {f'bronze_{resource}': {'config': {
            'refresh': refresh, 'reconciled_run_ids': reconciled_run_ids or [],
        }}}},
        raise_on_error=False,
    )


def test_definitions_cover_catalog_and_safe_execution_policy():
    Definitions.validate_loadable(defs)
    assert {a.key.to_user_string() for a in bronze.bronze_assets} == {
        f'bronze_{d.identity.resource}' for d in RESOURCE_CATALOG
    }
    for asset in bronze.bronze_assets:
        assert asset.op.pool == bronze.INGESTION_POOL
        assert asset.op.retry_policy.max_retries == 0
        assert asset.backfill_policy.max_partitions_per_run == 1
    assert bronze_daily_schedule.default_status is DefaultScheduleStatus.STOPPED


def test_schedule_refreshes_three_closed_vietnam_dates_at_year_boundary():
    tick = datetime(2026, 1, 2, 1, tzinfo=UTC)
    requests = list(bronze_daily_schedule(build_schedule_context(scheduled_execution_time=tick)))
    assert [r.partition_key for r in requests] == ['2025-12-30', '2025-12-31', '2026-01-01']
    assert len({r.run_key for r in requests}) == 3
    assert all(op['config']['refresh'] for r in requests for op in r.run_config['ops'].values())


@pytest.mark.parametrize('resource', [d.identity.resource for d in RESOURCE_CATALOG])
def test_committed_empty_day_is_reused_without_new_writes(storage, monkeypatch, resource):
    attempt(storage, resource=resource)
    run = Mock(side_effect=AssertionError('must not write'))
    monkeypatch.setattr(bronze, 'run_resource_day', run)
    result = execute(storage, resource)
    assert result.success
    run.assert_not_called()
    assert all(check.passed for check in result.get_asset_check_evaluations())
    metadata = result.get_asset_materialization_events()[0].event_specific_data.materialization.metadata
    assert metadata['run_id'].value == 'attempt'
    assert metadata['reused_attempt'].value is True


def test_new_attempt_is_verified(storage, monkeypatch):
    run = Mock(side_effect=lambda *a, **kw: attempt(storage))
    monkeypatch.setattr(bronze, 'run_resource_day', run)
    assert execute(storage).success
    assert run.call_count == 1


@pytest.mark.parametrize('status', [None, DayStatus.FAILED, DayStatus.RUNNING])
def test_failed_refresh_cannot_hide_behind_previous_success(storage, monkeypatch, status):
    attempt(storage, run_id='old-success')

    def run(*args, **kwargs):
        if status:
            attempt(storage, run_id='new-attempt', status=status)
        return 'new-attempt'

    monkeypatch.setattr(bronze, 'run_resource_day', run)
    result = execute(storage, refresh=True)
    assert not result.success
    assert not result.get_asset_materialization_events()


def test_unresolved_attempt_blocks_new_writes(storage, monkeypatch):
    attempt(storage, status=DayStatus.RUNNING)
    run = Mock()
    monkeypatch.setattr(bronze, 'run_resource_day', run)
    assert not execute(storage, refresh=True).success
    run.assert_not_called()


def test_uncertain_commit_is_not_automatically_retried(storage, monkeypatch):
    run = Mock(side_effect=DayCommitUncertainError('unacknowledged'))
    monkeypatch.setattr(bronze, 'run_resource_day', run)
    result = execute(storage)
    assert not result.success
    assert run.call_count == 1
    assert not result.get_asset_materialization_events()


@pytest.mark.parametrize('corruption', ['missing_file', 'count', 'hash', 'lineage'])
def test_corrupt_committed_data_fails_checks(storage, corruption):
    attempt(storage, records=2 if corruption == 'count' else 1)
    if corruption != 'missing_file':
        key = (f'{settings.OBJECT_STORAGE_BUCKET}/bronze/muasamcong/project_detail/'
               f'source_date={DAY}/run_id=attempt/part.parquet')
        payload = {'id': 'record'}
        record = {'payload': payload, 'content_hash': calculate_content_hash(payload),
                  'run_id': 'attempt', 'source_date': str(DAY)}
        if corruption == 'hash':
            record['content_hash'] = 'invalid'
        if corruption == 'lineage':
            record['run_id'] = 'other'
        with storage.open(key, 'wb') as target:
            pq.write_table(pa.Table.from_pylist([record]), target)
    result = execute(storage)
    assert not result.success
    assert not result.get_asset_materialization_events()
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks == {'committed_manifest': True, 'committed_integrity': False}


def test_quality_unresolved_blocks_materialization_without_rewriting(storage, monkeypatch):
    attempt(storage, resource='notify_contractor')
    monkeypatch.setattr(bronze, 'audit_day', lambda *a: {
        'status': 'complete', 'rows': [{'result': {'status': 'unresolved'}}],
    })
    result = execute(storage, 'notify_contractor')
    assert not result.success
    assert not result.get_asset_materialization_events()
    assert not result.get_asset_check_evaluations()[-1].passed


def test_explicit_reconciliation_preserves_uncertain_history(storage, monkeypatch):
    from procurement.storage.control import read_day_manifest

    attempt(storage, run_id='uncertain', status=DayStatus.RUNNING)
    run = Mock(side_effect=lambda *a, **kw: attempt(storage, run_id='recovery'))
    monkeypatch.setattr(bronze, 'run_resource_day', run)
    assert not execute(storage, reconciled_run_ids=['different-run']).success
    run.assert_not_called()
    assert execute(storage, reconciled_run_ids=['uncertain']).success
    old = read_day_manifest(storage, get_resource('project').identity, 'uncertain', DAY)
    assert old.status is DayStatus.RUNNING
    assert run.call_count == 1
