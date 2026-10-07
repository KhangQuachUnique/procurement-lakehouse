from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

pytest.importorskip('dagster')
from dagster import (
    DagsterInstance,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    build_sensor_context,
)

from procurement.common.settings import settings
from procurement.orchestration import watcher
from procurement.storage.control import DayCommitUncertainError
from procurement.watcher import WatchStore


@pytest.fixture
def state(tmp_path, monkeypatch):
    path = tmp_path / 'watch.db'
    monkeypatch.setattr(settings, 'BID_OPENING_WATCH_PATH', str(path))
    monkeypatch.setattr(settings, 'INGESTION_LOCK_DIR', str(tmp_path / 'locks'))
    return path


def add_due(path, *, status='pending', due=None, day='2025-01-01'):
    store = WatchStore(path)
    with store.db:
        store.db.execute('INSERT OR REPLACE INTO notices VALUES (?,?,?,?,?,?,?)',
                         (day, day, '{}', status, due or datetime.now(UTC).isoformat(), None, None))
    store.close()


def context(active=False):
    instance = Mock(spec=DagsterInstance)
    instance.get_runs.return_value = [object()] if active else []
    return build_sensor_context(instance=instance)


def test_empty_sensor_never_creates_database(state):
    assert isinstance(watcher.bid_opening_due_sensor(context()), SkipReason)
    assert not state.exists()
    assert watcher.bid_opening_due_sensor.default_status is DefaultSensorStatus.STOPPED


def test_due_sensor_deduplicates_and_does_not_write(state):
    add_due(state)
    before = state.read_bytes()
    first = watcher.bid_opening_due_sensor(context())
    assert isinstance(first, RunRequest)
    assert watcher.bid_opening_due_sensor(context()).run_key == first.run_key
    assert state.read_bytes() == before
    add_due(state, due=(datetime.now(UTC) - timedelta(hours=1)).isoformat())
    assert watcher.bid_opening_due_sensor(context()).run_key != first.run_key


def test_queued_or_running_job_prevents_additional_request(state):
    add_due(state)
    assert isinstance(watcher.bid_opening_due_sensor(context(active=True)), SkipReason)


@pytest.mark.parametrize('status,offset', [('captured', -1), ('unresolved', -1), ('pending', 1)])
def test_sensor_ignores_terminal_and_future_records(state, status, offset):
    add_due(state, status=status, due=(datetime.now(UTC) + timedelta(days=offset)).isoformat())
    assert isinstance(watcher.bid_opening_due_sensor(context()), SkipReason)


def test_due_reader_excludes_today_even_when_explicit_end_includes_it(state):
    from procurement.common.dates import today_vn
    add_due(state, day=str(today_vn()))
    store = WatchStore(state, read_only=True)
    try:
        assert store.due_days(end=today_vn()) == []
    finally:
        store.close()


def test_job_rechecks_due_state_after_queueing(state, monkeypatch):
    run = Mock()
    monkeypatch.setattr(watcher, 'run_watch', run)
    result = watcher.bid_opening_watch_job.execute_in_process(resources={'object_storage': object()})
    assert result.success
    run.assert_not_called()


def test_check_reuses_domain_runner_without_full_seed(state, monkeypatch):
    add_due(state)
    run = Mock(return_value={'check': {'days': []}})
    monkeypatch.setattr(watcher, 'run_watch', run)
    fs = object()
    result = watcher.bid_opening_watch_job.execute_in_process(resources={'object_storage': fs})
    assert result.success
    run.assert_called_once_with(fs, mode='check', seed_before_check=False)
    assert watcher.check_bid_opening.pool == 'muasamcong_ingestion'


def test_uncertain_commit_fails_without_retry(state, monkeypatch):
    add_due(state)
    run = Mock(side_effect=DayCommitUncertainError('unknown'))
    monkeypatch.setattr(watcher, 'run_watch', run)
    result = watcher.bid_opening_watch_job.execute_in_process(
        resources={'object_storage': object()}, raise_on_error=False)
    assert not result.success
    assert run.call_count == 1


def test_seed_runs_domain_reconciliation(state, monkeypatch):
    run = Mock(return_value={'seed': {'seeded_days': 2}})
    monkeypatch.setattr(watcher, 'run_watch', run)
    fs = object()
    assert watcher.bid_opening_seed_job.execute_in_process(resources={'object_storage': fs}).success
    run.assert_called_once_with(fs, mode='seed')
