from datetime import UTC, date, datetime
from unittest.mock import Mock

import fsspec
import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.models.control import DayManifest, RunManifest
from procurement.storage.control import list_run_manifests, write_day_manifest, write_run_manifest


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, 'OBJECT_STORAGE_BUCKET', tmp_path.as_posix())
    return fsspec.filesystem('file', auto_mkdir=True, skip_instance_cache=True)


@pytest.mark.parametrize('day_status,blocked', [(None, True), ('running', True),
                                             ('success', False), ('failed', False)])
def test_single_day_range_run_only_reads_requested_day(store, monkeypatch, day_status, blocked):
    identity = get_resource('project').identity
    day = date(2022, 7, 1)
    started = datetime(2026, 1, 1, tzinfo=UTC)
    write_run_manifest(store, identity, RunManifest(
        run_id='range', source=identity.source, resource=identity.resource,
        start_date=date(2022, 1, 1), end_date=date(2022, 12, 31), total_dates=365,
        status='running', started_at=started,
    ))
    for source_date, status in [(date(2022, 1, 1), 'success'), (day, day_status)]:
        if status:
            write_day_manifest(store, identity, DayManifest(
                run_id='range', source=identity.source, resource=identity.resource,
                source_date=source_date, status=status, started_at=started,
            ))
    monkeypatch.setattr(store, 'glob', Mock(side_effect=AssertionError('No recursive listing')))
    original_open = store.open

    def open_requested_only(path, *args, **kwargs):
        assert 'source_date=2022-01-01' not in str(path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(store, 'open', open_requested_only)
    coverage = read_coverage(store, identity, day, day, workers=8)[0]
    assert coverage.active_run_ids == (('range',) if blocked else ())
    assert (coverage.effective is not None) == (day_status == 'success')


def test_missing_resource_is_empty_but_storage_errors_propagate(store, monkeypatch):
    identity = get_resource('project').identity
    assert list_run_manifests(store, identity, workers=8) == []
    monkeypatch.setattr(store, 'ls', Mock(side_effect=PermissionError('denied')))
    with pytest.raises(PermissionError):
        list_run_manifests(store, identity, workers=8)
