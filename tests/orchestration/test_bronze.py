from datetime import UTC, date, datetime
from unittest.mock import Mock
from uuid import uuid4

import fsspec
import pytest

pytest.importorskip("dagster")
from dagster import (
    DefaultScheduleStatus,
    Definitions,
    build_schedule_context,
    materialize,
)

from procurement import bootstrap
from procurement.common.catalog import RESOURCE_CATALOG
from procurement.common.settings import settings
from procurement.ingestion.contracts import MaterializeDayResult
from procurement.orchestration import bronze
from procurement.orchestration.definitions import bronze_daily_schedule, defs
from procurement.storage.control import DayCommitUncertainError

DAY = date(2025, 1, 1)


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", tmp_path.as_posix())
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    return fsspec.filesystem("file", auto_mkdir=True)


def execute(fs, resource="project", *, refresh=False, reconciled_run_ids=None):
    selected = next(
        a for a in bronze.bronze_assets if a.key.to_user_string() == f"bronze_{resource}"
    )
    return materialize(
        [selected],
        resources={"object_storage": fs},
        partition_key=str(DAY),
        run_config={
            "ops": {
                f"bronze_{resource}": {
                    "config": {
                        "refresh": refresh,
                        "reconciled_run_ids": reconciled_run_ids or [],
                    }
                }
            }
        },
        raise_on_error=False,
    )


def test_definitions_cover_catalog_and_safe_execution_policy():
    Definitions.validate_loadable(defs)
    assert {a.key.to_user_string() for a in bronze.bronze_assets} == {
        f"bronze_{d.identity.resource}" for d in RESOURCE_CATALOG
    }
    for asset in bronze.bronze_assets:
        assert asset.op.pool == bronze.INGESTION_POOL
        assert asset.op.retry_policy.max_retries == 0
        assert asset.backfill_policy.max_partitions_per_run == 1
    assert bronze_daily_schedule.default_status is DefaultScheduleStatus.STOPPED


def test_schedule_refreshes_three_closed_vietnam_dates_at_year_boundary():
    tick = datetime(2026, 1, 2, 1, tzinfo=UTC)
    requests = list(bronze_daily_schedule(build_schedule_context(scheduled_execution_time=tick)))
    assert [r.partition_key for r in requests] == ["2025-12-30", "2025-12-31", "2026-01-01"]
    assert len({r.run_key for r in requests}) == 3
    assert all(op["config"]["refresh"] for r in requests for op in r.run_config["ops"].values())


@pytest.mark.parametrize("resource", [d.identity.resource for d in RESOURCE_CATALOG])
def test_committed_empty_day_is_reused_without_new_writes(storage, monkeypatch, resource):
    cid = uuid4()
    aid = uuid4()
    mock_service = Mock()
    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource=resource,
        source_date=DAY,
        status="success",
        reused=True,
        commit_id=cid,
        attempt_id=aid,
        record_count=0,
        file_count=0,
    )
    monkeypatch.setattr(bootstrap, "bootstrap_services", lambda: (mock_service, Mock()))

    result = execute(storage, resource)
    assert result.success
    assert mock_service.materialize_day.call_count == 1
    call_req = mock_service.materialize_day.call_args[0][0]
    assert call_req.resource == resource
    assert call_req.source_date == DAY
    assert call_req.refresh is False

    assert all(check.passed for check in result.get_asset_check_evaluations())
    metadata = (
        result.get_asset_materialization_events()[0]
        .event_specific_data.materialization.metadata
    )
    assert metadata["reused"].value is True
    assert metadata["commit_id"].value == str(cid)


def test_new_attempt_is_verified(storage, monkeypatch):
    cid = uuid4()
    aid = uuid4()
    mock_service = Mock()
    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="project",
        source_date=DAY,
        status="success",
        reused=False,
        commit_id=cid,
        attempt_id=aid,
        record_count=10,
        file_count=2,
    )
    monkeypatch.setattr(bootstrap, "bootstrap_services", lambda: (mock_service, Mock()))

    result = execute(storage)
    assert result.success
    assert mock_service.materialize_day.call_count == 1
    assert all(check.passed for check in result.get_asset_check_evaluations())
    metadata = (
        result.get_asset_materialization_events()[0]
        .event_specific_data.materialization.metadata
    )
    assert metadata["reused"].value is False
    assert metadata["record_count"].value == 10
    assert metadata["file_count"].value == 2


@pytest.mark.parametrize("status", ["failed", "unresolved"])
def test_failed_refresh_cannot_hide_behind_previous_success(storage, monkeypatch, status):
    mock_service = Mock()
    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="project",
        source_date=DAY,
        status=status,
        reused=False,
    )
    monkeypatch.setattr(bootstrap, "bootstrap_services", lambda: (mock_service, Mock()))

    result = execute(storage, refresh=True)
    assert not result.success
    assert not result.get_asset_materialization_events()
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks["committed_manifest"] is False


def test_uncertain_commit_is_not_automatically_retried(storage, monkeypatch):
    mock_service = Mock()
    mock_service.materialize_day.side_effect = DayCommitUncertainError("unacknowledged")
    monkeypatch.setattr(bootstrap, "bootstrap_services", lambda: (mock_service, Mock()))

    result = execute(storage)
    assert not result.success
    assert mock_service.materialize_day.call_count == 1
    assert not result.get_asset_materialization_events()


def test_quality_unresolved_blocks_materialization_without_rewriting(storage, monkeypatch):
    mock_service = Mock()
    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="notify_contractor",
        source_date=DAY,
        status="failed",
        reused=False,
    )
    monkeypatch.setattr(bootstrap, "bootstrap_services", lambda: (mock_service, Mock()))

    result = execute(storage, "notify_contractor")
    assert not result.success
    assert not result.get_asset_materialization_events()
    assert not result.get_asset_check_evaluations()[0].passed
