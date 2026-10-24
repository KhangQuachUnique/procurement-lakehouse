"""Unit tests for Dagster Silver orchestration assets, checks, jobs and schedules."""

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import fsspec
import pytest

pytest.importorskip("dagster")
from dagster import (
    DefaultScheduleStatus,
    Definitions,
    build_schedule_context,
    materialize,
)

from procurement.common.settings import settings
from procurement.orchestration import silver
from procurement.orchestration.definitions import defs, silver_daily_schedule

DAY = date(2025, 1, 1)


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", tmp_path.as_posix())
    return fsspec.filesystem("file", auto_mkdir=True)


def test_definitions_cover_silver_assets_and_policies():
    """Ensure defs is loadable and all silver table assets are properly wired."""
    Definitions.validate_loadable(defs)

    all_table_keys = {k.to_user_string() for a in silver.silver_assets for k in a.keys}
    expected_tables = {f"silver_{t.table}" for g in silver.SILVER_GROUPS for t in g.tables}
    assert all_table_keys == expected_tables

    for asset in silver.silver_assets:
        for k in asset.keys:
            assert asset.group_names_by_key[k] == "silver"
        assert asset.op.retry_policy.max_retries == 0
        assert asset.backfill_policy.max_partitions_per_run == 1
        group = next(g for g in silver.SILVER_GROUPS if f"silver_{g.primary_revision_table}" in {k.to_user_string() for k in asset.keys})
        # Check upstream bronze dependency
        for k in asset.keys:
            dep_keys = {d.to_user_string() for d in asset.asset_deps[k]}
            assert group.upstream_bronze_name in dep_keys

    assert silver_daily_schedule.default_status is DefaultScheduleStatus.STOPPED


def test_silver_schedule_three_closed_dates():
    """Verify schedule lookback produces the 3 preceding closed dates."""
    tick = datetime(2026, 1, 2, 2, tzinfo=UTC)
    requests = list(silver_daily_schedule(build_schedule_context(scheduled_execution_time=tick)))
    assert [r.partition_key for r in requests] == ["2025-12-30", "2025-12-31", "2026-01-01"]
    assert len({r.run_key for r in requests}) == 3


@pytest.mark.parametrize("group", silver.SILVER_GROUPS)
def test_silver_asset_materialize_success(storage, monkeypatch, group):
    """Verify successful execution produces correct metadata and passing checks for each table."""
    mock_run_resource = MagicMock()
    mock_run_resource.return_value = {
        "status": "completed",
        "run_id": "test-run-123",
        "days": [
            {
                "date": "2025-01-01",
                "status": "completed",
                "quality": {
                    "accepted": 100,
                    "quarantined": 2,
                    "issues": 1,
                    "blocking_issues": 0,
                },
                "files": {t.table: f"procurement-lakehouse/silver/{t.table}/source_date=2025-01-01/data.parquet" for t in group.tables},
                "iceberg": {t.table: {"snapshot_id": 12345, "row_count": 100} for t in group.tables},
            }
        ],
    }
    monkeypatch.setattr(silver, "run_resource", mock_run_resource)

    target_asset = next(a for a in silver.silver_assets if f"silver_{group.primary_revision_table}" in {k.to_user_string() for k in a.keys})

    result = materialize(
        [target_asset],
        resources={"object_storage": storage},
        partition_key=str(DAY),
        run_config={
            "ops": {
                f"silver_{group.resource_name}": {
                    "config": {
                        "publish_iceberg": True,
                        "skip_existing": False,
                    }
                }
            }
        },
        raise_on_error=True,
    )

    assert result.success
    mock_run_resource.assert_called_once_with(
        fs=storage,
        bucket=settings.OBJECT_STORAGE_BUCKET,
        resource=group.resource_name,
        start=DAY,
        end=DAY,
        family=None,
        skip_existing=False,
        iceberg=True,
    )

    # Check asset check evaluations (3 checks on primary table)
    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 3
    assert all(chk.passed for chk in check_evals)

    # Check that each table in the group was materialized
    materialized_keys = {e.asset_key.to_user_string() for e in result.all_events if e.is_step_materialization}
    expected_keys = {f"silver_{t.table}" for t in group.tables}
    assert materialized_keys == expected_keys


def test_silver_asset_materialize_high_quarantine_rate(storage, monkeypatch):
    """Verify quarantine_threshold check fails when quarantine rate > 20%."""
    mock_run_resource = MagicMock()
    group = next(g for g in silver.SILVER_GROUPS if g.resource_name == "notice")
    mock_run_resource.return_value = {
        "status": "needs_review",
        "run_id": "test-run-456",
        "days": [
            {
                "date": "2025-01-01",
                "status": "needs_review",
                "quality": {
                    "accepted": 50,
                    "quarantined": 50,  # 50% quarantine rate
                    "issues": 5,
                    "blocking_issues": 0,
                },
                "files": {t.table: f"path/{t.table}" for t in group.tables},
                "iceberg": {},
            }
        ],
    }
    monkeypatch.setattr(silver, "run_resource", mock_run_resource)

    target_asset = next(a for a in silver.silver_assets if "silver_notice_revision" in {k.to_user_string() for k in a.keys})

    result = materialize(
        [target_asset],
        resources={"object_storage": storage},
        partition_key=str(DAY),
        raise_on_error=True,
    )

    assert result.success
    check_evals = {chk.asset_check_key.name: chk for chk in result.get_asset_check_evaluations()}
    assert check_evals["execution_status"].passed is True
    # Quarantine check warning triggered (non-blocking)
    assert check_evals["quarantine_threshold"].passed is False


def test_silver_asset_materialize_failure(storage, monkeypatch):
    """Verify pipeline failure raises Dagster Failure."""
    mock_run_resource = MagicMock()
    mock_run_resource.return_value = {
        "status": "failed",
        "days": [
            {
                "date": "2025-01-01",
                "status": "failed",
                "error": "Corrupt parquet partition",
                "error_type": "ParquetFormatError",
            }
        ],
    }
    monkeypatch.setattr(silver, "run_resource", mock_run_resource)

    target_asset = next(a for a in silver.silver_assets if "silver_project_revision" in {k.to_user_string() for k in a.keys})

    result = materialize(
        [target_asset],
        resources={"object_storage": storage},
        partition_key=str(DAY),
        raise_on_error=False,
    )

    assert not result.success


def test_silver_sync_iceberg_job_execution(storage, monkeypatch):
    """Verify batch sync iceberg op calls silver.sync_iceberg correctly."""
    mock_sync = MagicMock()
    with patch("procurement.cli.silver.sync_iceberg", mock_sync):
        job_result = silver.silver_sync_iceberg_job.execute_in_process(
            resources={"object_storage": storage},
            run_config={
                "ops": {
                    "sync_silver_iceberg_op": {
                        "config": {"table": "project_revision", "year": 2024}
                    }
                }
            },
        )
        assert job_result.success
        mock_sync.assert_called_once_with(table="project_revision", resource=None, year=2024)
