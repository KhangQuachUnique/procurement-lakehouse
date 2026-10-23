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
    """Ensure defs is loadable and all silver assets are properly wired."""
    Definitions.validate_loadable(defs)
    silver_asset_names = {a.key.to_user_string() for a in silver.silver_assets}
    expected_names = {s.name for s in silver.SILVER_SPECS}
    assert silver_asset_names == expected_names

    for asset in silver.silver_assets:
        assert asset.group_names_by_key[asset.key] == "silver"
        assert asset.op.retry_policy.max_retries == 0
        assert asset.backfill_policy.max_partitions_per_run == 1
        spec = next(s for s in silver.SILVER_SPECS if s.name == asset.key.to_user_string())
        # Check upstream bronze dependency
        dep_keys = {d.to_user_string() for d in asset.asset_deps[asset.key]}
        assert spec.upstream_bronze_name in dep_keys

    assert silver_daily_schedule.default_status is DefaultScheduleStatus.STOPPED


def test_silver_schedule_three_closed_dates():
    """Verify schedule lookback produces the 3 preceding closed dates."""
    tick = datetime(2026, 1, 2, 2, tzinfo=UTC)
    requests = list(silver_daily_schedule(build_schedule_context(scheduled_execution_time=tick)))
    assert [r.partition_key for r in requests] == ["2025-12-30", "2025-12-31", "2026-01-01"]
    assert len({r.run_key for r in requests}) == 3


@pytest.mark.parametrize("spec", silver.SILVER_SPECS)
def test_silver_asset_materialize_success(storage, monkeypatch, spec):
    """Verify successful execution produces correct metadata and passing checks."""
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
                "files": ["s3://procurement-lakehouse/silver/tbl/data.parquet"],
                "iceberg": [{"table": "tbl", "rows": 100, "snapshot_id": 12345}],
            }
        ],
    }
    monkeypatch.setattr(silver, "run_resource", mock_run_resource)

    target_asset = next(a for a in silver.silver_assets if a.key.to_user_string() == spec.name)

    result = materialize(
        [target_asset],
        resources={"object_storage": storage},
        partition_key=str(DAY),
        run_config={
            "ops": {
                spec.name: {
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
        bucket=str(DAY).join("") if False else storage.fs.root_marker if False else settings.OBJECT_STORAGE_BUCKET,
        resource=spec.resource_name,
        start=DAY,
        end=DAY,
        family=None,
        skip_existing=False,
        iceberg=True,
    )

    # Check asset check evaluations
    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 3
    assert all(chk.passed for chk in check_evals)


def test_silver_asset_materialize_high_quarantine_rate(storage, monkeypatch):
    """Verify quarantine_threshold check fails when quarantine rate > 20%."""
    mock_run_resource = MagicMock()
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
                "files": ["s3://procurement-lakehouse/silver/tbl/data.parquet"],
                "iceberg": [],
            }
        ],
    }
    monkeypatch.setattr(silver, "run_resource", mock_run_resource)

    target_asset = next(a for a in silver.silver_assets if a.key.to_user_string() == "silver_notice")

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

    target_asset = next(a for a in silver.silver_assets if a.key.to_user_string() == "silver_project")

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
