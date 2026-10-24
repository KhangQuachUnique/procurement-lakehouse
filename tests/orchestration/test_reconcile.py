"""Unit tests for Lakehouse to Dagster state reconciliation."""

from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetKey, DagsterInstance

from procurement.orchestration.reconcile import (
    discover_s3_partitions,
    reconcile_lakehouse_job,
    reconcile_lakehouse_to_dagster,
)


def test_discover_s3_partitions():
    """Verify S3 paths parsing into YYYY-MM-DD partition keys."""
    mock_fs = MagicMock()
    mock_fs.glob.return_value = [
        "bucket/silver/plan_revision/source_date=2024-01-01",
        "bucket/silver/plan_revision/source_date=2024-01-02",
        "bucket/silver/plan_revision/source_date=invalid-date",
    ]
    res = discover_s3_partitions(mock_fs, "bucket", "silver/plan_revision")
    assert res == ["2024-01-01", "2024-01-02"]


def test_reconcile_lakehouse_to_dagster():
    """Verify missing partitions are reported while existing ones are skipped."""
    instance = DagsterInstance.ephemeral()
    mock_fs = MagicMock()

    # Return 2 partitions for any folder
    mock_fs.glob.return_value = [
        "bucket/prefix/source_date=2024-01-01",
        "bucket/prefix/source_date=2024-01-02",
    ]

    # Reconcile once
    res1 = reconcile_lakehouse_to_dagster(instance=instance, fs=mock_fs, bucket="bucket")
    assert res1["bronze_project"] == 2
    assert res1["silver_project_revision"] == 2

    # Verify partitions are now marked as materialized
    parts = instance.get_materialized_partitions(AssetKey("silver_project_revision"))
    assert set(parts) == {"2024-01-01", "2024-01-02"}

    # Reconcile second time -> should report 0 missing partitions (idempotent)
    res2 = reconcile_lakehouse_to_dagster(instance=instance, fs=mock_fs, bucket="bucket")
    assert res2["bronze_project"] == 0
    assert res2["silver_project_revision"] == 0


def test_reconcile_lakehouse_job_in_process():
    """Verify reconcile_lakehouse_job executes in-process."""
    mock_fs = MagicMock()
    mock_fs.glob.return_value = ["bucket/prefix/source_date=2024-01-01"]

    res = reconcile_lakehouse_job.execute_in_process(
        resources={"object_storage": mock_fs},
        run_config={"ops": {"reconcile_lakehouse_op": {"config": {"dry_run": True}}}},
    )
    assert res.success
