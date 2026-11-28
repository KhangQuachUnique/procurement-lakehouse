"""Unit tests for Gold Spark incremental runner (start-snapshot-id + MERGE INTO + fallback)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from procurement.gold.cli import main as gold_spark_cli_main
from procurement.gold.runner import run_spark_gold_incremental
from procurement.spark.config import SparkSilverSettings
from procurement.spark.iceberg_writer import merge_into_table


def _test_cfg() -> SparkSilverSettings:
    return SparkSilverSettings(
        app_database_url="postgresql://test:test@localhost/test",
        iceberg_namespace="silver_spark",
    )


def test_merge_into_table_sql() -> None:
    fake_spark = MagicMock()
    fake_df = MagicMock()
    merge_into_table(
        fake_spark,
        full_name="lake.gold_spark_core.fact_procurement_demand",
        source_df=fake_df,
        merge_keys=("package_key",),
    )
    fake_df.createOrReplaceTempView.assert_called_once()
    sql = fake_spark.sql.call_args[0][0]
    assert "MERGE INTO lake.gold_spark_core.fact_procurement_demand AS t" in sql
    assert "ON t.package_key <=> s.package_key" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_incremental_bootstraps_via_rebuild_when_no_watermark() -> None:
    fake_engine = MagicMock()
    fake_store = MagicMock()
    fake_store.get_latest_published_watermarks.return_value = None

    with (
        patch("procurement.gold.runner._create_pg_engine", return_value=fake_engine),
        patch("procurement.gold.runner.GoldWatermarkStore", return_value=fake_store),
        patch(
            "procurement.gold.runner.run_spark_gold_rebuild",
            return_value={"status": "PUBLISHED", "mode": "rebuild"},
        ) as mock_rebuild,
    ):
        res = run_spark_gold_incremental(settings=_test_cfg())

    assert res["status"] == "PUBLISHED"
    mock_rebuild.assert_called_once()


def test_incremental_returns_noop_when_snapshots_unchanged() -> None:
    snaps = {"package_revision": 1001, "notice_revision": 2002}
    fake_engine = MagicMock()
    fake_store = MagicMock()
    fake_store.get_latest_published_watermarks.return_value = {"silver_snapshots": snaps}
    fake_spark = MagicMock()

    with (
        patch("procurement.gold.runner._create_pg_engine", return_value=fake_engine),
        patch("procurement.gold.runner.GoldWatermarkStore", return_value=fake_store),
        patch("procurement.gold.runner.read_silver_snapshot_ids", return_value=dict(snaps)),
        patch("procurement.gold.runner.load_silver_incremental_deltas") as mock_deltas,
    ):
        res = run_spark_gold_incremental(settings=_test_cfg(), spark=fake_spark)

    assert res["status"] == "NO_OP"
    mock_deltas.assert_not_called()


def test_incremental_falls_back_to_rebuild_on_expired_snapshot() -> None:
    prev_snaps = {"package_revision": 1001}
    curr_snaps = {"package_revision": 1005}
    fake_engine = MagicMock()
    fake_store = MagicMock()
    fake_store.get_latest_published_watermarks.return_value = {"silver_snapshots": prev_snaps}
    fake_spark = MagicMock()

    with (
        patch("procurement.gold.runner._create_pg_engine", return_value=fake_engine),
        patch("procurement.gold.runner.GoldWatermarkStore", return_value=fake_store),
        patch("procurement.gold.runner.read_silver_snapshot_ids", return_value=curr_snaps),
        patch(
            "procurement.gold.runner.load_silver_incremental_deltas",
            side_effect=RuntimeError("Cannot read incremental: snapshot 1001 expired"),
        ),
        patch(
            "procurement.gold.runner.run_spark_gold_rebuild",
            return_value={"status": "PUBLISHED", "mode": "rebuild"},
        ) as mock_rebuild,
    ):
        res = run_spark_gold_incremental(settings=_test_cfg(), spark=fake_spark)

    assert res["status"] == "PUBLISHED"
    mock_rebuild.assert_called_once()


def test_gold_spark_cli_run_dispatch() -> None:
    with patch(
        "procurement.gold.cli.run_spark_gold_incremental",
        return_value={
            "status": "NO_OP",
            "gold_namespace": "gold_spark",
            "silver_namespace": "silver_spark",
            "award_amount_sum": "0",
            "table_counts": {},
            "wall_s": 0.4,
        },
    ) as mock_inc:
        with patch("procurement.gold.cli.settings_from_cli_args") as mock_cfg:
            mock_cfg.return_value.spark_master = "spark://192.168.1.11:7077"
            assert gold_spark_cli_main(["run", "--k3s", "--namespace", "gold_spark"]) == 0
            mock_inc.assert_called_once()
