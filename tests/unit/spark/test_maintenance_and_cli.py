"""Unit tests for Iceberg maintenance (API + CLI) and Spark Silver incremental/CLI runner."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from procurement.silver.cli import main as silver_spark_cli_main
from procurement.silver.runner import (
    _build_arg_parser as build_runner_parser,
)
from procurement.silver.runner import (
    run_spark_silver_all,
    run_spark_silver_incremental,
)
from procurement.spark.cli_common import render_maintenance_summary, render_runner_summary
from procurement.spark.config import SparkSilverSettings
from procurement.spark.maintenance import (
    MaintenanceOptions,
    discover_namespace_tables,
    maintain_iceberg_table,
    run_iceberg_maintenance,
)
from procurement.spark.maintenance import (
    _build_arg_parser as build_maint_parser,
)


class _FakeRow(dict):
    def asDict(self, recursive: bool = False) -> dict[str, Any]:
        return dict(self)


class _FakeSpark:
    def __init__(self, responses: dict[str, list[_FakeRow]]) -> None:
        self.queries: list[str] = []
        self._responses = responses
        self.catalog = MagicMock()
        self.stopped = False

    def sql(self, query: str) -> Any:
        self.queries.append(query)
        result = MagicMock()
        for prefix, rows in self._responses.items():
            if prefix in query:
                result.collect.return_value = rows
                return result
        result.collect.return_value = []
        return result

    def stop(self) -> None:
        self.stopped = True


def test_maintenance_options_guards() -> None:
    with pytest.raises(ValueError, match="orphan_older_than_days"):
        MaintenanceOptions(orphan_older_than_days=0)
    with pytest.raises(ValueError, match="retain_last_snapshots"):
        MaintenanceOptions(retain_last_snapshots=0)
    with pytest.raises(ValueError, match="target_file_size_bytes"):
        MaintenanceOptions(target_file_size_bytes=0)


def test_maintain_iceberg_table_executes_four_procedures() -> None:
    fake_spark = _FakeSpark(
        {
            "rewrite_data_files": [
                _FakeRow(
                    rewritten_data_files_count=12,
                    added_data_files_count=2,
                    rewritten_bytes_count=200_000_000,
                )
            ],
            "rewrite_manifests": [
                _FakeRow(rewritten_manifests_count=8, added_manifests_count=1)
            ],
            "expire_snapshots": [
                _FakeRow(deleted_data_files_count=10, deleted_manifest_files_count=7)
            ],
            "remove_orphan_files": [
                _FakeRow(orphan_file_location="s3://bucket/a.parquet"),
                _FakeRow(orphan_file_location="s3://bucket/b.parquet"),
            ],
        }
    )

    now = datetime(2026, 11, 28, 12, 0, 0, tzinfo=UTC)
    opts = MaintenanceOptions(
        retain_last_snapshots=5,
        expire_older_than_days=7,
        orphan_older_than_days=3,
        dry_run_orphans=True,
    )
    res = maintain_iceberg_table(
        fake_spark,  # type: ignore[arg-type]
        catalog="lake",
        namespace="silver_spark",
        table_name="notice_revision",
        options=opts,
        now_utc=now,
    )

    assert res.status == "OK"
    assert res.rewritten_data_files_count == 12
    assert res.added_data_files_count == 2
    assert res.rewritten_manifests_count == 8
    assert res.added_manifests_count == 1
    assert res.deleted_data_files_count == 10
    assert res.deleted_manifest_files_count == 7
    assert res.orphan_files_count == 2
    assert res.orphan_dry_run is True
    assert len(fake_spark.queries) == 4
    assert "CALL lake.system.rewrite_data_files" in fake_spark.queries[0]
    assert "CALL lake.system.rewrite_manifests" in fake_spark.queries[1]
    assert "older_than => TIMESTAMP '2026-11-21 12:00:00.000'" in fake_spark.queries[2]
    assert "retain_last => 5" in fake_spark.queries[2]
    assert "older_than => TIMESTAMP '2026-11-25 12:00:00.000'" in fake_spark.queries[3]
    assert "dry_run => true" in fake_spark.queries[3]
    fake_spark.catalog.clearCache.assert_called_once()


def test_maintain_iceberg_table_rejects_bad_identifier() -> None:
    fake_spark = _FakeSpark({})
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        maintain_iceberg_table(
            fake_spark,  # type: ignore[arg-type]
            catalog="lake",
            namespace="silver_spark; DROP TABLE x",
            table_name="notice_revision",
        )


def test_run_iceberg_maintenance_discovers_tables_and_sums_totals() -> None:
    fake_spark = _FakeSpark(
        {
            "SHOW TABLES": [
                _FakeRow(tableName="project_revision"),
                _FakeRow(tableName="award_lot"),
            ],
            "rewrite_data_files": [
                _FakeRow(
                    rewritten_data_files_count=5,
                    added_data_files_count=1,
                    rewritten_bytes_count=50_000,
                )
            ],
            "rewrite_manifests": [
                _FakeRow(rewritten_manifests_count=3, added_manifests_count=1)
            ],
            "expire_snapshots": [
                _FakeRow(deleted_data_files_count=4, deleted_manifest_files_count=2)
            ],
            "remove_orphan_files": [],
        }
    )

    assert discover_namespace_tables(
        fake_spark,  # type: ignore[arg-type]
        catalog="lake",
        namespace="silver_spark",
    ) == ["award_lot", "project_revision"]

    cfg = SparkSilverSettings(
        app_database_url="postgresql://test:test@localhost/test",
        iceberg_namespace="silver_spark",
    )
    summary = run_iceberg_maintenance(
        namespace="silver_spark",
        settings=cfg,
        spark=fake_spark,  # type: ignore[arg-type]
    )
    assert summary["status"] == "OK"
    assert summary["tables_total"] == 2
    assert summary["totals"]["rewritten_data_files_count"] == 10
    assert summary["totals"]["added_data_files_count"] == 2
    assert summary["totals"]["deleted_data_files_count"] == 8
    assert not fake_spark.stopped  # caller-owned SparkSession stays open


def test_run_spark_silver_incremental_noop_when_no_bronze() -> None:
    cfg = SparkSilverSettings(
        app_database_url="postgresql://test:test@localhost/test",
        iceberg_namespace="silver_spark",
    )
    fake_engine = MagicMock()
    with (
        patch("procurement.silver.runner._create_pg_engine", return_value=fake_engine),
        patch("procurement.silver.runner.resolve_bronze_date_bounds", return_value=None),
        patch("procurement.silver.runner.build_spark_session") as mock_spark,
    ):
        res = run_spark_silver_incremental("project", settings=cfg)

    assert res["status"] == "NO_OP"
    assert res["resource"] == "project"
    mock_spark.assert_not_called()
    fake_engine.dispose.assert_called_once()


def test_run_spark_silver_incremental_selects_daily_vs_backfill_mode() -> None:
    cfg = SparkSilverSettings(
        app_database_url="postgresql://test:test@localhost/test",
        iceberg_namespace="silver_spark",
    )
    fake_engine = MagicMock()
    with (
        patch("procurement.silver.runner._create_pg_engine", return_value=fake_engine),
        patch(
            "procurement.silver.runner.resolve_bronze_date_bounds",
            return_value=(date(2026, 10, 28), date(2026, 10, 31)),
        ),
        patch(
            "procurement.silver.runner.run_spark_silver",
            return_value={"resource": "project", "status": "PUBLISHED", "batches": []},
        ) as mock_run,
    ):
        res = run_spark_silver_incremental("project", settings=cfg)

    assert res["status"] == "PUBLISHED"
    assert res["discovered_start_date"] == "2026-10-28"
    assert res["discovered_end_date"] == "2026-10-31"
    assert mock_run.call_args.kwargs["mode"] == "daily"


def test_run_spark_silver_all_validates_dates_for_backfill() -> None:
    cfg = SparkSilverSettings(
        app_database_url="postgresql://test:test@localhost/test",
        iceberg_namespace="silver_spark",
    )
    with pytest.raises(ValueError, match="--start-date and --end-date"):
        run_spark_silver_all(["project"], mode="backfill", settings=cfg)


def test_cli_parsers() -> None:
    m_args = build_maint_parser().parse_args(
        ["--namespace", "silver_spark", "--tables", "a,b", "--dry-run-orphans", "--retain-last", "3"]
    )
    assert m_args.namespace == "silver_spark"
    assert m_args.tables == "a,b"
    assert m_args.dry_run_orphans is True
    assert m_args.retain_last == 3

    r_args = build_runner_parser().parse_args(
        ["--resource", "notice,khlcnt", "--mode", "backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-31", "--k3s"]
    )
    assert r_args.resource == "notice,khlcnt"
    assert r_args.mode == "backfill"
    assert r_args.start_date == "2025-01-01"
    assert r_args.end_date == "2025-01-31"
    assert r_args.k3s is True


def test_k3s_auto_detects_lan_ip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DATABASE_URL", "postgresql://test:test@localhost/test")
    monkeypatch.delenv("SPARK_MASTER", raising=False)
    monkeypatch.delenv("SPARK_K3S_MASTER", raising=False)
    monkeypatch.delenv("SPARK_LOCAL_IP", raising=False)
    monkeypatch.delenv("SPARK_DRIVER_MEMORY", raising=False)

    with patch("procurement.spark.config.detect_lan_ip", return_value="192.168.1.11"):
        cfg = SparkSilverSettings.from_environment(k3s=True)

    assert cfg.spark_master == "spark://192.168.1.11:7077"
    assert cfg.spark_driver_host == "192.168.1.11"
    assert cfg.spark_driver_memory == "3g"


def test_remote_master_auto_detects_driver_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DATABASE_URL", "postgresql://test:test@localhost/test")
    monkeypatch.delenv("SPARK_LOCAL_IP", raising=False)

    with patch("procurement.spark.config.detect_lan_ip", return_value="192.168.1.90") as mock_ip:
        cfg = SparkSilverSettings.from_environment(spark_master="spark://192.168.1.11:7077")

    mock_ip.assert_called_once_with("192.168.1.11")
    assert cfg.spark_master == "spark://192.168.1.11:7077"
    assert cfg.spark_driver_host == "192.168.1.90"


def test_silver_spark_cli_dispatch() -> None:
    with patch("procurement.silver.cli.runner_main", return_value=0) as mock_run:
        assert silver_spark_cli_main(["run", "--k3s"]) == 0
        mock_run.assert_called_once_with(["--mode", "incremental", "--k3s"])

    with patch("procurement.silver.cli.runner_main", return_value=0) as mock_backfill:
        assert silver_spark_cli_main(["backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-31"]) == 0
        mock_backfill.assert_called_once_with(
            ["--mode", "backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-31"]
        )

    with patch("procurement.silver.cli.maintenance_main", return_value=0) as mock_maint:
        assert silver_spark_cli_main(["maintain", "--k3s", "--dry-run-orphans"]) == 0
        mock_maint.assert_called_once_with(["--k3s", "--dry-run-orphans"])


def test_rich_renderers_smoke() -> None:
    render_runner_summary(
        {
            "namespace": "silver_spark",
            "mode": "incremental",
            "status": "NO_OP",
            "wall_s": 0.12,
            "resources_failed": [],
            "results": [
                {
                    "resource": "project",
                    "status": "NO_OP",
                    "batches": [],
                    "missing_bronze_dates": [],
                    "wall_s": 0.05,
                }
            ],
        },
        master="spark://192.168.1.11:7077",
    )
    render_maintenance_summary(
        {
            "catalog": "lake",
            "namespace": "silver_spark",
            "status": "OK",
            "tables_total": 1,
            "wall_s": 1.23,
            "options": {"dry_run_orphans": True},
            "totals": {
                "rewritten_data_files_count": 2,
                "added_data_files_count": 1,
                "rewritten_manifests_count": 3,
                "added_manifests_count": 1,
                "deleted_data_files_count": 1,
                "orphan_files_count": 0,
            },
            "tables": [
                {
                    "table": "project_revision",
                    "status": "OK",
                    "rewritten_data_files_count": 2,
                    "added_data_files_count": 1,
                    "rewritten_manifests_count": 3,
                    "added_manifests_count": 1,
                    "deleted_data_files_count": 1,
                    "orphan_files_count": 0,
                    "wall_s": 1.23,
                    "error": None,
                }
            ],
        },
        master="spark://192.168.1.11:7077",
    )
