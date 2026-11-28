"""Dagster jobs and schedules for the Apache Spark 4.0 + Iceberg Silver & Two-Tier Gold pipeline."""

from datetime import date, datetime
from typing import Any

from dagster import (
    Config,
    DefaultScheduleStatus,
    In,
    Nothing,
    OpExecutionContext,
    Output,
    ScheduleDefinition,
    job,
    op,
)


class SparkSilverConfig(Config):
    resources: str = "all"
    mode: str = "incremental"  # incremental | daily | backfill
    start_date: str | None = None
    end_date: str | None = None
    namespace: str = "silver_spark"
    k3s: bool = True
    spark_master: str | None = None
    driver_memory: str = "3g"
    force: bool = False


class SparkGoldConfig(Config):
    mode: str = "incremental"  # incremental | rebuild
    event_start: str = "2022-01-01"
    event_end: str | None = None  # defaults to today (Asia/Ho_Chi_Minh)
    observation_day: str | None = None
    silver_namespace: str = "silver_spark"
    core_namespace: str = "gold_spark_core"
    gold_namespace: str = "gold_spark"
    k3s: bool = True
    spark_master: str | None = None
    driver_memory: str = "3g"


class SparkMaintenanceConfig(Config):
    namespaces: list[str] = ["silver_spark", "gold_spark_core", "gold_spark"]
    retain_last_snapshots: int = 5
    expire_older_than_days: int = 7
    orphan_older_than_days: int = 3
    dry_run_orphans: bool = False
    k3s: bool = True
    spark_master: str | None = None
    driver_memory: str = "3g"


@op(name="spark_silver_run")
def spark_silver_op(context: OpExecutionContext, config: SparkSilverConfig) -> Output[dict[str, Any]]:
    """Run Spark Silver incremental (or backfill) across all 6 procurement resources."""
    from procurement_spark.platform.config import SparkSilverSettings
    from procurement_spark.silver.runner import run_spark_silver_all

    cfg = SparkSilverSettings.from_environment(
        load_dotenv_file=True,
        namespace=config.namespace,
        k3s=config.k3s,
        spark_master=config.spark_master,
        spark_driver_memory=config.driver_memory,
    )
    resources = (
        None
        if config.resources.strip().lower() == "all"
        else [r.strip() for r in config.resources.split(",") if r.strip()]
    )
    summary = run_spark_silver_all(
        resources=resources,
        mode=config.mode,  # type: ignore[arg-type]
        start_date=date.fromisoformat(config.start_date) if config.start_date else None,
        end_date=date.fromisoformat(config.end_date) if config.end_date else None,
        force=config.force,
        settings=cfg,
    )
    context.log.info(
        "Spark Silver %s (%s): status=%s wall_s=%s",
        summary["namespace"],
        summary["mode"],
        summary["status"],
        summary["wall_s"],
    )
    if summary["status"] == "FAILED":
        raise RuntimeError(f"Spark Silver failed for resources: {summary['resources_failed']}")
    return Output(
        summary,
        metadata={
            "namespace": summary["namespace"],
            "mode": summary["mode"],
            "status": summary["status"],
            "wall_s": summary["wall_s"],
            "spark_master": cfg.spark_master,
        },
    )


@op(name="spark_gold_run", ins={"upstream": In(Nothing)})
def spark_gold_op(context: OpExecutionContext, config: SparkGoldConfig) -> Output[dict[str, Any]]:
    """Run Two-Tier Spark Gold incremental (start-snapshot-id + MERGE INTO) or full rebuild."""
    from procurement.common.dates import VIETNAM_TZ
    from procurement_spark.gold.runner import run_spark_gold_incremental, run_spark_gold_rebuild
    from procurement_spark.platform.config import SparkSilverSettings

    cfg = SparkSilverSettings.from_environment(
        load_dotenv_file=True,
        namespace=config.silver_namespace,
        k3s=config.k3s,
        spark_master=config.spark_master,
        spark_driver_memory=config.driver_memory,
    )
    ev_start = date.fromisoformat(config.event_start)
    ev_end = (
        date.fromisoformat(config.event_end)
        if config.event_end
        else datetime.now(VIETNAM_TZ).date()
    )
    obs_day = date.fromisoformat(config.observation_day) if config.observation_day else ev_end

    if config.mode == "rebuild":
        summary = run_spark_gold_rebuild(
            event_start=ev_start,
            event_end=ev_end,
            observation_day=obs_day,
            silver_namespace=config.silver_namespace,
            gold_namespace=config.gold_namespace,
            core_namespace=config.core_namespace,
            persist_core=True,
            settings=cfg,
        )
    else:
        summary = run_spark_gold_incremental(
            event_start=ev_start,
            event_end=ev_end,
            observation_day=obs_day,
            silver_namespace=config.silver_namespace,
            gold_namespace=config.gold_namespace,
            core_namespace=config.core_namespace,
            settings=cfg,
        )

    context.log.info(
        "Spark Gold %s <- %s (%s): status=%s wall_s=%s",
        summary["gold_namespace"],
        summary["silver_namespace"],
        config.mode,
        summary["status"],
        summary["wall_s"],
    )
    return Output(
        summary,
        metadata={
            "gold_namespace": summary["gold_namespace"],
            "silver_namespace": summary["silver_namespace"],
            "status": summary["status"],
            "award_amount_sum": str(summary.get("award_amount_sum", "0")),
            "wall_s": summary["wall_s"],
            "spark_master": cfg.spark_master,
        },
    )


@op(name="spark_iceberg_maintenance")
def spark_maintenance_op(
    context: OpExecutionContext, config: SparkMaintenanceConfig
) -> Output[dict[str, Any]]:
    """Run Iceberg compaction, manifest rewrite, snapshot expiration, and orphan cleanup."""
    from procurement_spark.platform.config import SparkSilverSettings
    from procurement_spark.platform.maintenance import MaintenanceOptions, run_iceberg_maintenance
    from procurement_spark.platform.spark_session import build_spark_session

    cfg = SparkSilverSettings.from_environment(
        load_dotenv_file=True,
        k3s=config.k3s,
        spark_master=config.spark_master,
        spark_driver_memory=config.driver_memory,
    )
    opts = MaintenanceOptions(
        retain_last_snapshots=config.retain_last_snapshots,
        expire_older_than_days=config.expire_older_than_days,
        orphan_older_than_days=config.orphan_older_than_days,
        dry_run_orphans=config.dry_run_orphans,
    )

    spark = build_spark_session(cfg)
    results: dict[str, Any] = {}
    failed: list[str] = []
    try:
        for ns in config.namespaces:
            res = run_iceberg_maintenance(
                namespace=ns,
                options=opts,
                settings=cfg,
                spark=spark,
            )
            results[ns] = res
            if res["status"] != "OK":
                failed.append(ns)
            context.log.info(
                "Maintenance %s: status=%s tables=%d wall_s=%s",
                ns,
                res["status"],
                res["tables_total"],
                res["wall_s"],
            )
    finally:
        spark.stop()

    if failed:
        raise RuntimeError(f"Iceberg maintenance failed for namespaces: {failed}")
    return Output(
        {"status": "OK", "namespaces": results},
        metadata={"namespaces": ", ".join(config.namespaces), "spark_master": cfg.spark_master},
    )


@job(
    name="spark_silver_daily_job",
    description="Incremental Spark Silver sync across all 6 procurement resources.",
    tags={"dagster/max_retries": "0"},
)
def spark_silver_daily_job():
    spark_silver_op()


@job(
    name="spark_gold_daily_job",
    description="Incremental (or full rebuild) Two-Tier Spark Gold job.",
    tags={"dagster/max_retries": "0"},
)
def spark_gold_daily_job():
    spark_gold_op()


@job(
    name="spark_lakehouse_daily_job",
    description="Chained daily Spark Silver incremental -> Two-Tier Spark Gold incremental.",
    tags={"dagster/max_retries": "0"},
)
def spark_lakehouse_daily_job():
    spark_gold_op(upstream=spark_silver_op())


@job(
    name="spark_iceberg_maintenance_job",
    description="Weekly Iceberg maintenance (compaction, manifests, expire snapshots, orphan cleanup).",
    tags={"dagster/max_retries": "0"},
)
def spark_iceberg_maintenance_job():
    spark_maintenance_op()


spark_lakehouse_daily_schedule = ScheduleDefinition(
    name="spark_lakehouse_daily_schedule",
    job=spark_lakehouse_daily_job,
    cron_schedule="30 8 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)

spark_iceberg_maintenance_weekly_schedule = ScheduleDefinition(
    name="spark_iceberg_maintenance_weekly_schedule",
    job=spark_iceberg_maintenance_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)

SPARK_LAKEHOUSE_JOBS = [
    spark_silver_daily_job,
    spark_gold_daily_job,
    spark_lakehouse_daily_job,
    spark_iceberg_maintenance_job,
]

SPARK_LAKEHOUSE_SCHEDULES = [
    spark_lakehouse_daily_schedule,
    spark_iceberg_maintenance_weekly_schedule,
]
