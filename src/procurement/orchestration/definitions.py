"""Load with `dagster dev -m procurement.orchestration.definitions`."""

from datetime import timedelta

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    multiprocess_executor,
    schedule,
)

from procurement.common.dates import VIETNAM_TZ
from procurement.orchestration.bronze import BRONZE_PARTITIONS, bronze_assets
from procurement.orchestration.resources import object_storage
from procurement.orchestration.silver import silver_assets, silver_sync_iceberg_job
from procurement.orchestration.watcher import (
    bid_opening_due_sensor,
    bid_opening_seed_job,
    bid_opening_seed_schedule,
    bid_opening_seed_sensor,
    bid_opening_watch_job,
)
from procurement.orchestration.workflows import benchmark_job, quality_audit_job, quality_repair_job

bronze_job = define_asset_job(
    "bronze_daily",
    selection=AssetSelection.groups("bronze"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
    tags={"dagster/max_retries": "0"},
)

silver_job = define_asset_job(
    "silver_daily",
    selection=AssetSelection.groups("silver"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
    tags={"dagster/max_retries": "0"},
)

lakehouse_job = define_asset_job(
    "lakehouse_daily",
    selection=AssetSelection.groups("bronze") | AssetSelection.groups("silver"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
    tags={"dagster/max_retries": "0"},
)


@schedule(
    job=bronze_job,
    cron_schedule="0 8 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)
def bronze_daily_schedule(context: ScheduleEvaluationContext):
    """Refresh the last three closed dates, matching the legacy intended lookback."""
    tick = context.scheduled_execution_time.astimezone(VIETNAM_TZ)
    for offset in (3, 2, 1):
        key = (tick.date() - timedelta(days=offset)).isoformat()
        if key < BRONZE_PARTITIONS.start.date().isoformat():
            continue
        yield RunRequest(
            run_key=f"bronze:{tick.date()}:{key}",
            partition_key=key,
            run_config={
                "ops": {
                    getattr(asset, "key").to_user_string(): {"config": {"refresh": True}}
                    for asset in bronze_assets
                }
            },
        )


@schedule(
    job=silver_job,
    cron_schedule="0 9 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)
def silver_daily_schedule(context: ScheduleEvaluationContext):
    """Run Silver standardization on the last three closed dates."""
    tick = context.scheduled_execution_time.astimezone(VIETNAM_TZ)
    for offset in (3, 2, 1):
        key = (tick.date() - timedelta(days=offset)).isoformat()
        if key < BRONZE_PARTITIONS.start.date().isoformat():
            continue
        yield RunRequest(
            run_key=f"silver:{tick.date()}:{key}",
            partition_key=key,
        )


defs = Definitions(
    assets=[*bronze_assets, *silver_assets],
    jobs=[
        bronze_job,
        silver_job,
        lakehouse_job,
        silver_sync_iceberg_job,
        bid_opening_seed_job,
        bid_opening_watch_job,
        quality_audit_job,
        quality_repair_job,
        benchmark_job,
    ],
    schedules=[bronze_daily_schedule, silver_daily_schedule, bid_opening_seed_schedule],
    sensors=[bid_opening_due_sensor, bid_opening_seed_sensor],
    resources={"object_storage": object_storage},
)
