"""Production Dagster code location: Bronze ingestion + Bid-Opening watcher + Spark Silver/Gold Lakehouse."""

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
from procurement.orchestration.spark_lakehouse import (
    SPARK_LAKEHOUSE_JOBS,
    SPARK_LAKEHOUSE_SCHEDULES,
)
from procurement.orchestration.watcher import (
    bid_opening_due_sensor,
    bid_opening_seed_job,
    bid_opening_seed_schedule,
    bid_opening_seed_sensor,
    bid_opening_watch_job,
)

# ============================================================================
# PIPELINE JOBS
# ============================================================================

bronze_job = define_asset_job(
    "bronze_daily",
    selection=AssetSelection.groups("bronze"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
    tags={"dagster/max_retries": "0"},
)

# ============================================================================
# SCHEDULES
# ============================================================================


@schedule(
    job=bronze_job,
    cron_schedule="0 8 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)
def bronze_daily_schedule(context: ScheduleEvaluationContext):
    """Refresh the last three closed dates, matching the intended lookback."""
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
                    asset.key.to_user_string(): {"config": {"refresh": True}}
                    for asset in bronze_assets
                    if asset.key.to_user_string() != "bronze_bid_opening"
                }
            },
        )


# ============================================================================
# DEFINITIONS ROOT
# ============================================================================

defs = Definitions(
    assets=[*bronze_assets],
    jobs=[
        bronze_job,
        bid_opening_seed_job,
        bid_opening_watch_job,
        *SPARK_LAKEHOUSE_JOBS,
    ],
    schedules=[
        bronze_daily_schedule,
        bid_opening_seed_schedule,
        *SPARK_LAKEHOUSE_SCHEDULES,
    ],
    sensors=[bid_opening_due_sensor, bid_opening_seed_sensor],
    resources={"object_storage": object_storage},
)
