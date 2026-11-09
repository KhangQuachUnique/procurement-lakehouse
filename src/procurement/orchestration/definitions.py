"""Candidate replacement code location; no schedules enabled or production switch."""

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
from procurement.orchestration.gold_assets import (
    GOLD_ASSET_CHECKS,
    GOLD_DIMENSIONS_ASSETS,
    gold_facts_monthly,
)
from procurement.orchestration.gold_v2 import gold_v2_active_release, gold_v2_run_job
from procurement.orchestration.reconcile import reconcile_lakehouse_job
from procurement.orchestration.resources import object_storage
from procurement.orchestration.silver import SILVER_GROUPS, silver_assets, silver_sync_iceberg_job
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

gold_dimensions_job = define_asset_job(
    "gold_dimensions_sync",
    selection=AssetSelection.groups("gold_dimensions"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
    tags={"dagster/max_retries": "0"},
)

gold_facts_job = define_asset_job(
    "gold_facts_monthly_job",
    selection=AssetSelection.groups("gold_facts"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
    tags={"dagster/max_retries": "0"},
)

lakehouse_end_to_end_job = define_asset_job(
    "lakehouse_end_to_end",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
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
                    asset.key.to_user_string(): {"config": {"refresh": True}}
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


@schedule(
    job=lakehouse_end_to_end_job,
    cron_schedule="0 10 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)
def lakehouse_end_to_end_schedule(context: ScheduleEvaluationContext):
    """Candidate schedule requires explicit Gold v2 runtime config before enablement.

    One closed day through dependency-checked Bronze, Silver and Gold.

    Opt-in deployment only; do not enable alongside overlapping independent
    schedules. Bootstrap/migrate Gold state before enabling this schedule.
    """
    tick = context.scheduled_execution_time.astimezone(VIETNAM_TZ)
    key = (tick.date() - timedelta(days=1)).isoformat()
    if key < BRONZE_PARTITIONS.start.date().isoformat():
        return
    yield RunRequest(
        run_key=f"lakehouse:{tick.date()}:{key}",
        partition_key=key,
        run_config={
            "ops": {
                asset.key.to_user_string(): {"config": {"refresh": True}} for asset in bronze_assets
            }
            | {
                "silver_" + group.resource_name: {"config": {"readiness_evidence": True}}
                for group in SILVER_GROUPS
            }
        },
    )


defs = Definitions(
    assets=[
        *bronze_assets,
        *silver_assets,
        *GOLD_DIMENSIONS_ASSETS,
        gold_facts_monthly,
        gold_v2_active_release,
    ],
    asset_checks=GOLD_ASSET_CHECKS,
    jobs=[
        lakehouse_end_to_end_job,
        gold_v2_run_job,
        gold_dimensions_job,
        gold_facts_job,
        bronze_job,
        silver_job,
        lakehouse_job,
        silver_sync_iceberg_job,
        reconcile_lakehouse_job,
        quality_audit_job,
        quality_repair_job,
        benchmark_job,
    ],
    schedules=[bronze_daily_schedule, silver_daily_schedule],
    resources={"object_storage": object_storage},
)
