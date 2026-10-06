"""Dagster wakes the watcher; WatchStore retains all procurement-specific state."""

from pathlib import Path

from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Failure,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    SkipReason,
    asset_sensor,
    job,
    op,
    sensor,
)

from procurement.common.settings import settings
from procurement.ingestion.bid_opening_watch import WatchStore, namespace
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.jobs.lock import execution_lock
from procurement.orchestration.bronze import INGESTION_POOL
from procurement.storage.control import DayCommitUncertainError
from procurement.tools.watch_bid_opening import run_watch

ACTIVE = [DagsterRunStatus.NOT_STARTED, DagsterRunStatus.QUEUED,
          DagsterRunStatus.STARTING, DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]


def due_days():
    store = WatchStore(settings.BID_OPENING_WATCH_PATH, read_only=True)
    try:
        return store.due_days(limit=settings.BID_OPENING_WATCH_MAX_DAYS)
    finally:
        store.close()


@op(required_resource_keys={"object_storage"}, pool=INGESTION_POOL,
    retry_policy=RetryPolicy(max_retries=0))
def seed_bid_opening(context):
    with execution_lock(Path(settings.INGESTION_LOCK_DIR)):
        report = run_watch(context.resources.object_storage, mode="seed")
    context.add_output_metadata({"report": MetadataValue.json(report)})
    return report


@op(required_resource_keys={"object_storage"}, pool=INGESTION_POOL,
    retry_policy=RetryPolicy(max_retries=0))
def check_bid_opening(context):
    try:
        with execution_lock(Path(settings.INGESTION_LOCK_DIR)):
            # Re-evaluate after queueing: another run may already have handled these dates.
            if not due_days():
                return {"skipped": "No closed dates are due"}
            report = run_watch(context.resources.object_storage, mode="check",
                               seed_before_check=False)
        context.add_output_metadata({"report": MetadataValue.json(report)})
        return report
    except DayCommitUncertainError as exc:
        raise Failure("Reconcile the uncertain bid-opening commit before retrying",
                      allow_retries=False) from exc


@job(tags={"dagster/max_retries": "0"})
def bid_opening_seed_job():
    seed_bid_opening()


@job(tags={"dagster/max_retries": "0"})
def bid_opening_watch_job():
    check_bid_opening()


@sensor(job=bid_opening_watch_job, minimum_interval_seconds=60,
        default_status=DefaultSensorStatus.STOPPED)
def bid_opening_due_sensor(context):
    if context.instance.get_runs(
        filters=RunsFilter(job_name=bid_opening_watch_job.name, statuses=ACTIVE), limit=1,
    ):
        return SkipReason("A watcher run is already queued or running")
    rows = due_days()
    if not rows:
        return SkipReason("No closed dates are due; seed the watch store first if empty")
    # Same due state has one run; failed runs require explicit re-execution.
    # Successful checks advance next_check, allowing the next generation to run.
    return RunRequest(run_key=calculate_content_hash([namespace(), rows]))


@asset_sensor(asset_key=AssetKey("bronze_notify_contractor"), job=bid_opening_seed_job,
              minimum_interval_seconds=60, default_status=DefaultSensorStatus.STOPPED)
def bid_opening_seed_sensor(context, _event):
    # Asset sensors coalesce events. Full seed reconciles ALL committed dates,
    # so backfills with many materializations between ticks cannot lose a date.
    return RunRequest(run_key=context.cursor)


# Also reconciles CLI imports and commits whose later quality check failed.
bid_opening_seed_schedule = ScheduleDefinition(
    name="bid_opening_seed_daily", job=bid_opening_seed_job,
    cron_schedule="0 9 * * *", execution_timezone="Asia/Ho_Chi_Minh",
    default_status=DefaultScheduleStatus.STOPPED,
)
