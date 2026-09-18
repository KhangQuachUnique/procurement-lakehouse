import logging
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import s3fs

from procurement.common.dates import api_day_window
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import DailyResult
from procurement.models.control import RunManifest, RunStatus
from procurement.storage.control import write_run_manifest
from procurement.storage.execution import ExecutionHeartbeat

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    @property
    def partition_date(self) -> date:
        return self.start


def split_by_day(start: date, end: date) -> list[DateWindow]:
    if start > end:
        raise ValueError("start must be before or equal to end")
    return [
        DateWindow(start=start + timedelta(days=offset), end=start + timedelta(days=offset))
        for offset in range((end - start).days + 1)
    ]


def to_api_window(window: DateWindow) -> tuple[str, str]:
    return api_day_window(window.partition_date)


def _now() -> datetime:
    return datetime.now(UTC)


def _final_status(success_dates: int, failed_dates: int) -> RunStatus:
    if failed_dates == 0:
        return RunStatus.SUCCESS
    if success_dates == 0:
        return RunStatus.FAILED
    return RunStatus.PARTIAL_FAILED


def run_batch_range(
    start: date,
    end: date,
    *,
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    run_day: Callable[[str, date], DailyResult],
    heartbeat: bool = False,
) -> str:
    """Run a date range under one run_id while isolating each source_date attempt."""

    windows = split_by_day(start, end)
    run_id = uuid.uuid4().hex
    manifest = RunManifest(
        run_id=run_id,
        source=identity.source,
        resource=identity.resource,
        start_date=start,
        end_date=end,
        status=RunStatus.RUNNING,
        total_dates=len(windows),
        started_at=_now(),
    )
    write_run_manifest(fs, identity, manifest)
    logger.info(
        "batch_started run_id=%s resource=%s start=%s end=%s",
        run_id,
        identity.resource,
        start,
        end,
    )

    monitor = ExecutionHeartbeat(fs, identity, run_id) if heartbeat else nullcontext()
    with monitor:
        success_dates = 0
        failed_dates = 0
        for window in windows:
            source_date = window.partition_date
            try:
                result = run_day(run_id, source_date)
                if result["status"] == "success":
                    success_dates += 1
                else:
                    failed_dates += 1
            except Exception:
                failed_dates += 1
                logger.exception(
                    "daily_run_failed run_id=%s source_date=%s resource=%s",
                    run_id,
                    source_date,
                    identity.resource,
                )

            manifest = manifest.model_copy(
                update={
                    "success_dates": success_dates,
                    "failed_dates": failed_dates,
                }
            )
            write_run_manifest(fs, identity, manifest)

        completed = manifest.model_copy(
            update={
                "status": _final_status(success_dates, failed_dates),
                "success_dates": success_dates,
                "failed_dates": failed_dates,
                "completed_at": _now(),
            }
        )
        write_run_manifest(fs, identity, completed)
        logger.info(
            "batch_completed run_id=%s resource=%s status=%s success_dates=%s failed_dates=%s",
            run_id,
            identity.resource,
            completed.status.value,
            success_dates,
            failed_dates,
        )
    return run_id
