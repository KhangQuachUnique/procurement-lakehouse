import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol
from zoneinfo import ZoneInfo

import s3fs

from procurement.common.errors import sanitize_error_message
from procurement.common.logging_config import configure_logging
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.sources.muasamcong.contractor_result.resource import (
    CONTRACTOR_RESULT_IDENTITY,
)
from procurement.ingestion.sources.muasamcong.khlcnt.resource import KHLCNT_IDENTITY
from procurement.ingestion.sources.muasamcong.notify_contractor.resource import (
    NOTIFY_CONTRACTOR_IDENTITY,
)
from procurement.ingestion.sources.muasamcong.project.resource import PROJECT_IDENTITY
from procurement.jobs.crawl_contractor_result import crawl_contractor_result
from procurement.jobs.crawl_khlcnt import crawl_khlcnt
from procurement.jobs.crawl_notify_contractor import crawl_notify_contractor
from procurement.jobs.crawl_project import crawl_project
from procurement.models.control import RunStatus
from procurement.storage.control import read_run_manifest
from procurement.storage.object_store import create_s3_filesystem

VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
MAX_RESOURCE_WORKERS = 2


class CrawlFunction(Protocol):
    def __call__(
        self,
        start: date,
        end: date,
        *,
        page_size: int = 50,
    ) -> str: ...


@dataclass(frozen=True)
class ResourceJob:
    identity: ResourceIdentity
    crawl: CrawlFunction


@dataclass(frozen=True)
class ResourceExecution:
    resource: str
    run_id: str | None
    error: str | None = None


@dataclass(frozen=True)
class ResourceRunResult:
    resource: str
    run_id: str | None
    status: str
    error: str | None = None


def _resource_jobs() -> tuple[ResourceJob, ...]:
    # Business order is preserved in the final summary even though execution is concurrent.
    return (
        ResourceJob(PROJECT_IDENTITY, crawl_project),
        ResourceJob(KHLCNT_IDENTITY, crawl_khlcnt),
        ResourceJob(NOTIFY_CONTRACTOR_IDENTITY, crawl_notify_contractor),
        ResourceJob(CONTRACTOR_RESULT_IDENTITY, crawl_contractor_result),
    )


def _today_vn() -> date:
    return datetime.now(VIETNAM_TZ).date()


def _year_range(year: int, *, today: date) -> tuple[date, date]:
    if year < 1:
        raise ValueError("year must be a positive integer")
    if year >= today.year:
        raise ValueError("crawl_all only accepts fully closed calendar years")
    return date(year, 1, 1), date(year, 12, 31)


def _read_run_status(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    run_id: str,
) -> str:
    manifest = read_run_manifest(fs, identity, run_id)
    return "unknown" if manifest is None else manifest.status.value


def _execute_resource_job(
    job: ResourceJob,
    start: date,
    end: date,
    page_size: int,
) -> ResourceExecution:
    """Run one resource inside its worker process and return a serializable result."""

    configure_logging()
    try:
        run_id = job.crawl(start, end, page_size=page_size)
        return ResourceExecution(resource=job.identity.resource, run_id=run_id)
    # Resource jobs are independent. Capture a resource crash so the pool can continue
    # scheduling the remaining jobs.
    except Exception as exc:  # noqa: BLE001
        return ResourceExecution(
            resource=job.identity.resource,
            run_id=None,
            error=sanitize_error_message(str(exc)),
        )


def crawl_all(year: int, *, page_size: int = 50) -> list[ResourceRunResult]:
    """Backfill all Muasamcong resources with at most two worker processes.

    This job is orchestration only. It does not retry failed dates and does not decide
    whether the year is globally complete; coverage is projected by Ops from committed
    DayManifest records.
    """

    if page_size <= 0:
        raise ValueError("page_size must be greater than zero")

    start, end = _year_range(year, today=_today_vn())
    jobs = _resource_jobs()
    filesystem = create_s3_filesystem()
    results_by_resource: dict[str, ResourceRunResult] = {}
    worker_count = min(MAX_RESOURCE_WORKERS, len(jobs))

    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_to_job = {
            executor.submit(_execute_resource_job, job, start, end, page_size): job
            for job in jobs
        }

        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                execution = future.result()
            # Covers worker-process failures such as serialization or process crashes.
            except Exception as exc:  # noqa: BLE001
                results_by_resource[job.identity.resource] = ResourceRunResult(
                    resource=job.identity.resource,
                    run_id=None,
                    status="crashed",
                    error=sanitize_error_message(str(exc)),
                )
                continue

            if execution.run_id is None:
                results_by_resource[job.identity.resource] = ResourceRunResult(
                    resource=job.identity.resource,
                    run_id=None,
                    status="crashed",
                    error=execution.error,
                )
                continue

            status = _read_run_status(filesystem, job.identity, execution.run_id)
            results_by_resource[job.identity.resource] = ResourceRunResult(
                resource=job.identity.resource,
                run_id=execution.run_id,
                status=status,
            )

    return [results_by_resource[job.identity.resource] for job in jobs]


def _print_summary(year: int, results: list[ResourceRunResult]) -> None:
    print(f"Backfill {year} finished")
    for result in results:
        run_id = result.run_id or "-"
        line = f"{result.resource:<22} {result.status:<15} run_id={run_id}"
        if result.error:
            line += f" error={result.error}"
        print(line)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl all Muasamcong resources for one closed year with two workers"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--page-size", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    results = crawl_all(args.year, page_size=args.page_size)
    _print_summary(args.year, results)

    # This is the outcome of this execution only. Overall year coverage remains an Ops concern.
    if any(result.status != RunStatus.SUCCESS.value for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
