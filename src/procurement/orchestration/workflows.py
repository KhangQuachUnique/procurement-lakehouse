"""Auxiliary jobs call existing domain workflows synchronously in the shared API pool."""

from pathlib import Path
from typing import Literal

from dagster import Config, Failure, MetadataValue, RetryPolicy, job, op

from procurement.benchmark.models import BenchmarkConfig
from procurement.benchmark.runner import BenchmarkRunner
from procurement.benchmark.store import BenchmarkStore
from procurement.common.dates import today_vn
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.jobs.lock import execution_lock
from procurement.orchestration.bronze import INGESTION_POOL
from procurement.quality.audit import run_audit
from procurement.quality.contracts import load_config
from procurement.quality.workflow import run_workflow


class QualityJobConfig(Config):
    resource: Literal["notify_contractor", "bid_opening"] = "notify_contractor"
    year: int
    directory: str
    config_path: str | None = None


def validate_year(year):
    if not 1900 <= year < today_vn().year:
        raise Failure("Select a completed year", allow_retries=False)


@op(required_resource_keys={"object_storage"}, pool=INGESTION_POOL,
    retry_policy=RetryPolicy(max_retries=0))
def quality_audit(context, config: QualityJobConfig):
    validate_year(config.year)
    directory = Path(config.directory)
    report = run_audit(context.resources.object_storage, resource=config.resource, year=config.year,
                       config=load_config(config.config_path, resource=config.resource),
                       output=directory, resume=(directory / "selection.json").exists())
    context.add_output_metadata({"summary": str(directory / "summary.json"),
                                 "quality": MetadataValue.json(report["quality_status"])})
    if not report["fully_verified"]:
        raise Failure("Quality audit requires attention; inspect the persisted report",
                      allow_retries=False)
    return report


@op(required_resource_keys={"object_storage"}, pool=INGESTION_POOL,
    retry_policy=RetryPolicy(max_retries=0))
def quality_repair(context, config: QualityJobConfig):
    validate_year(config.year)
    if not settings.MUASAMCONG_TOKEN:
        raise Failure("MUASAMCONG_TOKEN is missing", allow_retries=False)
    opening = config.resource == "bid_opening"
    budget = RequestBudget(settings.BID_OPENING_MAX_INFLIGHT if opening
                           else settings.MUASAMCONG_MAX_INFLIGHT,
                           min_interval=settings.BID_OPENING_REQUEST_INTERVAL_SECONDS if opening
                           else settings.MUASAMCONG_REQUEST_INTERVAL_SECONDS)
    with execution_lock(Path(settings.INGESTION_LOCK_DIR)), MuasamcongClient(
        token=settings.MUASAMCONG_TOKEN, request_budget=budget,
        max_attempts=settings.MUASAMCONG_MAX_ATTEMPTS,
        max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
    ) as client:
        report = run_workflow(context.resources.object_storage, client, year=config.year,
                              directory=config.directory, config_path=config.config_path,
                              resource=config.resource,
                              detail_workers=settings.BID_OPENING_DETAIL_WORKERS if opening else 3)
    context.add_output_metadata({"report": report["report"], "status": report["status"]})
    if report["status"] != "complete":
        raise Failure("Quality repair requires attention; resume with the same directory",
                      allow_retries=False)
    return report


class BenchmarkJobConfig(Config):
    plan_json: str


@op(pool=INGESTION_POOL, retry_policy=RetryPolicy(max_retries=0))
def benchmark(context, config: BenchmarkJobConfig):
    plan = BenchmarkConfig.model_validate_json(config.plan_json)
    if not settings.MUASAMCONG_TOKEN:
        raise Failure("MUASAMCONG_TOKEN is missing", allow_retries=False)
    with execution_lock(Path(settings.INGESTION_LOCK_DIR)):
        store = BenchmarkStore(settings.BENCHMARK_STATE_DIR, settings.BENCHMARK_EXPORT_DIR)
        run_id = store.create(plan)
        context.add_output_metadata({"benchmark_id": run_id, "artifacts": str(store.folder(run_id))})
        BenchmarkRunner(store, run_id).execute()
        report = store.get(run_id)
    if report["status"] != "completed":
        raise Failure("Benchmark did not complete; inspect its result artifacts", allow_retries=False)
    return report["summary"]


@job(tags={"dagster/max_retries": "0"})
def quality_audit_job():
    quality_audit()


@job(tags={"dagster/max_retries": "0"})
def quality_repair_job():
    quality_repair()


@job(tags={"dagster/max_retries": "0"})
def benchmark_job():
    benchmark()
