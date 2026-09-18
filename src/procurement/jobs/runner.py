"""Execute one resource/day attempt; the ingestion flow owns range scheduling."""

from datetime import date
from importlib import import_module

from procurement.common.catalog import DEFAULT_SOURCE, get_resource
from procurement.common.dates import today_vn, validate_closed_range, validate_page_size
from procurement.common.settings import settings
from procurement.ingestion.batch_runner import run_batch_range
from procurement.ingestion.engine.daily_runner import run_daily_resource
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.storage.object_store import create_s3_filesystem


def run_resource_day(
    resource: str,
    source_date: date,
    *,
    page_size: int = 50,
    source: str = DEFAULT_SOURCE,
    request_budget: RequestBudget | None = None,
    khlcnt_package_workers: int | None = None,
) -> str:
    validate_closed_range(source_date, source_date, today=today_vn())
    validate_page_size(page_size)
    definition = get_resource(resource, source=source)
    if not settings.MUASAMCONG_TOKEN:
        raise RuntimeError("MUASAMCONG_TOKEN is missing")

    module_name, factory_name = definition.spec_factory.split(":")
    factory = getattr(import_module(module_name), factory_name)
    fs = create_s3_filesystem()
    budget = request_budget if request_budget is not None else RequestBudget(
        settings.MUASAMCONG_MAX_INFLIGHT
    )
    with MuasamcongClient(
        token=settings.MUASAMCONG_TOKEN,
        max_attempts=settings.MUASAMCONG_MAX_ATTEMPTS,
        max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
        request_budget=budget,
    ) as client:
        spec = (
            factory(client, package_workers=khlcnt_package_workers)
            if resource == "khlcnt" else factory(client)
        )
        if spec.identity != definition.identity:
            raise ValueError("Resource factory identity does not match the catalog")
        return run_batch_range(
            source_date,
            source_date,
            fs=fs,
            identity=spec.identity,
            heartbeat=True,
            run_day=lambda run_id, source_date: run_daily_resource(
                fs=fs,
                spec=spec,
                run_id=run_id,
                source_date=source_date,
                page_size=page_size,
                check_cancelled=budget.check_cancelled,
            ),
        )
