import logging
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.errors import build_error_record
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import BronzeItem
from procurement.ingestion.engine.records import build_bronze_item
from procurement.ingestion.engine.stats import PageStats
from procurement.models.errors import ErrorRecord

logger = logging.getLogger(__name__)
PROGRESS_INTERVAL = 10
DETAIL_EXCEPTIONS = (httpx.HTTPError, KeyError, TypeError, ValueError)
PLAN_TABLE = "khlcnt_plan_detail"
BID_PACKAGE_TABLE = "khlcnt_bid_package_detail"
PLAN_DETAIL_STAGE = "plan_detail"
BID_PACKAGE_DETAIL_STAGE = "bid_package_detail"


class KhlcntDetailApi(Protocol):
    def get_plan_detail(self, plan_id: str) -> dict[str, Any]: ...
    def get_bid_package_detail(self, bid_package_id: str) -> dict[str, Any]: ...


def _package_results(client: KhlcntDetailApi, package_ids: list[str], workers: int):
    """Only package HTTP calls run in workers; the caller owns records, errors and stats."""
    def fetch(package_id):
        try:
            return package_id, client.get_bid_package_detail(package_id), None
        except DETAIL_EXCEPTIONS as exc:
            return package_id, None, exc

    if workers == 1 or len(package_ids) <= 1:
        for package_id in package_ids:
            yield fetch(package_id)
        return

    # Bound submissions too: a large plan never creates an unbounded future queue.
    remaining = iter(package_ids)
    with ThreadPoolExecutor(
        max_workers=min(workers, len(package_ids)), thread_name_prefix="khlcnt-package"
    ) as pool:
        pending = {pool.submit(fetch, package_id) for package_id in package_ids[:workers]}
        for _ in range(min(workers, len(package_ids))):
            next(remaining)
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()
                package_id = next(remaining, None)
                if package_id is not None:
                    pending.add(pool.submit(fetch, package_id))


def build_plan_record(
    *,
    source_id: str,
    source_version: str | None,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    return build_bronze_item(
        table=PLAN_TABLE,
        source_id=source_id,
        source_version=source_version,
        run_id=run_id,
        source_date=source_date,
        payload=payload,
    )


def build_bid_package_record(
    *,
    source_id: str,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    # MuaSamCong does not expose a version belonging to the bid-package entity
    # itself here. Do not reuse the parent plan version as package version.
    return build_bronze_item(
        table=BID_PACKAGE_TABLE,
        source_id=source_id,
        source_version=None,
        run_id=run_id,
        source_date=source_date,
        payload=payload,
    )


def _record_detail_error(
    *,
    identity: ResourceIdentity,
    errors: list[ErrorRecord],
    exc: Exception,
    stage: str,
    run_id: str,
    source_date: date,
    search_page: int,
    source_id: str | None,
) -> None:
    errors.append(
        build_error_record(
            identity=identity,
            run_id=run_id,
            stage=stage,
            source_date=source_date,
            page_number=search_page,
            exc=exc,
            source_id=source_id,
        )
    )
    logger.error(
        "detail_fetch_failed run_id=%s source_date=%s page=%s stage=%s source_id=%s",
        run_id,
        source_date,
        search_page,
        stage,
        source_id,
    )


def iter_khlcnt_records(
    client: KhlcntDetailApi,
    *,
    identity: ResourceIdentity,
    search_items: list[dict[str, Any]],
    run_id: str,
    source_date: date,
    search_page: int,
    errors: list[ErrorRecord],
    stats: PageStats,
    package_workers: int = 3,
) -> Iterator[BronzeItem]:
    if package_workers < 1:
        raise ValueError("package_workers must be positive")
    total_plans = len(search_items)
    processed_packages = 0

    for plan_index, search_item in enumerate(search_items, start=1):
        plan_id = search_item.get("id")
        source_version = search_item.get("planVersion")
        if not plan_id:
            stats.error("plan")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=KeyError("id"),
                stage=PLAN_DETAIL_STAGE,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=None,
            )
            continue

        try:
            plan_detail = client.get_plan_detail(plan_id)
        except DETAIL_EXCEPTIONS as exc:
            stats.error("plan")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=exc,
                stage=PLAN_DETAIL_STAGE,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=plan_id,
            )
            continue

        yield build_plan_record(
            source_id=plan_id,
            source_version=source_version,
            payload=plan_detail,
            run_id=run_id,
            source_date=source_date,
        )
        stats.record("plan")

        package_ids = []
        for package in plan_detail.get("bidpPlanDetailToProjectList") or []:
            package_id = package.get("id")
            if not package_id:
                stats.error("bid_package")
                _record_detail_error(
                    identity=identity,
                    errors=errors,
                    exc=KeyError("id"),
                    stage=BID_PACKAGE_DETAIL_STAGE,
                    run_id=run_id,
                    source_date=source_date,
                    search_page=search_page,
                    source_id=None,
                )
                continue
            package_ids.append(package_id)

        for package_id, package_detail, exc in _package_results(
            client, package_ids, package_workers
        ):
            if exc is not None:
                stats.error("bid_package")
                _record_detail_error(
                    identity=identity,
                    errors=errors,
                    exc=exc,
                    stage=BID_PACKAGE_DETAIL_STAGE,
                    run_id=run_id,
                    source_date=source_date,
                    search_page=search_page,
                    source_id=package_id,
                )
                continue

            yield build_bid_package_record(
                source_id=package_id,
                payload=package_detail,
                run_id=run_id,
                source_date=source_date,
            )
            processed_packages += 1
            stats.record("bid_package")

        if plan_index % PROGRESS_INTERVAL == 0 or plan_index == total_plans:
            logger.info(
                "detail_progress run_id=%s source_date=%s page=%s "
                "plans=%s/%s packages=%s errors=%s",
                run_id,
                source_date,
                search_page,
                plan_index,
                total_plans,
                processed_packages,
                len(errors),
            )
