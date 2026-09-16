import logging
from collections.abc import Iterator
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.ingestion.engine.concurrency import ordered_parallel_map
from procurement.ingestion.engine.metadata import calculate_content_hash, utc_now
from procurement.ingestion.engine.models import BronzeItem
from procurement.ingestion.engine.stats import PageStats
from procurement.models.bronze import BronzeRecord
from procurement.models.errors import ErrorRecord
from procurement.storage.errors import build_error_record

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


def build_plan_record(
    *,
    source_id: str,
    source_version: str | None,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    return BronzeItem(
        table=PLAN_TABLE,
        record=BronzeRecord(
            source_id=source_id,
            source_version=source_version,
            run_id=run_id,
            source_date=source_date,
            ingested_at=utc_now(),
            content_hash=calculate_content_hash(payload),
            payload=payload,
        ),
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
    return BronzeItem(
        table=BID_PACKAGE_TABLE,
        record=BronzeRecord(
            source_id=source_id,
            source_version=None,
            run_id=run_id,
            source_date=source_date,
            ingested_at=utc_now(),
            content_hash=calculate_content_hash(payload),
            payload=payload,
        ),
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
) -> Iterator[BronzeItem]:
    total_plans = len(search_items)
    processed_packages = 0

    def fetch_plan(
        search_item: dict[str, Any],
    ) -> tuple[
        list[BronzeItem],
        list[tuple[str, str | None, Exception]],
        bool,
        int,
    ]:
        plan_id_raw = search_item.get("id")
        source_version_raw = search_item.get("planVersion")
        if not plan_id_raw:
            return [], [(PLAN_DETAIL_STAGE, None, KeyError("id"))], False, 0

        plan_id = str(plan_id_raw)
        source_version = None if source_version_raw is None else str(source_version_raw)
        try:
            plan_detail = client.get_plan_detail(plan_id)
        except DETAIL_EXCEPTIONS as exc:
            return [], [(PLAN_DETAIL_STAGE, plan_id, exc)], False, 0

        records = [
            build_plan_record(
                source_id=plan_id,
                source_version=source_version,
                payload=plan_detail,
                run_id=run_id,
                source_date=source_date,
            )
        ]
        fetch_errors: list[tuple[str, str | None, Exception]] = []
        package_records = 0

        # Keep packages inside one plan sequential. Parallelism is only across plans,
        # preventing nested worker pools from multiplying request concurrency.
        for package in plan_detail.get("bidpPlanDetailToProjectList") or []:
            package_id_raw = package.get("id")
            if not package_id_raw:
                fetch_errors.append((BID_PACKAGE_DETAIL_STAGE, None, KeyError("id")))
                continue

            package_id = str(package_id_raw)
            try:
                package_detail = client.get_bid_package_detail(package_id)
            except DETAIL_EXCEPTIONS as exc:
                fetch_errors.append((BID_PACKAGE_DETAIL_STAGE, package_id, exc))
                continue

            records.append(
                build_bid_package_record(
                    source_id=package_id,
                    payload=package_detail,
                    run_id=run_id,
                    source_date=source_date,
                )
            )
            package_records += 1

        return records, fetch_errors, True, package_records

    results = ordered_parallel_map(
        fetch_plan,
        search_items,
        max_workers=settings.MUASAMCONG_DETAIL_WORKERS,
    )

    for plan_index, (records, fetch_errors, plan_ok, package_records) in enumerate(
        results,
        start=1,
    ):
        if plan_ok:
            stats.record("plan")
        for stage, source_id, exc in fetch_errors:
            stats.error("plan" if stage == PLAN_DETAIL_STAGE else "bid_package")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=exc,
                stage=stage,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=source_id,
            )

        for _ in range(package_records):
            stats.record("bid_package")
        processed_packages += package_records

        yield from records

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
