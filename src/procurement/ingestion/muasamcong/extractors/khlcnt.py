import logging
from collections.abc import Iterator
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.errors import ErrorStage, classify_exception
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.metadata import calculate_content_hash, utc_now
from procurement.ingestion.engine.stats import PageStats
from procurement.storage.error_records import ErrorRecord, build_error_record

logger = logging.getLogger(__name__)
PROGRESS_INTERVAL = 10
DETAIL_EXCEPTIONS = (httpx.HTTPError, KeyError, TypeError, ValueError)


class KhlcntDetailApi(Protocol):
    def get_plan_detail(self, plan_id: str) -> dict[str, Any]: ...

    def get_bid_package_detail(self, bid_package_id: str) -> dict[str, Any]: ...


def _build_plan_record(
    *, search_item: dict[str, Any], payload: dict[str, Any], run_id: str,
    source_date: date, search_page: int,
) -> dict[str, Any]:
    return {
        "_source": "muasamcong", "_resource": "khlcnt_plan_detail",
        "_source_id": search_item["id"],
        "_source_version": search_item.get("planVersion"), "_run_id": run_id,
        "_source_date": source_date.isoformat(),
        "_ingested_at": utc_now().isoformat(), "_search_page": search_page,
        "_content_hash": calculate_content_hash(payload), "payload": payload,
    }


def _build_bid_package_record(
    *, package: dict[str, Any], payload: dict[str, Any], run_id: str,
    source_date: date, search_page: int,
) -> dict[str, Any]:
    return {
        "_source": "muasamcong", "_resource": "khlcnt_bid_package_detail",
        "_source_id": package["id"], "_parent_source_id": package.get("idPlan"),
        "_source_version": payload.get("planVersion"), "_run_id": run_id,
        "_source_date": source_date.isoformat(),
        "_ingested_at": utc_now().isoformat(), "_search_page": search_page,
        "_content_hash": calculate_content_hash(payload), "payload": payload,
    }


def _record_detail_error(
    *, identity: ResourceIdentity, errors: list[ErrorRecord],
    exc: Exception, stage: ErrorStage,
    run_id: str, source_date: date, search_page: int,
    source_id: str | None, parent_source_id: str | None,
    source_version: str | None, retry_input: dict[str, Any],
) -> None:
    classification = classify_exception(exc)
    errors.append(build_error_record(
        identity=identity, run_id=run_id, stage=stage, source_date=source_date,
        search_page=search_page, exc=exc, classification=classification,
        retry_input=retry_input, source_id=source_id,
        parent_source_id=parent_source_id, source_version=source_version,
    ))
    logger.error(
        "detail_fetch_failed run_id=%s source_date=%s page=%s stage=%s "
        "source_id=%s parent_source_id=%s error_code=%s retryable=%s",
        run_id, source_date, search_page, stage.value, source_id,
        parent_source_id, classification.code.value, classification.retryable,
    )


def iter_khlcnt_records(
    client: KhlcntDetailApi, *, identity: ResourceIdentity,
    search_items: list[dict[str, Any]],
    run_id: str, source_date: date, search_page: int,
    errors: list[ErrorRecord], stats: PageStats,
) -> Iterator[dict[str, Any]]:
    """Yield Bronze records and isolate individual detail failures."""

    total_plans = len(search_items)
    processed_packages = 0
    for plan_index, search_item in enumerate(search_items, start=1):
        plan_id = search_item.get("id")
        if not plan_id:
            stats.error("plan")
            _record_detail_error(
                identity=identity, errors=errors, exc=KeyError("id"),
                stage=ErrorStage.PLAN_DETAIL,
                run_id=run_id, source_date=source_date, search_page=search_page,
                source_id=None, parent_source_id=None,
                source_version=search_item.get("planVersion"), retry_input={},
            )
            continue
        try:
            plan_detail = client.get_plan_detail(plan_id)
        except DETAIL_EXCEPTIONS as exc:
            stats.error("plan")
            _record_detail_error(
                identity=identity, errors=errors, exc=exc,
                stage=ErrorStage.PLAN_DETAIL,
                run_id=run_id, source_date=source_date, search_page=search_page,
                source_id=plan_id, parent_source_id=None,
                source_version=search_item.get("planVersion"),
                retry_input={"plan_id": plan_id},
            )
            continue

        yield _build_plan_record(
            search_item=search_item, payload=plan_detail, run_id=run_id,
            source_date=source_date, search_page=search_page,
        )
        stats.record("plan")
        packages = plan_detail.get("bidpPlanDetailToProjectList") or []
        for package in packages:
            package_id = package.get("id")
            if not package_id:
                stats.error("bid_package")
                _record_detail_error(
                    identity=identity, errors=errors, exc=KeyError("id"),
                    stage=ErrorStage.BID_PACKAGE_DETAIL, run_id=run_id,
                    source_date=source_date, search_page=search_page,
                    source_id=None, parent_source_id=plan_id,
                    source_version=None, retry_input={"plan_id": plan_id},
                )
                continue
            try:
                package_detail = client.get_bid_package_detail(package_id)
            except DETAIL_EXCEPTIONS as exc:
                stats.error("bid_package")
                _record_detail_error(
                    identity=identity, errors=errors, exc=exc,
                    stage=ErrorStage.BID_PACKAGE_DETAIL, run_id=run_id,
                    source_date=source_date, search_page=search_page,
                    source_id=package_id, parent_source_id=plan_id,
                    source_version=None,
                    retry_input={"plan_id": plan_id, "package_id": package_id},
                )
                continue
            yield _build_bid_package_record(
                package=package, payload=package_detail, run_id=run_id,
                source_date=source_date, search_page=search_page,
            )
            processed_packages += 1
            stats.record("bid_package")

        if plan_index % PROGRESS_INTERVAL == 0 or plan_index == total_plans:
            logger.info(
                "detail_progress run_id=%s source_date=%s page=%s "
                "plans=%s/%s packages=%s errors=%s",
                run_id, source_date, search_page, plan_index, total_plans,
                processed_packages, len(errors),
            )
