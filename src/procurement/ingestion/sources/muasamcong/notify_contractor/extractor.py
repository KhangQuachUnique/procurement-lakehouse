import logging
from collections.abc import Iterator
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.errors import build_error_record
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import BronzeItem
from procurement.ingestion.engine.records import build_bronze_item
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.notify_contractor.router import (
    DetailKind,
    UnsupportedNotifyWorkflowError,
    resolve_detail_kind,
)
from procurement.models.errors import ErrorRecord

logger = logging.getLogger(__name__)

PROGRESS_INTERVAL = 10
DETAIL_EXCEPTIONS = (httpx.HTTPError, KeyError, TypeError, ValueError)

STANDARD_TABLE = "notify_contractor_standard_detail"
REOFFER_TABLE = "notify_contractor_reoffer_detail"

ROUTING_STAGE = "detail_routing"
STANDARD_DETAIL_STAGE = "standard_detail"
REOFFER_DETAIL_STAGE = "reoffer_detail"


class NotifyContractorDetailApi(Protocol):
    def get_standard_detail(self, notice_id: str) -> dict[str, Any]: ...
    def get_reoffer_detail(self, notice_id: str) -> dict[str, Any]: ...


def _as_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _standard_identity(
    payload: dict[str, Any],
    search_item: dict[str, Any],
) -> tuple[str, str | None]:
    notification = payload.get("bidoNotifyContractorM")
    if not isinstance(notification, dict):
        bid_response = payload.get("bidNoContractorResponse")
        if isinstance(bid_response, dict):
            candidate = bid_response.get("bidNotification")
            notification = candidate if isinstance(candidate, dict) else {}
        else:
            notification = {}

    source_id = (
        _as_string(notification.get("notifyNo"))
        or _as_string(search_item.get("notifyNo"))
        or _as_string(search_item.get("id"))
    )
    if source_id is None:
        raise KeyError("notifyNo")

    source_version = _as_string(notification.get("notifyVersion")) or _as_string(
        search_item.get("notifyVersion")
    )
    return source_id, source_version


def _reoffer_identity(
    payload: dict[str, Any],
    search_item: dict[str, Any],
) -> tuple[str, str | None]:
    source_id = (
        _as_string(payload.get("notifyNo"))
        or _as_string(payload.get("reofferNo"))
        or _as_string(search_item.get("notifyNo"))
        or _as_string(search_item.get("id"))
    )
    if source_id is None:
        raise KeyError("notifyNo")

    source_version = (
        _as_string(payload.get("notifyVersion"))
        or _as_string(payload.get("reofferVersion"))
        or _as_string(search_item.get("notifyVersion"))
    )
    return source_id, source_version


def _build_record(
    *,
    table: str,
    source_id: str,
    source_version: str | None,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    return build_bronze_item(
        table=table,
        source_id=source_id,
        source_version=source_version,
        run_id=run_id,
        source_date=source_date,
        payload=payload,
    )


def _record_error(
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
        "notify_detail_failed run_id=%s source_date=%s page=%s stage=%s source_id=%s",
        run_id,
        source_date,
        search_page,
        stage,
        source_id,
    )


def iter_notify_contractor_records(
    client: NotifyContractorDetailApi,
    *,
    identity: ResourceIdentity,
    search_items: list[dict[str, Any]],
    run_id: str,
    source_date: date,
    search_page: int,
    errors: list[ErrorRecord],
    stats: PageStats,
) -> Iterator[BronzeItem]:
    total = len(search_items)
    standard_count = 0
    reoffer_count = 0

    for item_index, search_item in enumerate(search_items, start=1):
        notice_id = _as_string(search_item.get("id"))
        source_hint = _as_string(search_item.get("notifyNo")) or notice_id

        if notice_id is None:
            stats.error("routing")
            _record_error(
                identity=identity,
                errors=errors,
                exc=KeyError("id"),
                stage=ROUTING_STAGE,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=source_hint,
            )
            continue

        try:
            detail_kind = resolve_detail_kind(_as_string(search_item.get("stepCode")))
        except UnsupportedNotifyWorkflowError as exc:
            stats.error("routing")
            _record_error(
                identity=identity,
                errors=errors,
                exc=exc,
                stage=ROUTING_STAGE,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=source_hint,
            )
            continue

        if detail_kind is DetailKind.STANDARD:
            stage = STANDARD_DETAIL_STAGE
            table = STANDARD_TABLE
            try:
                payload = client.get_standard_detail(notice_id)
                source_id, source_version = _standard_identity(payload, search_item)
            except DETAIL_EXCEPTIONS as exc:
                stats.error("standard")
                _record_error(
                    identity=identity,
                    errors=errors,
                    exc=exc,
                    stage=stage,
                    run_id=run_id,
                    source_date=source_date,
                    search_page=search_page,
                    source_id=source_hint,
                )
                continue
            standard_count += 1
            stats.record("standard")
        else:
            stage = REOFFER_DETAIL_STAGE
            table = REOFFER_TABLE
            try:
                payload = client.get_reoffer_detail(notice_id)
                source_id, source_version = _reoffer_identity(payload, search_item)
            except DETAIL_EXCEPTIONS as exc:
                stats.error("reoffer")
                _record_error(
                    identity=identity,
                    errors=errors,
                    exc=exc,
                    stage=stage,
                    run_id=run_id,
                    source_date=source_date,
                    search_page=search_page,
                    source_id=source_hint,
                )
                continue
            reoffer_count += 1
            stats.record("reoffer")

        yield _build_record(
            table=table,
            source_id=source_id,
            source_version=source_version,
            payload=payload,
            run_id=run_id,
            source_date=source_date,
        )

        if item_index % PROGRESS_INTERVAL == 0 or item_index == total:
            logger.info(
                "notify_detail_progress run_id=%s source_date=%s page=%s "
                "items=%s/%s standard=%s reoffer=%s errors=%s",
                run_id,
                source_date,
                search_page,
                item_index,
                total,
                standard_count,
                reoffer_count,
                len(errors),
            )
