import logging
from collections.abc import Iterator
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.metadata import calculate_content_hash, utc_now
from procurement.ingestion.engine.models import BronzeItem
from procurement.ingestion.engine.stats import PageStats
from procurement.models.bronze import BronzeRecord
from procurement.models.errors import ErrorRecord
from procurement.storage.errors import build_error_record

logger = logging.getLogger(__name__)

PROGRESS_INTERVAL = 10
DETAIL_EXCEPTIONS = (httpx.HTTPError, KeyError, TypeError, ValueError)
CONTRACTOR_RESULT_TABLE = "contractor_result_detail"
RESULT_DETAIL_STAGE = "result_detail"


class ContractorResultDetailApi(Protocol):
    def get_result_detail(self, result_id: str) -> dict[str, Any]: ...


def _as_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def extract_contractor_result_identity(
    payload: dict[str, Any],
    search_item: dict[str, Any],
) -> tuple[str, str | None]:
    result = payload.get("bideContractorInputResultDTO")
    if not isinstance(result, dict):
        raise KeyError("bideContractorInputResultDTO")

    source_id = (
        _as_string(result.get("notifyNo"))
        or _as_string(search_item.get("notifyNo"))
        or _as_string(search_item.get("inputResultId"))
    )
    if source_id is None:
        raise KeyError("notifyNo")

    source_version = _as_string(result.get("resultVersion"))
    return source_id, source_version


def build_contractor_result_record(
    *,
    source_id: str,
    source_version: str | None,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    return BronzeItem(
        table=CONTRACTOR_RESULT_TABLE,
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


def _record_detail_error(
    *,
    identity: ResourceIdentity,
    errors: list[ErrorRecord],
    exc: Exception,
    run_id: str,
    source_date: date,
    search_page: int,
    source_id: str | None,
) -> None:
    errors.append(
        build_error_record(
            identity=identity,
            run_id=run_id,
            stage=RESULT_DETAIL_STAGE,
            source_date=source_date,
            page_number=search_page,
            exc=exc,
            source_id=source_id,
        )
    )
    logger.error(
        "contractor_result_detail_failed run_id=%s source_date=%s page=%s "
        "stage=%s source_id=%s",
        run_id,
        source_date,
        search_page,
        RESULT_DETAIL_STAGE,
        source_id,
    )


def iter_contractor_result_records(
    client: ContractorResultDetailApi,
    *,
    identity: ResourceIdentity,
    search_items: list[dict[str, Any]],
    run_id: str,
    source_date: date,
    search_page: int,
    errors: list[ErrorRecord],
    stats: PageStats,
) -> Iterator[BronzeItem]:
    total_results = len(search_items)
    processed_results = 0

    for result_index, search_item in enumerate(search_items, start=1):
        input_result_id = _as_string(search_item.get("inputResultId"))
        source_hint = _as_string(search_item.get("notifyNo")) or input_result_id

        if input_result_id is None:
            stats.error("contractor_result")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=KeyError("inputResultId"),
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=source_hint,
            )
            continue

        try:
            detail = client.get_result_detail(input_result_id)
            source_id, source_version = extract_contractor_result_identity(
                detail,
                search_item,
            )
        except DETAIL_EXCEPTIONS as exc:
            stats.error("contractor_result")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=exc,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=source_hint,
            )
            continue

        yield build_contractor_result_record(
            source_id=source_id,
            source_version=source_version,
            payload=detail,
            run_id=run_id,
            source_date=source_date,
        )
        processed_results += 1
        stats.record("contractor_result")

        if result_index % PROGRESS_INTERVAL == 0 or result_index == total_results:
            logger.info(
                "contractor_result_progress run_id=%s source_date=%s page=%s "
                "results=%s/%s errors=%s",
                run_id,
                source_date,
                search_page,
                processed_results,
                total_results,
                len(errors),
            )
