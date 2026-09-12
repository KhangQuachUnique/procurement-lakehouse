from collections.abc import Iterator
from datetime import date
from typing import Any

from procurement.common.errors import ErrorStage
from procurement.storage.errors import ErrorRecord
from procurement.ingestion.sources.muasamcong.khlcnt.extractor import (
    KhlcntDetailApi,
    build_bid_package_record,
    build_plan_record,
)


def retry_khlcnt_error(
    client: KhlcntDetailApi,
    *,
    error: ErrorRecord,
    retry_run_id: str,
    source_date: date,
) -> Iterator[dict[str, Any]]:
    stage = ErrorStage(str(error["stage"]))
    error_id = str(error["error_id"])
    search_page = int(error.get("search_page", -1))
    retry_input = error.get("retry_input") or {}

    if stage is ErrorStage.PLAN_DETAIL:
        plan_id = str(retry_input.get("plan_id") or error.get("source_id") or "")
        if not plan_id:
            raise ValueError("Retryable plan detail error is missing plan_id")
        payload = client.get_plan_detail(plan_id)
        yield build_plan_record(
            source_id=plan_id,
            source_version=error.get("source_version"),
            payload=payload,
            run_id=retry_run_id,
            source_date=source_date,
            search_page=search_page,
            recovered_from_error_id=error_id,
        )
        return

    if stage is ErrorStage.BID_PACKAGE_DETAIL:
        package_id = str(retry_input.get("package_id") or error.get("source_id") or "")
        if not package_id:
            raise ValueError("Retryable bid package error is missing package_id")
        payload = client.get_bid_package_detail(package_id)
        yield build_bid_package_record(
            source_id=package_id,
            parent_source_id=error.get("parent_source_id") or retry_input.get("plan_id"),
            source_version=payload.get("planVersion") or error.get("source_version"),
            payload=payload,
            run_id=retry_run_id,
            source_date=source_date,
            search_page=search_page,
            recovered_from_error_id=error_id,
        )
        return

    raise ValueError(f"Error stage {stage.value} does not support record-level retry")
