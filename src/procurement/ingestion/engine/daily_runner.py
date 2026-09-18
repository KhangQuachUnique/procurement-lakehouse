"""Day lifecycle coordinator. Page execution and DLT loading have separate owners."""

import logging
import math
import re
from datetime import UTC, date, datetime

import dlt
import s3fs

from procurement.common.dates import api_day_window, validate_page_size
from procurement.common.errors import build_error_record
from procurement.common.settings import settings
from procurement.ingestion.engine.models import DailyResult, ResourceSpec
from procurement.ingestion.engine.page_runner import PageOutcome, run_page
from procurement.ingestion.engine.pagination import (
    PaginationInvariantError,
    SearchResultLimitError,
    iter_search_pages,
)
from procurement.ingestion.engine.stats import PageStats
from procurement.models.control import DayManifest, DayStatus, PageManifest, PageStatus
from procurement.storage.bronze import DltBronzeWriter, create_bronze_destination
from procurement.storage.control import (
    commit_day_manifest,
    write_day_manifest,
    write_page_manifest,
)
from procurement.storage.errors import save_error_records

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(UTC)


def _pipeline_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")


def _attempt_pipeline_name(spec: ResourceSpec, source_date: date, run_id: str) -> str:
    return "_".join(
        (
            _pipeline_component(spec.pipeline_name),
            _pipeline_component(spec.identity.resource),
            source_date.strftime("%Y%m%d"),
            _pipeline_component(run_id),
        )
    )


def _create_pipeline(spec: ResourceSpec, source_date: date, run_id: str):
    options = {}
    if settings.DLT_PIPELINES_DIR:
        options["pipelines_dir"] = settings.DLT_PIPELINES_DIR
    return dlt.pipeline(
        pipeline_name=_attempt_pipeline_name(spec, source_date, run_id),
        destination=create_bronze_destination(source_partition_date=source_date, run_id=run_id),
        dataset_name=spec.dataset_name,
        **options,
    )


def _result(day: DayManifest) -> DailyResult:
    return {
        "status": day.status.value,
        "pages": day.completed_pages,
        "search_items": day.search_items,
        "bronze_records": day.bronze_records,
        "errors": day.error_count,
    }


def _search_error_stage(exc: Exception) -> str:
    if isinstance(exc, SearchResultLimitError):
        return "search_limit"
    if isinstance(exc, PaginationInvariantError):
        return "pagination"
    return "search_page"


def run_daily_resource(
    *,
    fs: s3fs.S3FileSystem,
    spec: ResourceSpec,
    run_id: str,
    source_date: date,
    page_size: int,
) -> DailyResult:
    """One isolated day attempt. Recovery always uses a new run_id from page zero."""
    validate_page_size(page_size)
    day = DayManifest(
        run_id=run_id,
        source=spec.identity.source,
        resource=spec.identity.resource,
        source_date=source_date,
        status=DayStatus.RUNNING,
        started_at=_now(),
    )
    # Fail before calling the source if control storage cannot create the attempt.
    write_day_manifest(fs, spec.identity, day)
    writer = DltBronzeWriter(lambda: _create_pipeline(spec, source_date, run_id))
    window_from, window_to = api_day_window(source_date)
    pages = iter_search_pages(
        spec.fetch_page,
        window_from=window_from,
        window_to=window_to,
        page_size=page_size,
        search_key=spec.search_key,
    )
    try:
        while True:
            page = PageManifest(
                run_id=run_id,
                source_date=source_date,
                page_number=day.completed_pages,
                page_size=page_size,
                status=PageStatus.RUNNING,
                started_at=_now(),
            )
            try:
                page_number, response = next(pages)
            except StopIteration:
                break
            except Exception as exc:  # noqa: BLE001 -- source extension boundary
                outcome = PageOutcome(
                    stats=PageStats(),
                    errors=[
                        build_error_record(
                            identity=spec.identity,
                            run_id=run_id,
                            stage=_search_error_stage(exc),
                            source_date=source_date,
                            page_number=page.page_number,
                            exc=exc,
                        )
                    ],
                )
            else:
                page_data = response["page"]  # pagination has already validated this envelope
                page = page.model_copy(
                    update={
                        "page_number": page_number,
                        "search_items": len(page_data["content"]),
                    }
                )
                day = day.model_copy(
                    update={
                        "expected_pages": max(
                            1, math.ceil(int(page_data["totalElements"]) / page_size)
                        ),
                    }
                )
                write_page_manifest(fs, spec.identity, page)
                outcome = run_page(
                    spec=spec,
                    writer=writer,
                    search_items=page_data["content"],
                    run_id=run_id,
                    source_date=source_date,
                    page_number=page_number,
                )

            day = day.model_copy(
                update={
                    "search_items": day.search_items + outcome.stats.search_items,
                    "bronze_records": day.bronze_records + outcome.bronze_records,
                    "error_count": day.error_count + len(outcome.errors),
                }
            )
            if outcome.errors:
                save_error_records(
                    fs=fs,
                    identity=spec.identity,
                    source_date=source_date,
                    run_id=run_id,
                    page_number=page.page_number,
                    records=outcome.errors,
                )
            write_page_manifest(
                fs,
                spec.identity,
                page.model_copy(
                    update={
                        "status": PageStatus.FAILED if outcome.errors else PageStatus.SUCCESS,
                        "bronze_records": outcome.bronze_records,
                        "error_count": len(outcome.errors),
                        "completed_at": _now(),
                    }
                ),
            )
            if outcome.errors:
                day = day.model_copy(update={"status": DayStatus.FAILED, "completed_at": _now()})
                write_day_manifest(fs, spec.identity, day)
                return _result(day)

            day = day.model_copy(update={"completed_pages": day.completed_pages + 1})
            write_day_manifest(fs, spec.identity, day)

        if day.expected_pages is None or day.completed_pages != day.expected_pages:
            raise RuntimeError(
                f"Daily page count incomplete: completed={day.completed_pages}, "
                f"expected={day.expected_pages}"
            )
    except Exception:
        logger.exception(
            "daily_run_crashed run_id=%s source_date=%s resource=%s",
            run_id,
            source_date,
            spec.identity.resource,
        )
        try:
            write_day_manifest(
                fs,
                spec.identity,
                day.model_copy(
                    update={
                        "status": DayStatus.FAILED,
                        "error_count": max(1, day.error_count),
                        "completed_at": _now(),
                    }
                ),
            )
        except Exception:
            logger.exception("failed_to_persist_terminal_day_manifest run_id=%s", run_id)
        raise

    completed = day.model_copy(update={"status": DayStatus.SUCCESS, "completed_at": _now()})
    # Outside the failure handler: a lost commit ACK must never overwrite SUCCESS with FAILED.
    commit_day_manifest(fs, spec.identity, completed)
    return _result(completed)
