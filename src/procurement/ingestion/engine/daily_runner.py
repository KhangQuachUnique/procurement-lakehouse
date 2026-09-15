import logging
import math
import re
from collections import defaultdict
from datetime import UTC, date, datetime

import dlt
import s3fs

from procurement.ingestion.engine.models import DailyResult, ResourceSpec
from procurement.ingestion.engine.pagination import (
    PaginationInvariantError,
    SearchResultLimitError,
    iter_search_pages,
)
from procurement.ingestion.engine.stats import PageStats
from procurement.models.bronze import BronzeRecord
from procurement.models.control import DayManifest, DayStatus, PageManifest, PageStatus
from procurement.models.errors import ErrorRecord
from procurement.storage.bronze import create_bronze_destination, create_bronze_resource
from procurement.storage.control import write_day_manifest, write_page_manifest
from procurement.storage.errors import build_error_record, save_error_records

logger = logging.getLogger(__name__)

SEARCH_PAGE_STAGE = "search_page"
SEARCH_LIMIT_STAGE = "search_limit"
PAGINATION_STAGE = "pagination"
BRONZE_LOAD_STAGE = "bronze_load"
INTERNAL_STAGE = "internal"


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
    return dlt.pipeline(
        pipeline_name=_attempt_pipeline_name(spec, source_date, run_id),
        destination=create_bronze_destination(
            source_partition_date=source_date,
            run_id=run_id,
        ),
        dataset_name=spec.dataset_name,
    )


def _fatal_error_record(
    *,
    spec: ResourceSpec,
    run_id: str,
    stage: str,
    source_date: date,
    page_number: int | None,
    exc: Exception,
) -> ErrorRecord:
    return build_error_record(
        identity=spec.identity,
        run_id=run_id,
        stage=stage,
        source_date=source_date,
        page_number=page_number,
        exc=exc,
    )


def _persist_failed_page(
    *,
    fs: s3fs.S3FileSystem,
    spec: ResourceSpec,
    page: PageManifest,
    errors: list[ErrorRecord],
    bronze_records: int = 0,
) -> None:
    if errors:
        save_error_records(
            fs=fs,
            identity=spec.identity,
            source_date=page.source_date,
            run_id=page.run_id,
            page_number=page.page_number,
            records=errors,
        )
    write_page_manifest(
        fs,
        spec.identity,
        page.model_copy(
            update={
                "status": PageStatus.FAILED,
                "bronze_records": bronze_records,
                "error_count": len(errors),
                "completed_at": _now(),
            }
        ),
    )


def _failed_result(
    *,
    fs: s3fs.S3FileSystem,
    spec: ResourceSpec,
    day: DayManifest,
    completed_pages: int,
    stats: PageStats,
    bronze_records: int,
    errors: int,
    expected_pages: int | None,
) -> DailyResult:
    failed = day.model_copy(
        update={
            "status": DayStatus.FAILED,
            "expected_pages": expected_pages,
            "completed_pages": completed_pages,
            "search_items": stats.search_items,
            "bronze_records": bronze_records,
            "error_count": errors,
            "completed_at": _now(),
        }
    )
    write_day_manifest(fs, spec.identity, failed)
    return {
        "status": DayStatus.FAILED.value,
        "pages": completed_pages,
        "search_items": stats.search_items,
        "bronze_records": bronze_records,
        "errors": errors,
    }


def run_daily_resource(
    *,
    fs: s3fs.S3FileSystem,
    spec: ResourceSpec,
    run_id: str,
    source_date: date,
    page_size: int,
) -> DailyResult:
    """Ingest one source_date as one isolated attempt.

    ``(run_id, source_date)`` is the commit boundary. Any ingestion error marks the
    whole day FAILED. A later recovery is a new run_id that starts again from page 0;
    pages are never resumed or mixed across attempts.

    DLT state is isolated per attempt by a unique pipeline name. Concurrent attempts
    are allowed because Bronze and control paths are already partitioned by run_id.
    """
    window_from = f"{source_date.isoformat()}T00:00:00.000Z"
    window_to = f"{source_date.isoformat()}T23:59:59.999Z"

    day = DayManifest(
        run_id=run_id,
        source=spec.identity.source,
        resource=spec.identity.resource,
        source_date=source_date,
        status=DayStatus.RUNNING,
        started_at=_now(),
    )
    write_day_manifest(fs, spec.identity, day)

    completed_pages = 0
    daily_stats = PageStats()
    daily_errors = 0
    daily_bronze_records = 0
    expected_pages: int | None = None

    try:
        pipeline = _create_pipeline(spec, source_date, run_id)
        pages = iter_search_pages(
            spec.fetch_page,
            window_from=window_from,
            window_to=window_to,
            page_size=page_size,
        )

        while True:
            page_number = completed_pages
            page_started = _now()
            try:
                actual_page_number, response = next(pages)
            except StopIteration:
                break
            # fetch_page is a resource extension point. Any source exception must be
            # converted into a persisted failed attempt instead of escaping unrecorded.
            except Exception as exc:  # noqa: BLE001
                if isinstance(exc, SearchResultLimitError):
                    stage = SEARCH_LIMIT_STAGE
                elif isinstance(exc, PaginationInvariantError):
                    stage = PAGINATION_STAGE
                else:
                    stage = SEARCH_PAGE_STAGE
                record = _fatal_error_record(
                    spec=spec,
                    run_id=run_id,
                    stage=stage,
                    source_date=source_date,
                    page_number=page_number,
                    exc=exc,
                )
                page = PageManifest(
                    run_id=run_id,
                    source_date=source_date,
                    page_number=page_number,
                    page_size=page_size,
                    status=PageStatus.RUNNING,
                    started_at=page_started,
                )
                _persist_failed_page(
                    fs=fs,
                    spec=spec,
                    page=page,
                    errors=[record],
                )
                daily_errors += 1
                return _failed_result(
                    fs=fs,
                    spec=spec,
                    day=day,
                    completed_pages=completed_pages,
                    stats=daily_stats,
                    bronze_records=daily_bronze_records,
                    errors=daily_errors,
                    expected_pages=expected_pages,
                )

            page_number = actual_page_number
            page = PageManifest(
                run_id=run_id,
                source_date=source_date,
                page_number=page_number,
                page_size=page_size,
                status=PageStatus.RUNNING,
                started_at=page_started,
            )
            write_page_manifest(fs, spec.identity, page)

            try:
                page_data = response["page"]
                search_items = page_data["content"]
                item_count = len(search_items)
                total_elements = int(page_data["totalElements"])
                expected_pages = max(1, math.ceil(total_elements / page_size))
            except (KeyError, TypeError, ValueError) as exc:
                record = _fatal_error_record(
                    spec=spec,
                    run_id=run_id,
                    stage=SEARCH_PAGE_STAGE,
                    source_date=source_date,
                    page_number=page_number,
                    exc=exc,
                )
                _persist_failed_page(fs=fs, spec=spec, page=page, errors=[record])
                daily_errors += 1
                return _failed_result(
                    fs=fs,
                    spec=spec,
                    day=day,
                    completed_pages=completed_pages,
                    stats=daily_stats,
                    bronze_records=daily_bronze_records,
                    errors=daily_errors,
                    expected_pages=expected_pages,
                )

            page = page.model_copy(update={"search_items": item_count})
            stats = PageStats(search_items=item_count)
            page_errors: list[ErrorRecord] = []
            try:
                items = list(
                    spec.records(
                        search_items=search_items,
                        run_id=run_id,
                        source_date=source_date,
                        search_page=page_number,
                        errors=page_errors,
                        stats=stats,
                    )
                )
            # A resource extractor is also an extension point. Unknown extractor bugs
            # must fail the attempt and keep the original diagnostic information.
            except Exception as exc:  # noqa: BLE001
                page_errors.append(
                    _fatal_error_record(
                        spec=spec,
                        run_id=run_id,
                        stage=INTERNAL_STAGE,
                        source_date=source_date,
                        page_number=page_number,
                        exc=exc,
                    )
                )

            daily_stats.merge(stats)
            if page_errors:
                daily_errors += len(page_errors)
                _persist_failed_page(
                    fs=fs,
                    spec=spec,
                    page=page,
                    errors=page_errors,
                )
                return _failed_result(
                    fs=fs,
                    spec=spec,
                    day=day,
                    completed_pages=completed_pages,
                    stats=daily_stats,
                    bronze_records=daily_bronze_records,
                    errors=daily_errors,
                    expected_pages=expected_pages,
                )

            grouped: dict[str, list[BronzeRecord]] = defaultdict(list)
            for item in items:
                grouped[item.table].append(item.record)

            persisted_records = 0
            try:
                for table, records in grouped.items():
                    if not records:
                        continue
                    resource = create_bronze_resource(records, name=table)
                    pipeline.run(resource)
                    persisted_records += len(records)
            # DLT/storage may raise backend-specific exceptions. The engine boundary
            # intentionally captures all of them so the day cannot be falsely committed.
            except Exception as exc:  # noqa: BLE001
                record = _fatal_error_record(
                    spec=spec,
                    run_id=run_id,
                    stage=BRONZE_LOAD_STAGE,
                    source_date=source_date,
                    page_number=page_number,
                    exc=exc,
                )
                daily_errors += 1
                daily_bronze_records += persisted_records
                _persist_failed_page(
                    fs=fs,
                    spec=spec,
                    page=page,
                    errors=[record],
                    bronze_records=persisted_records,
                )
                return _failed_result(
                    fs=fs,
                    spec=spec,
                    day=day,
                    completed_pages=completed_pages,
                    stats=daily_stats,
                    bronze_records=daily_bronze_records,
                    errors=daily_errors,
                    expected_pages=expected_pages,
                )

            completed_pages += 1
            daily_bronze_records += persisted_records
            write_page_manifest(
                fs,
                spec.identity,
                page.model_copy(
                    update={
                        "status": PageStatus.SUCCESS,
                        "bronze_records": persisted_records,
                        "completed_at": _now(),
                    }
                ),
            )
            day = day.model_copy(
                update={
                    "expected_pages": expected_pages,
                    "completed_pages": completed_pages,
                    "search_items": daily_stats.search_items,
                    "bronze_records": daily_bronze_records,
                }
            )
            write_day_manifest(fs, spec.identity, day)

        if expected_pages is None or completed_pages != expected_pages:
            exc = RuntimeError(
                "Daily page count incomplete: "
                f"completed={completed_pages}, expected={expected_pages}"
            )
            page_number = completed_pages
            record = _fatal_error_record(
                spec=spec,
                run_id=run_id,
                stage=INTERNAL_STAGE,
                source_date=source_date,
                page_number=page_number,
                exc=exc,
            )
            page = PageManifest(
                run_id=run_id,
                source_date=source_date,
                page_number=page_number,
                page_size=page_size,
                status=PageStatus.RUNNING,
                started_at=_now(),
            )
            _persist_failed_page(fs=fs, spec=spec, page=page, errors=[record])
            daily_errors += 1
            return _failed_result(
                fs=fs,
                spec=spec,
                day=day,
                completed_pages=completed_pages,
                stats=daily_stats,
                bronze_records=daily_bronze_records,
                errors=daily_errors,
                expected_pages=expected_pages,
            )

        completed = day.model_copy(
            update={
                "status": DayStatus.SUCCESS,
                "expected_pages": expected_pages,
                "completed_pages": completed_pages,
                "search_items": daily_stats.search_items,
                "bronze_records": daily_bronze_records,
                "error_count": 0,
                "completed_at": _now(),
            }
        )
        write_day_manifest(fs, spec.identity, completed)
        return {
            "status": DayStatus.SUCCESS.value,
            "pages": completed_pages,
            "search_items": daily_stats.search_items,
            "bronze_records": daily_bronze_records,
            "errors": 0,
        }
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
                        "expected_pages": expected_pages,
                        "completed_pages": completed_pages,
                        "search_items": daily_stats.search_items,
                        "bronze_records": daily_bronze_records,
                        "error_count": max(1, daily_errors),
                        "completed_at": _now(),
                    }
                ),
            )
        except Exception:
            logger.exception("failed_to_persist_terminal_day_manifest run_id=%s", run_id)
        raise
