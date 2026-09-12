import logging
import math
from collections import defaultdict
from datetime import UTC, date, datetime
from time import perf_counter
from typing import Any

import dlt
import s3fs

from procurement.common.errors import (
    ErrorClassification,
    ErrorCode,
    ErrorStage,
    classify_exception,
)
from procurement.ingestion.engine.fingerprint import (
    calculate_page_fingerprint,
    calculate_query_fingerprint,
    ensure_compatible,
    ensure_page_compatible,
)
from procurement.ingestion.engine.models import DailyResult, ResourceSpec
from procurement.ingestion.engine.pagination import SearchResultLimitError, iter_search_pages
from procurement.ingestion.engine.stats import PageStats
from procurement.storage.bronze import create_bronze_destination, create_bronze_resource
from procurement.storage.checkpoints import (
    CONTROL_SCHEMA_VERSION,
    read_page_checkpoint,
    write_page_checkpoint,
)
from procurement.storage.errors import ErrorRecord, build_error_record, save_error_records
from procurement.storage.locks import acquire_daily_lock, refresh_daily_lock, release_daily_lock
from procurement.storage.manifests import (
    read_daily_success,
    write_daily_success,
    write_run_manifest,
)
from procurement.storage.raw import save_raw_search_page

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _save_page_errors(
    fs: s3fs.S3FileSystem,
    *,
    spec: ResourceSpec,
    source_date: date,
    run_id: str,
    page_number: int,
    records: list[ErrorRecord],
) -> list[str]:
    uris: list[str] = []
    by_stage: dict[ErrorStage, list[ErrorRecord]] = defaultdict(list)
    for record in records:
        by_stage[ErrorStage(record["stage"])].append(record)
    for stage, stage_records in by_stage.items():
        uris.append(
            save_error_records(
                fs=fs,
                identity=spec.identity,
                source_date=source_date,
                run_id=run_id,
                stage=stage,
                page_number=page_number,
                records=stage_records,
            )
        )
    return uris


def _fatal_error_record(
    *,
    spec: ResourceSpec,
    run_id: str,
    stage: ErrorStage,
    source_date: date,
    page_number: int,
    exc: Exception,
    classification: ErrorClassification,
    retry_input: dict[str, object],
) -> ErrorRecord:
    return build_error_record(
        identity=spec.identity,
        run_id=run_id,
        stage=stage,
        source_date=source_date,
        search_page=page_number,
        exc=exc,
        classification=classification,
        retry_input=retry_input,
    )


def _manifest(
    *,
    spec: ResourceSpec,
    run_id: str,
    source_date: date,
    status: str,
    window_from: str,
    window_to: str,
    page_size: int,
    fingerprint: str,
    started_at: str,
    completed_pages: int,
    stats: PageStats,
    errors: int,
    expected_items: int | None,
    expected_pages: int | None,
) -> dict[str, Any]:
    finished = status != "running"
    manifest: dict[str, Any] = {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "run_id": run_id,
        "source": spec.identity.source,
        "resource": spec.identity.resource,
        "source_date": source_date.isoformat(),
        "status": status,
        "crawl_complete": status in {"completed", "completed_with_errors"},
        "started_at": started_at,
        "completed_at": _now() if finished else None,
        "window_from": window_from,
        "window_to": window_to,
        "page_size": page_size,
        "query_fingerprint": fingerprint,
        "expected_search_items": expected_items,
        "expected_pages": expected_pages,
        "completed_pages": completed_pages,
        "search_items_processed": stats.search_items,
        "total_errors": errors,
        "last_completed_page": completed_pages - 1 if completed_pages else None,
    }
    manifest.update(stats.to_metadata())
    manifest["total_errors"] = errors
    return manifest


def _create_pipeline(spec: ResourceSpec, source_date: date):
    return dlt.pipeline(
        pipeline_name=spec.pipeline_name,
        destination=create_bronze_destination(source_partition_date=source_date),
        dataset_name=spec.dataset_name,
    )


def run_daily_resource(
    *,
    fs: s3fs.S3FileSystem,
    spec: ResourceSpec,
    run_id: str,
    source_date: date,
    page_size: int,
    force: bool,
) -> DailyResult:
    """Run one closed source-date partition.

    ``_SUCCESS`` means the daily crawl lifecycle completed. Record-level detail
    failures remain immutable error events and are recovered by the retry engine.
    Bronze is append-only/at-least-once, therefore ``force`` may append duplicates.
    """
    window_from = f"{source_date.isoformat()}T00:00:00.000Z"
    window_to = f"{source_date.isoformat()}T23:59:59.999Z"
    definition = spec.query_definition(
        source_date=source_date,
        window_from=window_from,
        window_to=window_to,
        page_size=page_size,
    )
    fingerprint = calculate_query_fingerprint(definition)

    success = read_daily_success(fs, spec.identity, source_date)
    if success is not None and not force:
        ensure_compatible(success, fingerprint)
        logger.info(
            "daily_batch_skipped_already_completed run_id=%s source_date=%s resource=%s",
            run_id,
            source_date,
            spec.identity.resource,
        )
        return {"status": "skipped", "pages": 0, "search_items": 0, "errors": 0}
    if success is not None and force:
        logger.warning(
            "forcing_completed_date_at_least_once run_id=%s source_date=%s resource=%s",
            run_id,
            source_date,
            spec.identity.resource,
        )

    acquire_daily_lock(fs, spec.identity, source_date, run_id)
    started_at = _now()
    started_timer = perf_counter()
    completed_pages = 0
    daily_errors = 0
    daily_stats = PageStats()
    expected_items: int | None = None
    expected_pages: int | None = None

    write_run_manifest(
        fs,
        spec.identity,
        source_date,
        run_id,
        _manifest(
            spec=spec,
            run_id=run_id,
            source_date=source_date,
            status="running",
            window_from=window_from,
            window_to=window_to,
            page_size=page_size,
            fingerprint=fingerprint,
            started_at=started_at,
            completed_pages=0,
            stats=daily_stats,
            errors=0,
            expected_items=None,
            expected_pages=None,
        ),
    )

    try:
        pipeline = _create_pipeline(spec, source_date)
        pages = iter_search_pages(
            spec.fetch_page,
            window_from=window_from,
            window_to=window_to,
            page_size=page_size,
        )

        while True:
            expected_page_number = completed_pages
            retry_page: dict[str, object] = {
                "window_from": window_from,
                "window_to": window_to,
                "page_number": expected_page_number,
                "page_size": page_size,
            }
            try:
                page_number, response = next(pages)
            except StopIteration:
                break
            except Exception as exc:
                stage = ErrorStage.SEARCH_LIMIT if isinstance(exc, SearchResultLimitError) else ErrorStage.SEARCH_PAGE
                classification = (
                    ErrorClassification(ErrorCode.SEARCH_RESULT_LIMIT_REACHED, False)
                    if stage is ErrorStage.SEARCH_LIMIT
                    else classify_exception(exc)
                )
                record = _fatal_error_record(
                    spec=spec,
                    run_id=run_id,
                    stage=stage,
                    source_date=source_date,
                    page_number=expected_page_number,
                    exc=exc,
                    classification=classification,
                    retry_input=retry_page,
                )
                _save_page_errors(
                    fs,
                    spec=spec,
                    source_date=source_date,
                    run_id=run_id,
                    page_number=expected_page_number,
                    records=[record],
                )
                daily_errors += 1
                raise

            page = response["page"]
            search_items = page["content"]
            item_count = len(search_items)
            expected_items = int(page["totalElements"])
            expected_pages = max(1, math.ceil(expected_items / page_size))
            page_fingerprint = calculate_page_fingerprint(search_items)

            checkpoint = read_page_checkpoint(fs, spec.identity, source_date, page_number)
            if checkpoint is not None and not force:
                ensure_compatible(checkpoint, fingerprint)
                ensure_page_compatible(checkpoint, page_fingerprint)
                checkpoint_stats = PageStats.from_metadata(checkpoint)
                daily_stats.merge(checkpoint_stats)
                daily_errors += checkpoint_stats.total_errors
                completed_pages += 1
                continue

            page_started = _now()
            page_timer = perf_counter()
            try:
                raw_uri = save_raw_search_page(
                    fs=fs,
                    source=spec.identity.source,
                    resource=spec.identity.resource,
                    source_date=source_date,
                    run_id=run_id,
                    page_number=page_number,
                    response=response,
                )
            except Exception as exc:
                record = _fatal_error_record(
                    spec=spec,
                    run_id=run_id,
                    stage=ErrorStage.RAW_STORAGE,
                    source_date=source_date,
                    page_number=page_number,
                    exc=exc,
                    classification=ErrorClassification(ErrorCode.OBJECT_STORAGE_WRITE_FAILED, True),
                    retry_input=retry_page,
                )
                try:
                    _save_page_errors(
                        fs,
                        spec=spec,
                        source_date=source_date,
                        run_id=run_id,
                        page_number=page_number,
                        records=[record],
                    )
                except Exception:
                    logger.exception("error_record_write_failed run_id=%s page=%s", run_id, page_number)
                daily_errors += 1
                raise

            page_errors: list[ErrorRecord] = []
            stats = PageStats(search_items=item_count)
            load_ids: list[str] = []
            if search_items:
                try:
                    records = spec.records(
                        search_items=search_items,
                        run_id=run_id,
                        source_date=source_date,
                        search_page=page_number,
                        errors=page_errors,
                        stats=stats,
                    )
                    resource = create_bronze_resource(records, name=spec.identity.resource)
                    load_info = pipeline.run(resource)
                    load_ids = list(load_info.loads_ids)
                except Exception as exc:
                    page_errors.append(
                        _fatal_error_record(
                            spec=spec,
                            run_id=run_id,
                            stage=ErrorStage.BRONZE_LOAD,
                            source_date=source_date,
                            page_number=page_number,
                            exc=exc,
                            classification=ErrorClassification(ErrorCode.BRONZE_LOAD_FAILED, True),
                            retry_input={**retry_page, "raw_search_uri": raw_uri},
                        )
                    )
                    _save_page_errors(
                        fs,
                        spec=spec,
                        source_date=source_date,
                        run_id=run_id,
                        page_number=page_number,
                        records=page_errors,
                    )
                    daily_errors += len(page_errors)
                    raise

            error_uris = (
                _save_page_errors(
                    fs,
                    spec=spec,
                    source_date=source_date,
                    run_id=run_id,
                    page_number=page_number,
                    records=page_errors,
                )
                if page_errors
                else []
            )
            status = "completed_with_errors" if page_errors else "completed"
            checkpoint_metadata: dict[str, Any] = {
                "schema_version": CONTROL_SCHEMA_VERSION,
                "run_id": run_id,
                "source": spec.identity.source,
                "resource": spec.identity.resource,
                "source_date": source_date.isoformat(),
                "status": status,
                "page_number": page_number,
                "page_size": page_size,
                "window_from": window_from,
                "window_to": window_to,
                "query_fingerprint": fingerprint,
                "search_page_fingerprint": page_fingerprint,
                "total_elements": expected_items,
                "search_items": stats.search_items,
                "raw_search_uri": raw_uri,
                "error_uris": error_uris,
                "bronze_load_ids": load_ids,
                "started_at": page_started,
                "completed_at": _now(),
                "duration_seconds": round(perf_counter() - page_timer, 3),
            }
            checkpoint_metadata.update(stats.to_metadata())
            write_page_checkpoint(fs, spec.identity, source_date, page_number, checkpoint_metadata)

            completed_pages += 1
            daily_stats.merge(stats)
            daily_errors += stats.total_errors
            refresh_daily_lock(fs, spec.identity, source_date, run_id)

        status = "completed_with_errors" if daily_errors else "completed"
        completed = _manifest(
            spec=spec,
            run_id=run_id,
            source_date=source_date,
            status=status,
            window_from=window_from,
            window_to=window_to,
            page_size=page_size,
            fingerprint=fingerprint,
            started_at=started_at,
            completed_pages=completed_pages,
            stats=daily_stats,
            errors=daily_errors,
            expected_items=expected_items,
            expected_pages=expected_pages,
        )
        completed["duration_seconds"] = round(perf_counter() - started_timer, 3)
        write_run_manifest(fs, spec.identity, source_date, run_id, completed)
        write_daily_success(fs, spec.identity, source_date, completed)
        return {
            "status": status,
            "pages": completed_pages,
            "search_items": daily_stats.search_items,
            "errors": daily_errors,
        }
    except Exception:
        failed = _manifest(
            spec=spec,
            run_id=run_id,
            source_date=source_date,
            status="failed",
            window_from=window_from,
            window_to=window_to,
            page_size=page_size,
            fingerprint=fingerprint,
            started_at=started_at,
            completed_pages=completed_pages,
            stats=daily_stats,
            errors=daily_errors,
            expected_items=expected_items,
            expected_pages=expected_pages,
        )
        failed["duration_seconds"] = round(perf_counter() - started_timer, 3)
        write_run_manifest(fs, spec.identity, source_date, run_id, failed)
        raise
    finally:
        release_daily_lock(fs, spec.identity, source_date, run_id)
