"""Sequential committed-Bronze to immutable Silver candidate runs; no publication.

Application Service facade delegating to the Hexagonal Silver Application Service.
"""

import sys
from collections.abc import Callable
from datetime import date
from typing import Any

from procurement.silver.adapters.driven.bronze.arrow_adapter import ArrowBronzeRepositoryAdapter
from procurement.silver.adapters.driven.storage.s3_candidate_adapter import S3SilverCandidateAdapter
from procurement.silver.application.process_silver_service import ProcessSilverService
from procurement.silver.domain.selection import (
    freeze as default_freeze,
)
from procurement.silver.domain.selection import (
    records as default_records,
)

freeze = default_freeze
records = default_records


def date_range(day: str | None, start: str | None, end: str | None) -> tuple[date, date]:
    if day and (start or end):
        raise ValueError("Use --date OR --start-date/--end-date, not both")
    if not day and not (start and end):
        raise ValueError("Supply --date or both --start-date and --end-date")
    s_val = day or start
    e_val = day or end
    if s_val is None or e_val is None:
        raise ValueError("Supply --date or both --start-date and --end-date")
    try:
        first, last = date.fromisoformat(s_val), date.fromisoformat(e_val)
    except (ValueError, TypeError) as exc:
        raise ValueError("Dates must use YYYY-MM-DD") from exc
    if first > last:
        raise ValueError("Start date must not be after end date")
    return first, last


def split_date_range_by_year(start: date, end: date) -> list[tuple[date, date]]:
    """Split an inclusive date range into year-bounded sub-ranges."""
    if start > end:
        raise ValueError("Start date must not be after end date")
    if start.year == end.year:
        return [(start, end)]

    ranges = []
    curr_start = start
    for y in range(start.year, end.year + 1):
        year_end = min(date(y, 12, 31), end)
        ranges.append((curr_start, year_end))
        curr_start = date(y + 1, 1, 1)
    return ranges


def merge_run_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple chunk reports into a unified multi-period report."""
    if not reports:
        raise ValueError("Cannot merge empty reports list")
    if len(reports) == 1:
        return reports[0]

    first_rep = reports[0]
    all_days = []
    for r in reports:
        all_days.extend(r.get("days", []))
    all_days.sort(key=lambda d: d.get("date", ""))

    statuses = {d.get("status") for d in all_days}
    if "failed" in statuses:
        overall_status = "failed"
    elif "needs_review" in statuses:
        overall_status = "needs_review"
    else:
        overall_status = "completed"

    return {
        "run_id": first_rep.get("run_id"),
        "resource": first_rep.get("resource"),
        "family": first_rep.get("family", "all"),
        "start_date": str(reports[0].get("start_date")),
        "end_date": str(reports[-1].get("end_date")),
        "status": overall_status,
        "mapping_version": first_rep.get("mapping_version"),
        "days": all_days,
        "output_prefix": first_rep.get("output_prefix"),
        "data_prefix": first_rep.get("data_prefix"),
        "iceberg_enabled": any(r.get("iceberg_enabled") for r in reports),
    }


def run_resource(
    fs,
    bucket: str,
    resource: str,
    start: date,
    end: date,
    *,
    family: str | None = None,
    continue_on_error: bool = False,
    skip_existing: bool = False,
    on_progress: Callable[[dict], None] | None = None,
    db_engine: Any = None,
    iceberg: bool = False,
    iceberg_config: Any = None,
    change_journal=None,
    readiness_evidence=False,
    iceberg_namespace="silver",
    iceberg_memory_limit="1536MB",
    iceberg_threads=2,
    direct_iceberg_only: bool = False,
    batch_size: int = 15,
    batch_mb: int = 512,
    shared_writer: Any = None,
    sync_linkage: bool = False,
) -> dict:
    """Verify all source families before applying a family filter; report every day."""
    current_module = sys.modules[__name__]
    active_freeze = getattr(current_module, "freeze", default_freeze)
    active_records = getattr(current_module, "records", default_records)

    bronze_repo = ArrowBronzeRepositoryAdapter(
        fs=fs,
        db_engine=db_engine,
        verify_files=resource == "contract_performance",
        freeze_fn=active_freeze if active_freeze is not default_freeze else None,
        records_fn=active_records if active_records is not default_records else None,
    )
    silver_repo = S3SilverCandidateAdapter(fs=fs)

    resolved_iceberg_config = None
    if iceberg:
        from procurement.common.settings import settings

        resolved_iceberg_config = iceberg_config or settings.iceberg_config()
    elif iceberg_config is not None:
        resolved_iceberg_config = iceberg_config

    if resolved_iceberg_config is not None and change_journal is None:
        from procurement.common.settings import settings

        change_journal = settings.SILVER_CHANGE_JOURNAL_ROOT

    use_case = ProcessSilverService(
        bronze_repo=bronze_repo,
        silver_repo=silver_repo,
        bucket=bucket,
        resource_name=resource,
        iceberg_config=resolved_iceberg_config,
        change_journal=change_journal,
        readiness_evidence=readiness_evidence,
        iceberg_namespace=iceberg_namespace,
        iceberg_memory_limit=iceberg_memory_limit,
        iceberg_threads=iceberg_threads,
        direct_iceberg_only=direct_iceberg_only,
        batch_size=batch_size,
        batch_mb=batch_mb,
        shared_writer=shared_writer,
    )

    result = use_case.execute(
        start_date=start,
        end_date=end,
        family=family,
        continue_on_error=continue_on_error,
        skip_existing=skip_existing,
        batch_size=batch_size,
        on_progress=on_progress,
    )

    if sync_linkage and resolved_iceberg_config is not None:
        from procurement.silver.application.linkage_index import sync_silver_linkage_index
        from procurement.storage.iceberg import connect, identifier

        clean_ns = identifier(iceberg_namespace)
        with connect(
            resolved_iceberg_config,
            max_memory=iceberg_memory_limit,
            threads=iceberg_threads,
        ) as conn:
            result["linkage_index"] = sync_silver_linkage_index(
                conn,
                source_schema=f"lake.{clean_ns}",
                target_table=f"lake.{clean_ns}.linkage_index",
                start_date=start.isoformat(),
                end_date=end.isoformat(),
            )

    return result


from contextlib import contextmanager


@contextmanager
def open_shared_silver_session(
    resource: str,
    namespace: str,
    start: date,
    end: date,
    *,
    iceberg: bool = True,
    batch_mb: int = 512,
):
    """Manage a single shared PostgreSQL engine, DuckDB Iceberg connection, and byte-bounded buffer across parallel workers."""
    import contextlib

    shared_db = None
    shared_cat = None
    shared_writer = None
    shared_pg_engine = None

    with contextlib.suppress(Exception):
        from procurement.infrastructure.database import create_application_engine

        shared_pg_engine = create_application_engine()

    if iceberg:
        from procurement.common.settings import settings as app_settings
        from procurement.silver.application.process_silver_service import SharedIcebergBatchBuffer
        from procurement.silver.application.resources import get_silver_resource
        from procurement.silver.application.warehouse import ensure_silver_tables, ensure_views
        from procurement.storage.catalog_scope import NamespaceCatalog
        from procurement.storage.iceberg import RestCatalog, connect, identifier

        try:
            cfg = app_settings.iceberg_config()
            clean_ns = identifier(namespace)
            raw_cat = RestCatalog(cfg)
            raw_cat.create_namespace_if_not_exists(clean_ns)
            shared_cat = NamespaceCatalog(raw_cat, clean_ns)
            shared_db = connect(cfg, max_memory="2GB", threads=2)
            contract = get_silver_resource(resource)
            ensure_silver_tables(
                shared_db, shared_cat, namespace=clean_ns, tables=contract.output_tables
            )
            ensure_views(shared_db, namespace=clean_ns)

            existing_dates: set[str] = set()
            with contextlib.suppress(Exception):
                obs_rows = shared_db.execute(
                    f"SELECT DISTINCT CAST(source_date AS VARCHAR) "
                    f"FROM lake.{clean_ns}.observation "
                    f"WHERE resource = '{contract.resource}' "
                    f"AND source_date BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}'"
                ).fetchall()
                existing_dates = {str(r[0]) for r in obs_rows if r[0]}

            shared_writer = SharedIcebergBatchBuffer(
                shared_db,
                shared_cat,
                clean_ns,
                existing_dates=existing_dates,
                change_journal=app_settings.SILVER_CHANGE_JOURNAL_ROOT,
                max_buffer_bytes=batch_mb * 1024 * 1024,
                max_days=0,
            )
        except Exception:
            if shared_db is not None:
                with contextlib.suppress(Exception):
                    shared_db.close()
                shared_db = None
            if shared_cat is not None:
                with contextlib.suppress(Exception):
                    shared_cat.close()
                shared_cat = None
            shared_writer = None

    try:
        yield shared_pg_engine, shared_writer
        if shared_writer is not None:
            shared_writer.flush()
    finally:
        if shared_db is not None:
            with contextlib.suppress(Exception):
                from procurement.silver.application.warehouse import ensure_views

                ensure_views(shared_db, namespace=namespace)
            with contextlib.suppress(Exception):
                shared_db.close()
        if shared_cat is not None:
            with contextlib.suppress(Exception):
                shared_cat.close()
        if shared_pg_engine is not None:
            with contextlib.suppress(Exception):
                shared_pg_engine.dispose()


__all__ = [
    "date_range",
    "freeze",
    "merge_run_reports",
    "open_shared_silver_session",
    "records",
    "run_resource",
    "split_date_range_by_year",
]
