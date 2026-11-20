"""Application Service implementing ProcessSilverUseCase.

Orchestrates Silver candidate builds via BronzeRepositoryPort and
SilverCandidateRepositoryPort in accordance with Hexagonal Architecture,
and optionally stages verified Iceberg Format v2 tables.
"""

from collections import defaultdict
from collections.abc import Callable
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pyarrow as pa

from procurement.silver.application.observations import transform
from procurement.silver.application.resources import get_silver_resource
from procurement.silver.application.revisions import assemble, validate
from procurement.silver.application.warehouse import typed_arrow_rows
from procurement.silver.ports.inbound.process_silver import ProcessSilverUseCase
from procurement.silver.ports.outbound.bronze_repository import BronzeRepositoryPort
from procurement.silver.ports.outbound.silver_repository import SilverCandidateRepositoryPort

if TYPE_CHECKING:
    from procurement.storage.iceberg import IcebergConfig


class SharedIcebergBatchBuffer:
    """Thread-safe byte-bounded accumulator that flushes Arrow batches across 1 shared DuckDB connection."""

    def __init__(
        self,
        iceberg_db: Any,
        iceberg_catalog: Any,
        namespace: str,
        *,
        existing_dates: set[str] | None = None,
        change_journal: Any = None,
        max_buffer_bytes: int = 512 * 1024 * 1024,
        max_days: int = 0,
    ) -> None:
        import threading

        self.iceberg_db = iceberg_db
        self.iceberg_catalog = iceberg_catalog
        self.namespace = namespace
        self.existing_dates: set[str] = existing_dates if existing_dates is not None else set()
        self.change_journal = change_journal
        self.max_buffer_bytes = max(1, max_buffer_bytes)
        self.max_days = max_days
        self._lock = threading.Lock()
        self.batch_buffer: dict[str, list[pa.Table]] = defaultdict(list)
        self.buffered_entries: list[dict[str, Any]] = []
        self.current_bytes: int = 0

    def has_date(self, day_str: str) -> bool:
        with self._lock:
            return day_str in self.existing_dates

    def add_day(
        self,
        entry: dict[str, Any],
        tables_with_data: dict[str, Any],
        *,
        force_flush: bool = False,
    ) -> None:
        with self._lock:
            for t_name, t_val in tables_with_data.items():
                tbl = (
                    t_val
                    if isinstance(t_val, pa.Table)
                    else (pa.Table.from_pylist(t_val) if t_val else None)
                )
                if tbl is not None:
                    self.batch_buffer[t_name].append(tbl)
                    self.current_bytes += tbl.nbytes
            self.buffered_entries.append(entry)
            should_flush = (
                force_flush
                or self.current_bytes >= self.max_buffer_bytes
                or (self.max_days > 0 and len(self.buffered_entries) >= self.max_days)
            )
            if should_flush:
                self._flush_locked()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self.batch_buffer:
            self.buffered_entries.clear()
            self.current_bytes = 0
            return
        from procurement.silver.application.warehouse import stage_partition
        from procurement.silver.domain.schema import get_primary_key

        has_existing_days = any(
            str(b.get("date")) in self.existing_dates for b in self.buffered_entries
        )
        can_skip_delete = (not has_existing_days) and (self.change_journal is None)

        can_stage = self.iceberg_db is not None and self.iceberg_catalog is not None

        replaced_result_rids: set[str] | None = None
        incoming_observation_ids: set[str] | None = None
        if can_stage and ((not can_skip_delete) or self.change_journal is not None):
            replaced_result_rids = set()
            for r_tbl in ("result_revision", "award_lot", "award_contractor"):
                for tbl in self.batch_buffer.get(r_tbl, ()):
                    if "revision_id" in tbl.column_names and len(tbl) > 0:
                        replaced_result_rids.update(
                            str(v) for v in tbl["revision_id"].to_pylist() if v is not None
                        )
            incoming_observation_ids = set()
            for o_tbl in ("observation", "quality_issue", "quarantine"):
                for tbl in self.batch_buffer.get(o_tbl, ()):
                    if "observation_id" in tbl.column_names and len(tbl) > 0:
                        incoming_observation_ids.update(
                            str(v) for v in tbl["observation_id"].to_pylist() if v is not None
                        )

        staged_res: dict[str, dict[str, Any]] = {}
        for t_name in list(self.batch_buffer.keys()):
            t_list = self.batch_buffer.pop(t_name)
            non_empty = [t for t in t_list if len(t) > 0]
            first_fallback = t_list[0] if t_list else None
            del t_list

            combined: pa.Table | None = None
            if non_empty:
                combined = pa.concat_tables(non_empty) if len(non_empty) > 1 else non_empty[0]
                del non_empty
                if can_skip_delete and len(combined) > 1:
                    pk_col = get_primary_key(t_name)
                    if pk_col and pk_col in combined.column_names:
                        col_vals = combined[pk_col].to_pylist()
                        seen_idx = {v: idx for idx, v in enumerate(col_vals) if v is not None}
                        if len(seen_idx) < len(col_vals):
                            combined = combined.take(sorted(seen_idx.values()))
                        del col_vals, seen_idx
            elif first_fallback is not None:
                combined = first_fallback
            del first_fallback

            if combined is not None and can_stage:
                table_staged = stage_partition(
                    self.iceberg_db,
                    self.iceberg_catalog,
                    namespace=self.namespace,
                    tables={t_name: combined},
                    partition_date=None,
                    release_id=None,
                    change_journal=self.change_journal,
                    skip_delete=can_skip_delete,
                    ensure_views_after=False,
                    pop_tables=True,
                    replaced_result_rids=replaced_result_rids,
                    incoming_observation_ids=incoming_observation_ids,
                )
                staged_res.update(table_staged)
            del combined

        if can_stage and staged_res:
            for b_entry in self.buffered_entries:
                b_entry["iceberg"] = staged_res
                if b_entry.get("date"):
                    self.existing_dates.add(str(b_entry["date"]))

        self.batch_buffer.clear()
        self.buffered_entries.clear()
        self.current_bytes = 0
        try:
            import ctypes
            import gc

            pa.default_memory_pool().release_unused()
            gc.collect()
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:  # noqa: BLE001, S110
            pass


class ProcessSilverService(ProcessSilverUseCase):
    """Hexagonal use case implementation for Silver candidate generation and Iceberg publication."""

    def __init__(
        self,
        bronze_repo: BronzeRepositoryPort,
        silver_repo: SilverCandidateRepositoryPort,
        bucket: str,
        resource_name: str = "notice",
        iceberg_config: "IcebergConfig | None" = None,
        change_journal=None,
        readiness_evidence=False,
        iceberg_namespace="silver",
        iceberg_memory_limit="1536MB",
        iceberg_threads=2,
        direct_iceberg_only: bool = False,
        batch_size: int = 15,
        batch_mb: int = 512,
        shared_writer: SharedIcebergBatchBuffer | None = None,
    ) -> None:
        self.bronze_repo = bronze_repo
        self.silver_repo = silver_repo
        self.bucket = bucket
        self.resource_name = resource_name
        self.contract = get_silver_resource(resource_name)
        self.iceberg_config = iceberg_config
        self.change_journal = change_journal
        self.readiness_evidence = readiness_evidence
        self.direct_iceberg_only = direct_iceberg_only
        self.batch_size = max(1, batch_size)
        self.batch_mb = max(16, batch_mb)
        self.shared_writer = shared_writer
        from procurement.storage.iceberg import identifier

        self.iceberg_namespace = identifier(iceberg_namespace)
        self.iceberg_memory_limit = iceberg_memory_limit
        self.iceberg_threads = iceberg_threads

    def execute(
        self,
        start_date: date,
        end_date: date,
        *,
        family: str | None = None,
        continue_on_error: bool = False,
        skip_existing: bool = False,
        batch_size: int | None = None,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Verify all source families before applying family filter; report every day."""
        if start_date > end_date:
            raise ValueError("Start date must not be after end date")
        if self.contract.resource == "contract_performance":
            if family is not None:
                raise ValueError("contract_performance must publish both families together")
            if skip_existing and self.iceberg_config is not None:
                raise ValueError(
                    "Cannot skip contract_performance Iceberg publication by file existence alone"
                )

        active_batch_size = max(1, batch_size if batch_size is not None else self.batch_size)
        selected_tables = self.contract.select_tables(family)
        run_id = "r" + uuid4().hex
        data_prefix = f"{self.bucket}/silver"
        report_prefix = f"{self.bucket}/silver/_reports/{self.contract.resource}/{run_id}"
        storage_groups = self.contract.candidate_groups(
            data_prefix, isolate_governance=self.readiness_evidence
        )

        report: dict[str, Any] = {
            "run_id": run_id,
            "resource": self.contract.resource,
            "family": family or "all",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "status": "running",
            "mapping_version": self.contract.mapping_version,
            "days": [],
            "output_prefix": report_prefix,
            "data_prefix": data_prefix,
            "iceberg_enabled": self.iceberg_config is not None,
            "iceberg_namespace": self.iceberg_namespace,
            "candidate_layout_version": "resource_governance_v1"
            if self.readiness_evidence
            else "legacy",
        }

        def save_report() -> None:
            self.silver_repo.save_run_report(report_prefix, report)

        # Batch select all committed partitions across the date range in 1 single pass
        try:
            all_partitions = self.bronze_repo.select_committed_partitions(
                [self.contract.resource], start_date, end_date
            )
            partitions_by_date = {str(p.source_date): p for p in all_partitions}
        except Exception:  # noqa: BLE001
            partitions_by_date = {}

        owns_iceberg_conn = False
        iceberg_db = None
        iceberg_catalog = None
        active_writer: SharedIcebergBatchBuffer | None = self.shared_writer

        if active_writer is not None:
            iceberg_db = active_writer.iceberg_db
            iceberg_catalog = active_writer.iceberg_catalog
        elif self.iceberg_config is not None:
            from procurement.silver.application.warehouse import ensure_silver_tables, ensure_views
            from procurement.storage.iceberg import RestCatalog, connect, identifier

            try:
                from procurement.storage.catalog_scope import NamespaceCatalog

                raw_catalog = RestCatalog(self.iceberg_config)
                raw_catalog.create_namespace_if_not_exists(self.iceberg_namespace)
                iceberg_catalog = NamespaceCatalog(
                    raw_catalog, self.iceberg_namespace
                )
                iceberg_db = connect(
                    self.iceberg_config,
                    max_memory=self.iceberg_memory_limit,
                    threads=self.iceberg_threads,
                )
                owns_iceberg_conn = True
                ensure_silver_tables(
                    iceberg_db,
                    iceberg_catalog,
                    namespace=self.iceberg_namespace,
                    tables=self.contract.output_tables,
                )
                ensure_views(iceberg_db, namespace=self.iceberg_namespace)

                existing_iceberg_dates: set[str] = set()
                try:
                    clean_ns = identifier(self.iceberg_namespace)
                    res_tag = self.contract.resource
                    query = f"""
                        SELECT DISTINCT CAST(source_date AS VARCHAR)
                        FROM lake.{clean_ns}.observation
                        WHERE resource = '{res_tag}'
                          AND source_date BETWEEN DATE '{start_date.isoformat()}' AND DATE '{end_date.isoformat()}'
                    """
                    obs_rows = iceberg_db.execute(query).fetchall()
                    existing_iceberg_dates = {str(r[0]) for r in obs_rows if r[0]}
                except Exception:
                    existing_iceberg_dates = set()

                if active_batch_size > 1:
                    active_writer = SharedIcebergBatchBuffer(
                        iceberg_db,
                        iceberg_catalog,
                        self.iceberg_namespace,
                        existing_dates=existing_iceberg_dates,
                        change_journal=self.change_journal,
                        max_buffer_bytes=self.batch_mb * 1024 * 1024,
                        max_days=active_batch_size,
                    )
            except Exception as exc:
                if iceberg_db is not None:
                    import contextlib

                    with contextlib.suppress(Exception):
                        iceberg_db.close()
                raise RuntimeError(
                    f"Iceberg initialization failed for {self.contract.resource}; publication was requested"
                ) from exc

        try:
            day = start_date
            while day <= end_date:
                day_str = str(day)
                entry: dict[str, Any] = {"date": day_str, "status": "running"}

                is_already_staged = False
                if skip_existing:
                    if active_writer is not None and active_writer.has_date(day_str):
                        is_already_staged = True
                    elif hasattr(self.silver_repo, "partition_exists") and all(
                        self.silver_repo.partition_exists(prefix, day, names)
                        for prefix, names in storage_groups
                    ):
                        is_already_staged = True

                if is_already_staged:
                    input_counts = dict.fromkeys(dict(self.contract.families), 0)
                    entry.update(
                        status="skipped",
                        input_counts=input_counts,
                        quality={
                            "accepted": 0,
                            "quarantined": 0,
                            "issues": 0,
                            "blocking_issues": 0,
                        },
                        message="Skipped: partition already committed in lakehouse/storage",
                    )
                    report["days"].append(entry)
                    save_report()
                    if on_progress:
                        on_progress(entry)
                    day += timedelta(days=1)
                    continue

                try:
                    if day_str in partitions_by_date:
                        partition = partitions_by_date[day_str]
                    else:
                        single_parts = self.bronze_repo.select_committed_partitions(
                            [self.contract.resource], day, day
                        )
                        if len(single_parts) == 0:
                            entry.update(
                                status="skipped",
                                input_counts=dict.fromkeys(dict(self.contract.families), 0),
                                quality={
                                    "accepted": 0,
                                    "quarantined": 0,
                                    "issues": 0,
                                    "blocking_issues": 0,
                                },
                                message=f"Skipped: no committed bronze data for {day}",
                            )
                            report["days"].append(entry)
                            save_report()
                            if on_progress:
                                on_progress(entry)
                            day += timedelta(days=1)
                            continue
                        elif len(single_parts) > 1:
                            raise ValueError(f"Expected at most one committed day for {day}, got {len(single_parts)}")
                        partition = single_parts[0]
                    input_counts = dict.fromkeys(dict(self.contract.families), 0)
                    names = {table: name for name, table in self.contract.families}
                    transformed = []

                    # Convert partition object to legacy dict if needed for transform signature
                    raw_partition_dict = {
                        "resource": partition.resource,
                        "source_date": partition.source_date,
                        "run_id": partition.run_id,
                        "expected_records": partition.expected_records,
                        "manifest": partition.manifest,
                        "files": [
                            {"table": f.table, "key": f.key, "sha256": f.sha256}
                            for f in partition.files
                        ],
                    }

                    for file_dict, ordinal, record in self.bronze_repo.stream_partition_records(
                        partition, selected_tables=selected_tables
                    ):
                        input_counts[names[file_dict["table"]]] += 1
                        if file_dict["table"] in selected_tables:
                            transformed.append(
                                transform(
                                    "muasamcong", raw_partition_dict, file_dict, ordinal, record
                                )
                            )

                    tables = assemble(transformed)
                    del transformed
                    quality = validate(tables)
                    if self.contract.resource == "contract_performance" and (
                        quality["quarantined"] or quality["blocking_issues"]
                    ):
                        raise ValueError("Contract execution Silver has blocking quality issues")

                    # Validate and cast typed schemas via SQL contract, freeing Python dicts table-by-table
                    output = {}
                    for name in self.contract.output_tables:
                        rows = tables.pop(name, [])
                        output[name] = typed_arrow_rows(name, rows)
                        del rows
                    del tables

                    paths = {}
                    if not self.direct_iceberg_only:
                        for prefix, names in storage_groups:
                            paths.update(
                                self.silver_repo.write_candidate_tables(
                                    prefix,
                                    day,
                                    {name: output[name] for name in names},
                                )
                            )

                    iceberg_staged = None
                    if iceberg_db is not None and iceberg_catalog is not None:
                        tables_with_data = {
                            name: tbl for name, tbl in output.items() if len(tbl) > 0
                        }
                        if self.contract.resource == "contract_performance":
                            # Empty report days must also clear stale domain rows on refresh.
                            # Shared governance tables retain their existing nonempty-only policy.
                            for name in ("contract_revision", "contract_performance_revision"):
                                tables_with_data[name] = output[name]
                        if self.contract.resource in {"contractor_result", "result"}:
                            # Empty child tables must also be staged so stale children are deleted on rerun
                            for child_name in ("award_lot", "award_contractor"):
                                if child_name in output:
                                    tables_with_data[child_name] = output[child_name]
                        if "observation" in output and len(output["observation"]) > 0:
                            # Always stage quality_issue and quarantine (including empty) when observations are nonempty
                            for gov_name in ("quality_issue", "quarantine"):
                                if gov_name in output:
                                    tables_with_data[gov_name] = output[gov_name]
                        del output

                        if active_batch_size <= 1 or active_writer is None:
                            from procurement.silver.application.warehouse import stage_partition

                            if tables_with_data:
                                iceberg_staged = stage_partition(
                                    iceberg_db,
                                    iceberg_catalog,
                                    namespace=self.iceberg_namespace,
                                    tables=tables_with_data,
                                    partition_date=day,
                                    release_id=None,
                                    change_journal=self.change_journal,
                                )
                            else:
                                iceberg_staged = {}
                            entry["iceberg"] = iceberg_staged
                            del tables_with_data
                    else:
                        del output
                        tables_with_data = None

                    entry.update(
                        status="needs_review"
                        if quality["quarantined"] or quality["blocking_issues"]
                        else "completed",
                        input_counts=input_counts,
                        quality=quality,
                        files=paths,
                        iceberg=iceberg_staged,
                        input_run_id=partition.run_id,
                        input_files=[
                            {"table": f.table, "key": f.key, "sha256": f.sha256}
                            for f in partition.files
                        ],
                    )

                    if active_batch_size > 1 and active_writer is not None and tables_with_data is not None:
                        active_writer.add_day(entry, tables_with_data)
                        del tables_with_data
                    if self.readiness_evidence and entry["status"] == "completed":
                        if iceberg_catalog is None:
                            raise ValueError(
                                "Readiness evidence requires actual Iceberg publication"
                            )
                        from procurement.silver.application.readiness import (
                            complete_day_publication,
                        )

                        entry = complete_day_publication(
                            entry,
                            resource=self.contract.resource,
                            namespace=self.iceberg_namespace,
                            catalog=iceberg_catalog,
                        )
                except Exception as exc:  # noqa: BLE001
                    entry.update(
                        status="failed",
                        error_type=type(exc).__name__,
                        error=str(exc)
                        if isinstance(exc, (ValueError, RuntimeError))
                        else f"Processing or storage error: {exc}",
                    )

                report["days"].append(entry)
                if active_batch_size <= 1 or entry["status"] == "failed":
                    save_report()

                if on_progress:
                    on_progress(entry)

                # Release unneeded memory buffers from PyArrow pool and Python runtime
                try:
                    import gc

                    pa.default_memory_pool().release_unused()
                    if len(report["days"]) % 10 == 0:
                        gc.collect()
                except Exception:  # noqa: BLE001, S110
                    pass

                if entry["status"] == "failed" and not continue_on_error:
                    break

                day += timedelta(days=1)

            if active_batch_size > 1 and active_writer is not None and self.shared_writer is None:
                active_writer.flush()

            statuses = {e["status"] for e in report["days"]}
            report["status"] = (
                "failed"
                if "failed" in statuses
                else "needs_review"
                if "needs_review" in statuses
                else "completed"
            )

            # Pin final batch snapshot per output table
            if (
                owns_iceberg_conn
                and report["status"] in ("completed", "needs_review")
                and iceberg_catalog is not None
            ):
                for clean_name in self.contract.output_tables:
                    try:
                        table_info = iceberg_catalog.table(self.iceberg_namespace, clean_name)
                        if table_info:
                            snap_id = table_info.get("metadata", {}).get("current-snapshot-id")
                            if snap_id is not None and snap_id >= 0:
                                iceberg_catalog.pin(
                                    self.iceberg_namespace, clean_name, snap_id, run_id
                                )
                    except Exception:  # noqa: BLE001, S110 — snapshot pinning is best effort
                        pass
        finally:
            if owns_iceberg_conn:
                if iceberg_db is not None:
                    try:
                        from procurement.silver.application.warehouse import ensure_views

                        ensure_views(iceberg_db, namespace=self.iceberg_namespace)
                    except Exception:  # noqa: BLE001, S110 — views are recreated by query sessions
                        pass
                    iceberg_db.close()
                if iceberg_catalog is not None:
                    iceberg_catalog.close()

        save_report()
        return report


# Backward compatibility alias
ProcessNoticeService = ProcessSilverService

__all__ = ["ProcessNoticeService", "ProcessSilverService", "SharedIcebergBatchBuffer"]
