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
        iceberg_memory_limit="4GB",
        iceberg_threads=2,
        direct_iceberg_only: bool = False,
        batch_size: int = 30,
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

        iceberg_db = None
        iceberg_catalog = None
        existing_iceberg_dates: set[str] = set()
        if self.iceberg_config is not None:
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
                ensure_silver_tables(
                    iceberg_db,
                    iceberg_catalog,
                    namespace=self.iceberg_namespace,
                    tables=self.contract.output_tables,
                )
                ensure_views(iceberg_db, namespace=self.iceberg_namespace)

                if skip_existing:
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
            except Exception as exc:
                if iceberg_db is not None:
                    import contextlib

                    with contextlib.suppress(Exception):
                        iceberg_db.close()
                raise RuntimeError(
                    f"Iceberg initialization failed for {self.contract.resource}; publication was requested"
                ) from exc

        batch_buffer: dict[str, list[pa.Table]] = defaultdict(list)
        buffered_entries: list[dict[str, Any]] = []

        def flush_batch() -> None:
            nonlocal batch_buffer, buffered_entries
            if not batch_buffer:
                buffered_entries.clear()
                return
            merged_tables = {}
            for t_name, t_list in batch_buffer.items():
                non_empty = [t for t in t_list if len(t) > 0]
                if non_empty:
                    merged_tables[t_name] = pa.concat_tables(non_empty)
                elif t_list:
                    merged_tables[t_name] = t_list[0]

            if merged_tables and iceberg_db is not None and iceberg_catalog is not None:
                from procurement.silver.application.warehouse import stage_partition

                staged_res = stage_partition(
                    iceberg_db,
                    iceberg_catalog,
                    namespace=self.iceberg_namespace,
                    tables=merged_tables,
                    partition_date=None,
                    release_id=None,
                    change_journal=self.change_journal,
                )
                for b_entry in buffered_entries:
                    b_entry["iceberg"] = staged_res
            batch_buffer.clear()
            buffered_entries.clear()
            save_report()

        try:
            day = start_date
            while day <= end_date:
                day_str = str(day)
                entry: dict[str, Any] = {"date": day_str, "status": "running"}

                is_already_staged = False
                if skip_existing:
                    if day_str in existing_iceberg_dates:
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
                    quality = validate(tables)
                    if self.contract.resource == "contract_performance" and (
                        quality["quarantined"] or quality["blocking_issues"]
                    ):
                        raise ValueError("Contract execution Silver has blocking quality issues")

                    # Validate and cast typed schemas via SQL contract
                    output = {
                        name: typed_arrow_rows(name, tables[name])
                        for name in self.contract.output_tables
                    }

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

                        if active_batch_size > 1:
                            for t_name, t_val in tables_with_data.items():
                                if isinstance(t_val, pa.Table):
                                    batch_buffer[t_name].append(t_val)
                                elif t_val:
                                    batch_buffer[t_name].append(pa.Table.from_pylist(t_val))
                            buffered_entries.append(entry)
                        else:
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

                    if active_batch_size > 1 and batch_buffer:
                        next_day = day + timedelta(days=1)
                        is_end_of_range = (day == end_date)
                        is_end_of_month = (day.month != next_day.month)
                        is_batch_full = (len(buffered_entries) >= active_batch_size)
                        if is_end_of_range or is_end_of_month or is_batch_full:
                            flush_batch()
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

            if active_batch_size > 1:
                flush_batch()

            statuses = {e["status"] for e in report["days"]}
            report["status"] = (
                "failed"
                if "failed" in statuses
                else "needs_review"
                if "needs_review" in statuses
                else "completed"
            )

            # Pin final batch snapshot per output table
            if report["status"] in ("completed", "needs_review") and iceberg_catalog is not None:
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

__all__ = ["ProcessNoticeService", "ProcessSilverService"]
