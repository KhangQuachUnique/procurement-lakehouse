"""Application Service implementing ProcessSilverUseCase.

Orchestrates Silver candidate builds via BronzeRepositoryPort and
SilverCandidateRepositoryPort in accordance with Hexagonal Architecture,
and optionally stages verified Iceberg Format v2 tables.
"""

from collections.abc import Callable
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

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
    ) -> None:
        self.bronze_repo = bronze_repo
        self.silver_repo = silver_repo
        self.bucket = bucket
        self.resource_name = resource_name
        self.contract = get_silver_resource(resource_name)
        self.iceberg_config = iceberg_config

    def execute(
        self,
        start_date: date,
        end_date: date,
        *,
        family: str | None = None,
        continue_on_error: bool = False,
        skip_existing: bool = False,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Verify all source families before applying family filter; report every day."""
        if start_date > end_date:
            raise ValueError("Start date must not be after end date")

        selected_tables = self.contract.select_tables(family)
        run_id = "r" + uuid4().hex
        data_prefix = f"{self.bucket}/silver"
        report_prefix = f"{self.bucket}/silver/_reports/{self.contract.resource}/{run_id}"

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
        }

        def save_report() -> None:
            self.silver_repo.save_run_report(report_prefix, report)

        # Batch select all committed partitions across the date range in 1 single pass
        try:
            all_partitions = self.bronze_repo.select_committed_partitions(
                [self.contract.resource], start_date, end_date
            )
            partitions_by_date = {p.source_date: p for p in all_partitions}
        except Exception:  # noqa: BLE001
            partitions_by_date = {}

        iceberg_db = None
        iceberg_catalog = None
        if self.iceberg_config is not None:
            from procurement.silver.application.warehouse import ensure_views
            from procurement.storage.iceberg import RestCatalog, connect

            try:
                iceberg_db = connect(self.iceberg_config)
                iceberg_catalog = RestCatalog(self.iceberg_config)
                ensure_views(iceberg_db, namespace="silver")
            except Exception:
                pass

        try:
            day = start_date
            while day <= end_date:
                entry: dict[str, Any] = {"date": str(day), "status": "running"}
                if (
                    skip_existing
                    and hasattr(self.silver_repo, "partition_exists")
                    and self.silver_repo.partition_exists(
                        data_prefix, day, self.contract.output_tables
                    )
                ):
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
                        message="Skipped: candidate already exists in storage",
                    )
                    report["days"].append(entry)
                    save_report()
                    if on_progress:
                        on_progress(entry)
                    day += timedelta(days=1)
                    continue

                try:
                    day_str = str(day)
                    if day_str in partitions_by_date:
                        partition = partitions_by_date[day_str]
                    else:
                        single_parts = self.bronze_repo.select_committed_partitions(
                            [self.contract.resource], day, day
                        )
                        if len(single_parts) != 1:
                            raise ValueError(f"Expected exactly one committed day for {day}")
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
                                transform("muasamcong", raw_partition_dict, file_dict, ordinal, record)
                            )

                    tables = assemble(transformed)
                    quality = validate(tables)

                    # Validate and cast typed schemas via SQL contract
                    output = {
                        name: typed_arrow_rows(name, tables[name])
                        for name in self.contract.output_tables
                    }

                    paths = self.silver_repo.write_candidate_tables(data_prefix, day, output)

                    iceberg_staged = None
                    if iceberg_db is not None and iceberg_catalog is not None:
                        from procurement.silver.application.warehouse import stage_partition

                        tables_with_data = {
                            name: tbl for name, tbl in output.items() if len(tbl) > 0
                        }
                        if tables_with_data:
                            iceberg_staged = stage_partition(
                                iceberg_db,
                                iceberg_catalog,
                                namespace="silver",
                                tables=tables_with_data,
                                partition_date=day,
                                release_id=None,
                            )
                        else:
                            iceberg_staged = {}

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

                    import pyarrow as pa

                    pa.default_memory_pool().release_unused()
                    if len(report["days"]) % 10 == 0:
                        gc.collect()
                except Exception:  # noqa: BLE001, S110
                    pass

                if entry["status"] == "failed" and not continue_on_error:
                    break

                day += timedelta(days=1)

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
                report["status"] in ("completed", "needs_review")
                and iceberg_catalog is not None
            ):
                for clean_name in self.contract.output_tables:
                    try:
                        table_info = iceberg_catalog.table("silver", clean_name)
                        if table_info:
                            snap_id = table_info.get("metadata", {}).get("current-snapshot-id")
                            if snap_id is not None and snap_id >= 0:
                                iceberg_catalog.pin("silver", clean_name, snap_id, run_id)
                    except Exception:
                        pass
        finally:
            if iceberg_db is not None:
                try:
                    from procurement.silver.application.warehouse import ensure_views

                    ensure_views(iceberg_db, namespace="silver")
                except Exception:
                    pass
                iceberg_db.close()
            if iceberg_catalog is not None:
                iceberg_catalog.close()

        save_report()
        return report


# Backward compatibility alias
ProcessNoticeService = ProcessSilverService

__all__ = ["ProcessNoticeService", "ProcessSilverService"]
