"""Application Service implementing ProcessNoticeUseCase.

Orchestrates Silver notice candidate builds via BronzeRepositoryPort and
SilverCandidateRepositoryPort in accordance with Hexagonal Architecture.
"""

from collections.abc import Callable
from datetime import date, timedelta
from typing import Any
from uuid import uuid4

from procurement.silver.application.observations import transform
from procurement.silver.application.resources import get_silver_resource
from procurement.silver.application.revisions import assemble, validate
from procurement.silver.application.warehouse import typed_arrow_rows
from procurement.silver.ports.inbound.process_notice import ProcessNoticeUseCase
from procurement.silver.ports.outbound.bronze_repository import BronzeRepositoryPort
from procurement.silver.ports.outbound.silver_repository import SilverCandidateRepositoryPort


class ProcessNoticeService(ProcessNoticeUseCase):
    """Hexagonal use case implementation for notice candidate generation."""

    def __init__(
        self,
        bronze_repo: BronzeRepositoryPort,
        silver_repo: SilverCandidateRepositoryPort,
        bucket: str,
        resource_name: str = "notice",
    ) -> None:
        self.bronze_repo = bronze_repo
        self.silver_repo = silver_repo
        self.bucket = bucket
        self.resource_name = resource_name
        self.contract = get_silver_resource(resource_name)

    def execute(
        self,
        start_date: date,
        end_date: date,
        *,
        family: str | None = None,
        continue_on_error: bool = False,
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
        }

        def save_report() -> None:
            self.silver_repo.save_run_report(report_prefix, report)

        day = start_date
        while day <= end_date:
            entry: dict[str, Any] = {"date": str(day), "status": "running"}
            try:
                partitions = self.bronze_repo.select_committed_partitions(
                    [self.contract.resource], day, day
                )
                if len(partitions) != 1:
                    raise ValueError(f"Expected exactly one committed day for {day}")

                partition = partitions[0]
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

                entry.update(
                    status="needs_review"
                    if quality["quarantined"] or quality["blocking_issues"]
                    else "completed",
                    input_counts=input_counts,
                    quality=quality,
                    files=paths,
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
                    if isinstance(exc, ValueError)
                    else "Storage or processing error; inspect local service logs",
                )

            report["days"].append(entry)
            save_report()

            if on_progress:
                on_progress(entry)

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
        save_report()
        return report
