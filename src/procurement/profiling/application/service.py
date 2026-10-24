"""Application service orchestrating profiling use cases across ports and domain."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from procurement.profiling.domain.models import TableProfile, WindowDrift
from procurement.profiling.domain.profiler import TableProfilerEngine
from procurement.profiling.ports.outbound.bronze_metadata_port import (
    ActiveBronzeMetadataPort,
)
from procurement.profiling.ports.outbound.record_stream_port import (
    BronzeRecordStreamPort,
    JsonlRecordStreamPort,
)
from procurement.profiling.ports.outbound.report_export_port import ReportExportPort


class ProfilingService:
    """Hexagonal application service implementing profiling use cases.
    
    Coordinates domain profilers, metadata lookups, streaming ports, and export ports.
    """

    def __init__(
        self,
        *,
        metadata_port: ActiveBronzeMetadataPort | None = None,
        bronze_stream_port: BronzeRecordStreamPort | None = None,
        jsonl_stream_port: JsonlRecordStreamPort | None = None,
        report_export_port: ReportExportPort | None = None,
    ) -> None:
        self.metadata_port = metadata_port
        self.bronze_stream_port = bronze_stream_port
        self.jsonl_stream_port = jsonl_stream_port
        self.report_export_port = report_export_port

    def get_summary(self) -> list[dict[str, Any]]:
        """Fetch high-level metadata summary of active Bronze resources."""
        if not self.metadata_port:
            raise ValueError("metadata_port is required for get_summary()")
        return self.metadata_port.get_active_summary()

    def get_active_tables(self) -> list[dict[str, Any]]:
        """Fetch list of active tables."""
        if not self.metadata_port:
            raise ValueError("metadata_port is required for get_active_tables()")
        return self.metadata_port.get_active_tables()

    def profile_active_table(
        self,
        table_name: str,
        resource: str,
        *,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        max_files: int | None = None,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> TableProfile:
        """Execute profiling on an active Bronze table."""
        if not self.metadata_port or not self.bronze_stream_port:
            raise ValueError("Both metadata_port and bronze_stream_port are required for profile_active_table()")

        files = self.metadata_port.get_active_files(
            table_name=table_name,
            resource=resource,
            start_date=start_date,
            end_date=end_date,
            max_files=max_files,
        )

        total_active_rows = sum(f.row_count for f in files)
        total_active_files = len(files)

        engine = TableProfilerEngine(table_name=table_name, resource=resource)
        for _, record in self.bronze_stream_port.stream_active_records(
            files,
            max_records=max_records,
            sample_step=sample_step,
        ):
            engine.feed_record(record)

        return engine.finalize(
            total_active_records=total_active_rows,
            total_active_files=total_active_files,
        )

    def profile_jsonl_file(
        self,
        file_path: str | Path,
        table_name: str = "jsonl_dataset",
        resource: str = "jsonl",
        *,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> TableProfile:
        """Execute profiling on a local JSONL file."""
        if not self.jsonl_stream_port:
            raise ValueError("jsonl_stream_port is required for profile_jsonl_file()")

        engine = TableProfilerEngine(table_name=table_name, resource=resource)
        total_records = 0

        for idx, record in self.jsonl_stream_port.stream_jsonl_records(
            file_path,
            max_records=max_records,
            sample_step=sample_step,
        ):
            engine.feed_record(record)
            total_records = idx

        return engine.finalize(
            total_active_records=total_records,
            total_active_files=1,
        )

    def compare_windows(
        self,
        table_name: str,
        resource: str,
        windows: list[tuple[str, str]],
        *,
        max_records_per_window: int = 2000,
    ) -> tuple[dict[str, TableProfile], list[WindowDrift]]:
        """Profile multiple date windows and calculate drift across them."""
        profiles: dict[str, TableProfile] = {}
        drifts: list[WindowDrift] = []

        for start_d, end_d in windows:
            label = f"{start_d}..{end_d}"
            prof = self.profile_active_table(
                table_name=table_name,
                resource=resource,
                start_date=start_d,
                end_date=end_d,
                max_records=max_records_per_window,
            )
            profiles[label] = prof

        labels = list(profiles.keys())
        for i in range(len(labels) - 1):
            w1 = labels[i]
            w2 = labels[i + 1]
            drift = TableProfilerEngine.compare_windows(profiles[w1], profiles[w2])
            drifts.append(drift)

        return profiles, drifts

    def export_reports(
        self,
        profiles: dict[str, TableProfile],
        drifts: list[WindowDrift],
        summary: list[dict[str, Any]],
        *,
        json_path: str | Path = "profile.json",
        report_path: str | Path = "profiling-report.md",
    ) -> tuple[Path, Path]:
        """Export results via report export port."""
        if not self.report_export_port:
            raise ValueError("report_export_port is required for export_reports()")

        j_path = self.report_export_port.export_json(json_path, profiles, drifts, summary)
        r_path = self.report_export_port.export_markdown_report(report_path, profiles, drifts, summary)
        return j_path, r_path
