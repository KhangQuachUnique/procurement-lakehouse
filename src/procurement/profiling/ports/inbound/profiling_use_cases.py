"""Inbound ports (driver ports) defining profiling use cases."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Protocol

from procurement.profiling.domain.models import TableProfile, WindowDrift


class ProfileTableUseCase(Protocol):
    """Use case for profiling an active Bronze table."""

    def execute(
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
        ...


class ProfileJsonlUseCase(Protocol):
    """Use case for profiling a local JSONL dataset."""

    def execute(
        self,
        file_path: str | Path,
        table_name: str = "jsonl_dataset",
        resource: str = "jsonl",
        *,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> TableProfile:
        ...


class CompareWindowsUseCase(Protocol):
    """Use case for detecting drift between two or more ingestion windows."""

    def execute(
        self,
        table_name: str,
        resource: str,
        windows: list[tuple[str, str]],
        *,
        max_records_per_window: int = 2000,
    ) -> tuple[dict[str, TableProfile], list[WindowDrift]]:
        ...
