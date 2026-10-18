"""Outbound port for exporting profiling metrics and markdown reports."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from procurement.profiling.domain.models import TableProfile, WindowDrift


class ReportExportPort(Protocol):
    """Port for writing profile.json and profiling-report.md."""

    def export_json(
        self,
        output_path: str | Path,
        profiles: dict[str, TableProfile],
        drifts: list[WindowDrift],
        summary: list[dict[str, Any]],
    ) -> Path:
        ...

    def export_markdown_report(
        self,
        output_path: str | Path,
        profiles: dict[str, TableProfile],
        drifts: list[WindowDrift],
        summary: list[dict[str, Any]],
    ) -> Path:
        ...
