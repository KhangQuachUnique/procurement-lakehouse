"""Outbound port for streaming records from storage or files."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

from procurement.profiling.ports.outbound.bronze_metadata_port import (
    ActiveCommitFileDescriptor,
)


class BronzeRecordStreamPort(Protocol):
    """Port for streaming active Bronze Parquet records."""

    def stream_active_records(
        self,
        files: list[ActiveCommitFileDescriptor],
        *,
        batch_size: int = 1024,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> Iterator[tuple[ActiveCommitFileDescriptor, dict[str, Any]]]:
        ...


class JsonlRecordStreamPort(Protocol):
    """Port for streaming records from local JSONL files."""

    def stream_jsonl_records(
        self,
        file_path: str | Path,
        *,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        ...
