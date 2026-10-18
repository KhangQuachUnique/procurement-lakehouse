"""Outbound port for querying active Bronze metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


@dataclass(frozen=True)
class ActiveCommitFileDescriptor:
    commit_id: str
    partition_id: str
    source: str
    resource: str
    source_date: date
    table_name: str
    bucket: str
    object_key: str
    file_number: int
    row_count: int
    size_bytes: int
    sha256: str | None = None


class ActiveBronzeMetadataPort(Protocol):
    """Port for discovering active resources and pin-pointing active commit files."""

    def get_active_summary(self) -> list[dict[str, Any]]:
        """Get summary of active resources from metadata catalog."""
        ...

    def get_active_tables(self) -> list[dict[str, Any]]:
        """Get list of active tables and row/file counts."""
        ...

    def get_active_files(
        self,
        *,
        resource: str | None = None,
        table_name: str | None = None,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        max_files: int | None = None,
    ) -> list[ActiveCommitFileDescriptor]:
        """Query active commit files matching the filter."""
        ...
