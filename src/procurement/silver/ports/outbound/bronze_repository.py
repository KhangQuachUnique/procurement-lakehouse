"""Bronze Repository Port for Silver.

Defines the interface for Silver to query committed Bronze partitions and records
without coupling to internal Bronze ingestion mechanics.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


@dataclass(frozen=True)
class BronzeFileDescriptor:
    table: str
    key: str
    sha256: str


@dataclass(frozen=True)
class CommittedBronzePartition:
    resource: str
    source_date: str
    run_id: str
    expected_records: int
    files: tuple[BronzeFileDescriptor, ...]
    manifest: dict[str, Any]


class BronzeRepositoryPort(Protocol):
    """Outbound port for querying committed Bronze data."""

    def select_committed_partitions(
        self,
        resources: list[str],
        start: date,
        end: date,
    ) -> list[CommittedBronzePartition]:
        """Discover and freeze committed Bronze partitions for date range."""
        ...

    def stream_partition_records(
        self,
        partition: CommittedBronzePartition,
        selected_tables: tuple[str, ...] | None = None,
        verify_content_hash: bool = False,
    ) -> Iterator[tuple[dict[str, Any], int, dict[str, Any]]]:
        """Stream raw records from committed partition.

        Yields (file_info, ordinal, row_dict).
        """
        ...
