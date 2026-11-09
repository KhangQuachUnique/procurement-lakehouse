"""Outbound Port for reading Silver data."""

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

import pyarrow as pa

from procurement.gold.domain.models import SilverReadyInput, Snapshot


@runtime_checkable
class SilverReaderPort(Protocol):
    """Port interface for querying and streaming Silver lakehouse data."""

    def verify(self, ready: SilverReadyInput) -> None:
        """Verify that catalog snapshots match producer readiness attestations."""
        ...

    def columns(self, snapshot: Snapshot) -> tuple[tuple[str, str], ...]:
        """Return (column_name, sql_type) pairs for the given Silver table snapshot."""
        ...

    def batches(self, snapshot: Snapshot) -> Iterator[pa.RecordBatch]:
        """Stream Arrow RecordBatches for the given Silver snapshot."""
        ...
