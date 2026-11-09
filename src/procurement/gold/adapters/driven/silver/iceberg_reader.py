"""Injected Iceberg read adapter for Silver lakehouse.

Hexagonal Driven Outbound Adapter implementing SilverReaderPort.
"""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from procurement.common.silver_gold_contract import INPUT_TABLES, projection
from procurement.gold.domain.models import SilverReadyInput, Snapshot, identifier
from procurement.gold.ports.outbound.silver_reader import SilverReaderPort


class IcebergReader(SilverReaderPort):
    """Iceberg read adapter querying lake.{namespace} pinned by snapshot IDs."""

    def __init__(self, connection: Any, catalog: Any, namespace: str):
        self.db = connection
        self.catalog = catalog
        self.namespace = identifier(namespace)

    def verify(self, ready: SilverReadyInput) -> None:
        if ready.namespace != self.namespace:
            raise ValueError("Receipt source namespace differs")
        for snapshot in ready.tables:
            meta = self.catalog.table(self.namespace, snapshot.table)["metadata"]
            if meta.get("table-uuid") != snapshot.table_uuid:
                raise ValueError("Source table identity changed")
            ids = {s["snapshot-id"] for s in meta.get("snapshots", [])}
            if snapshot.snapshot_id is not None and snapshot.snapshot_id not in ids:
                raise ValueError("Required source snapshot no longer retained")
            if snapshot.snapshot_id is None and meta.get("current-snapshot-id") not in (None, -1):
                raise ValueError("Unpinned empty input cannot substitute a nonempty table")

    def query(self, snapshot: Snapshot) -> str:
        if snapshot.table not in INPUT_TABLES:
            raise ValueError("Unexpected source table")
        relation = f"lake.{self.namespace}.{snapshot.table}"
        columns = [r[0] for r in self.db.execute("DESCRIBE " + relation).fetchall()]
        select = ",".join(projection(columns, snapshot.table))
        if snapshot.snapshot_id is not None:
            relation += f" AT (VERSION => {snapshot.snapshot_id})"
            return f"SELECT {select} FROM {relation}"
        return f"SELECT {select} FROM {relation} WHERE FALSE"

    def columns(self, snapshot: Snapshot) -> tuple[tuple[str, str], ...]:
        return tuple(
            (r[0], r[1]) for r in self.db.execute("DESCRIBE " + self.query(snapshot)).fetchall()
        )

    def batches(self, snapshot: Snapshot) -> Iterator[pa.RecordBatch]:
        yield from self.db.execute(self.query(snapshot)).to_arrow_reader(batch_size=8192)
