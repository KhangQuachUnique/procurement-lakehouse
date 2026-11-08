"""Injected Iceberg read session. No source writes, tags, credentials or clock here."""

from procurement.common.silver_gold_contract import projection
from procurement.gold.v2.contracts import INPUT_TABLES, identifier


class IcebergReader:
    """Receipt pins need to remain available through the operator's retention policy.

    This adapter does not manufacture a producer readiness attestation. Each
    supplied snapshot must still exist with the exact table UUID; newer current
    snapshots do not invalidate a previously attested coherent source vector.
    """

    def __init__(self, connection, catalog, namespace):
        self.db, self.catalog, self.namespace = connection, catalog, identifier(namespace)

    def verify(self, ready):
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

    def query(self, snapshot):
        if snapshot.table not in INPUT_TABLES:
            raise ValueError("Unexpected source table")
        relation = f"lake.{self.namespace}.{snapshot.table}"
        columns = [r[0] for r in self.db.execute("DESCRIBE " + relation).fetchall()]
        select = ",".join(projection(columns, snapshot.table))
        if snapshot.snapshot_id is not None:
            relation += f" AT (VERSION => {snapshot.snapshot_id})"
            return f"SELECT {select} FROM {relation}"
        return f"SELECT {select} FROM {relation} WHERE FALSE"

    def columns(self, snapshot):
        return tuple(
            (r[0], r[1]) for r in self.db.execute("DESCRIBE " + self.query(snapshot)).fetchall()
        )

    def batches(self, snapshot):
        yield from self.db.execute(self.query(snapshot)).to_arrow_reader(batch_size=8192)
