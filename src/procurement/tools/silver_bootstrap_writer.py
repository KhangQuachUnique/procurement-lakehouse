"""Bulk INSERT to isolated empty tables; uncertain commits reconcile, never append blindly."""

import contextlib
import json

from procurement.silver.domain.schema import to_iceberg_ddl
from procurement.storage.catalog_scope import NamespaceCatalog
from procurement.storage.iceberg import identifier
from procurement.tools.gold_history_candidate import save


def mismatch(db, table, expected):
    return db.execute(
        f"SELECT count(*) FROM ((SELECT * FROM {table} EXCEPT ALL SELECT * FROM {expected}) "
        f"UNION ALL (SELECT * FROM {expected} EXCEPT ALL SELECT * FROM {table}))"
    ).fetchone()[0]


def load_empty(
    db,
    catalog,
    namespace,
    views,
    output,
    identity,
    *,
    after_commit=lambda _: None,
    allow_managed_namespace=False,
    partition_batch_size=0,
    after_partition_chunk=lambda *_: None,
):
    namespace = identifier(namespace)
    if not (
        namespace.startswith("test_silver_v2_")
        or (allow_managed_namespace and namespace.startswith("silver_v2_"))
    ):
        raise ValueError("Bootstrap requires isolated namespace")
    catalog = NamespaceCatalog(catalog, namespace)
    output.mkdir(parents=True, exist_ok=True)
    pins = {}
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{namespace}")
    for name, expected in sorted(views.items()):
        identifier(name)
        identifier(expected)
        db.execute(to_iceberg_ddl(name, namespace=namespace))
        table = f"lake.{namespace}.{name}"
        meta = catalog.table(namespace, name)["metadata"]
        if meta.get("format-version") != 2:
            raise ValueError("Bootstrap requires the approved Iceberg v2 format")
        sid = meta.get("current-snapshot-id")
        sid = sid if sid is not None and sid >= 0 else None
        ack = output / (name + ".json")
        intent = output / (name + ".intent.json")
        planned = {"identity": identity, "table_uuid": meta["table-uuid"], "base_snapshot": None}
        if intent.exists():
            if json.loads(intent.read_text()) != planned:
                raise ValueError("Bootstrap target UUID/inputs differ from its empty-table intent")
        elif sid is not None:
            raise ValueError("Initial bulk INSERT refuses an existing nonempty table")
        else:
            save(intent, planned)
        if ack.exists():
            value = json.loads(ack.read_text())
            if (
                value["identity"] != identity
                or value["table_uuid"] != meta["table-uuid"]
                or value["snapshot_id"] != sid
            ):
                raise ValueError("Bootstrap ACK/code/input or target snapshot changed")
            pins[name] = value
            continue
        from procurement.silver.domain.schema import get_partition_spec

        field = get_partition_spec(name)
        if partition_batch_size and field:
            from procurement.tools.silver_partitioned_insert import load_partitioned

            chunk_root = output / (name + "-chunks")

            def partition_progress(index, phase, chunk_root=chunk_root, table_name=name):
                if phase == "acknowledged":
                    units = json.loads((chunk_root / "plan.json").read_text())["units"]
                    after_partition_chunk(table_name, index + 1, len(units))

            value = load_partitioned(
                db,
                catalog,
                namespace,
                name,
                expected,
                output / (name + "-chunks"),
                identity,
                field,
                partitions_per_insert=partition_batch_size,
                after_chunk=partition_progress,
            )
            save(ack, value)
            pins[name] = value
            after_commit(name)
            continue
        recovered = sid is not None
        if recovered:
            # An INSERT committed but its local ACK may have been lost. Never
            # INSERT again into this nonempty/unknown state without reconciliation.
            if mismatch(db, table, expected):
                raise ValueError("Unacknowledged nonempty bootstrap table differs")
        else:
            db.execute("BEGIN")
            try:
                db.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {expected}")
                db.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    db.execute("ROLLBACK")
                raise
            after_commit(name)
            meta = catalog.table(namespace, name)["metadata"]
            sid = meta.get("current-snapshot-id")
            sid = sid if sid is not None and sid >= 0 else None
        value = {
            "identity": identity,
            "table_uuid": meta["table-uuid"],
            "snapshot_id": sid,
            "operation": "bulk_insert",
            "reconciled_commit_ack_loss": recovered,
        }
        save(ack, value)
        pins[name] = value
    return pins


def apply_delta(
    db,
    catalog,
    namespace,
    prepared,
    expected_full,
    base_pins,
    output,
    identity,
    *,
    after_commit=lambda _: None,
):
    """One set-based update per table; MERGE only with proven unique source+target.

    Owner-set replacement and duplicate-PK multiplicity require delete+insert;
    a plain MERGE does not retract children absent from the incoming set.
    Unknown committed states reconcile against the independent full result.
    """
    from procurement.silver.domain.schema import get_primary_key
    from procurement.tools.silver_bootstrap_views import OWNER_TABLES

    namespace = identifier(namespace)
    if not namespace.startswith("test_silver_v2_"):
        raise ValueError("Incremental proof requires isolated namespace")
    catalog = NamespaceCatalog(catalog, namespace)
    output.mkdir(parents=True, exist_ok=True)
    pins = {}
    for name, source in sorted(prepared["views"].items()):
        table = f"lake.{namespace}.{identifier(name)}"
        meta = catalog.table(namespace, name)["metadata"]
        sid = meta.get("current-snapshot-id")
        sid = sid if sid is not None and sid >= 0 else None
        base = base_pins[name]
        intent = {
            "identity": identity,
            "table_uuid": base["table_uuid"],
            "base_snapshot": base["snapshot_id"],
        }
        if meta["table-uuid"] != base["table_uuid"]:
            raise ValueError("Incremental source target UUID changed")
        intent_path = output / (name + ".intent.json")
        if intent_path.exists() and json.loads(intent_path.read_text()) != intent:
            raise ValueError("Incremental immutable intent changed")
        if not intent_path.exists():
            if sid != base["snapshot_id"]:
                raise ValueError("Incremental target moved before first write")
            save(intent_path, intent)
        ack = output / (name + ".json")
        if ack.exists():
            value = json.loads(ack.read_text())
            if (
                value["identity"] != identity
                or value["snapshot_id"] != sid
                or value["table_uuid"] != meta["table-uuid"]
            ):
                raise ValueError("Incremental ACK or live snapshot differs")
            pins[name] = value
            continue
        recovered = sid != base["snapshot_id"]
        operation = "reconciled_unknown_commit"
        if recovered:
            if mismatch(db, table, expected_full[name]):
                raise ValueError("Unacknowledged incremental table differs from full calculation")
        else:
            if name in OWNER_TABLES:
                owner = OWNER_TABLES[name]
                key = "revision_id" if owner == "result" else "observation_id"
                delete = f"DELETE FROM {table} WHERE {key} IN (SELECT owner FROM {prepared['owners'][owner]})"
                statements = [delete, f"INSERT INTO {table} BY NAME SELECT * FROM {source}"]
                operation = "replace_owner_set"
            else:
                pk = get_primary_key(name)
                duplicates = any(
                    db.execute(
                        f"SELECT 1 FROM {relation} GROUP BY {pk} HAVING count(*)>1 LIMIT 1"
                    ).fetchone()
                    for relation in (source, table)
                )
                if duplicates:
                    statements = [
                        f"DELETE FROM {table} WHERE {pk} IN (SELECT {pk} FROM {source})",
                        f"INSERT INTO {table} BY NAME SELECT * FROM {source}",
                    ]
                    operation = "upsert_preserving_cohort_multiplicity"
                else:
                    columns = [
                        r[0] for r in db.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
                    ]
                    fields = ",".join(identifier(c) for c in columns)
                    updates = ",".join(f"{identifier(c)}=s.{identifier(c)}" for c in columns)
                    values = ",".join(f"s.{identifier(c)}" for c in columns)
                    statements = [
                        f"MERGE INTO {table} t USING {source} s ON t.{pk}=s.{pk} WHEN MATCHED THEN UPDATE SET {updates} WHEN NOT MATCHED THEN INSERT ({fields}) VALUES ({values})"
                    ]
                    operation = "merge"
            db.execute("BEGIN")
            try:
                for sql in statements:
                    db.execute(sql)
                db.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    db.execute("ROLLBACK")
                raise
            after_commit(name)
            meta = catalog.table(namespace, name)["metadata"]
            sid = meta.get("current-snapshot-id")
            sid = sid if sid is not None and sid >= 0 else None
        value = {
            "identity": identity,
            "table_uuid": meta["table-uuid"],
            "snapshot_id": sid,
            "operation": operation,
            "reconciled_commit_ack_loss": recovered,
        }
        save(ack, value)
        pins[name] = value
    return pins
