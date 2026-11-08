"""Immutable local source change receipts tied to exact Iceberg snapshot edges.

Missing receipts, identity changes, unsupported ancestry and corrupt files are
not CDC: consumers must use a complete pinned source read instead. No timestamp
watermarks, snapshot-ID ordering assumptions or external services are used.
"""

import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from procurement.storage.iceberg import identifier, literal


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def record(root, namespace, table, before, after, table_uuid, old_rows, new_rows):
    """Only call after writer verifies one direct snapshot edge and its cohort.

    Directory publication is atomic; incomplete staging folders are never read.
    An acknowledged Iceberg commit without a receipt is a detectable gap.
    """
    identifier(namespace)
    identifier(table)
    if not table_uuid or after is None or before == after:
        raise ValueError("Change receipt requires an identified snapshot transition")
    parent = Path(root).resolve() / namespace / table
    parent.mkdir(parents=True, exist_ok=True)
    folder = parent / ("staging_" + uuid4().hex)
    folder.mkdir()
    files = {}
    for name, rows in (("old", old_rows), ("new", new_rows)):
        path = folder / (name + ".parquet")
        pq.write_table(rows, path)
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
        files[name] = {"sha256": digest(path), "rows": len(rows)}
    event_id = hashlib.sha256(
        json.dumps([namespace, table, table_uuid, before, after]).encode()
    ).hexdigest()
    receipt = {
        "version": 1,
        "event_id": event_id,
        "namespace": namespace,
        "table": table,
        "table_uuid": table_uuid,
        "before": before,
        "after": after,
        "files": files,
    }
    from procurement.silver.domain.schema import get_primary_key

    key = get_primary_key(table)
    if key and key in old_rows.column_names and key in new_rows.column_names:
        old = {row[key]: row for row in old_rows.to_pylist()}
        new = {row[key]: row for row in new_rows.to_pylist()}
        if len(old) != len(old_rows) or len(new) != len(new_rows) or None in old or None in new:
            raise ValueError("Change receipt requires unique non-null source keys")
        relationships = (
            "entity_id",
            "observation_id",
            "revision_id",
            "package_id",
            "bid_id",
            "package_no",
            "plan_id",
            "plan_no",
            "notify_no",
            "result_id",
        )
        receipt["changes"] = [
            {
                "key": identity,
                "type": "INSERT"
                if identity not in old
                else "DELETE"
                if identity not in new
                else "UPDATE",
                "old_relationships": {
                    k: old[identity][k] for k in relationships if k in old[identity]
                }
                if identity in old
                else None,
                "new_relationships": {
                    k: new[identity][k] for k in relationships if k in new[identity]
                }
                if identity in new
                else None,
            }
            for identity in sorted(old.keys() | new.keys())
            if old.get(identity) != new.get(identity)
        ]
    with (folder / "receipt.json").open("x") as stream:
        json.dump(receipt, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    # Repeated receipt publication is idempotent; conflicting evidence fails
    # closed. Unpublished staging folders remain ignored and audit-cleanable.
    destination = parent / ("event_" + event_id)
    if destination.exists():
        if json.loads((destination / "receipt.json").read_text()) != receipt:
            raise ValueError("Conflicting source change event identity")
        return destination
    os.rename(folder, destination)
    descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return destination


def chain(root, namespace, table, table_uuid, before, after):
    """Return a complete, unambiguous backwards chain or raise for safe fallback."""
    identifier(namespace)
    identifier(table)
    if before == after:
        return []
    events = {}
    for path in (Path(root) / namespace / table).glob("event_*/receipt.json"):
        data = json.loads(path.read_text())
        if (
            data.get("version") != 1
            or data.get("namespace") != namespace
            or data.get("table") != table
        ):
            raise ValueError("Invalid change receipt identity")
        if data["table_uuid"] == table_uuid:
            if data["after"] in events:
                raise ValueError("Ambiguous source change edge")
            events[data["after"]] = (path.parent, data)
    result, seen, cursor = [], set(), after
    while cursor != before:
        if cursor in seen or cursor not in events:
            raise ValueError("Missing/non-contiguous source change range")
        seen.add(cursor)
        folder, data = events[cursor]
        for side in ("old", "new"):
            file = folder / (side + ".parquet")
            if digest(file) != data["files"][side]["sha256"]:
                raise ValueError("Source change file integrity failure")
        result.append((folder, data))
        cursor = data["before"]
    return list(reversed(result))


def replay(db, base, events, target):
    """Multiset replacement verifies every old row before applying an edge.

    Local staging only. Caller atomically accepts the resulting cache with its
    Gold outbox, never advances an accepted checkpoint on receipt discovery.
    """
    for part in (*base.split("."), *target.split(".")):
        identifier(part)
    db.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {base}")
    changed = 0
    for folder, event in events:
        old = "read_parquet(" + literal(str((folder / "old.parquet").resolve())) + ")"
        new = "read_parquet(" + literal(str((folder / "new.parquet").resolve())) + ")"
        missing = db.execute(
            f"SELECT count(*) FROM (SELECT * FROM {old} EXCEPT ALL SELECT * FROM {target})"
        ).fetchone()[0]
        if missing:
            raise ValueError("Source cache does not contain journal's before image")
        db.execute(
            f"CREATE OR REPLACE TABLE {target} AS (SELECT * FROM {target} EXCEPT ALL SELECT * FROM {old}) UNION ALL SELECT * FROM {new}"
        )
        changed += event["files"]["old"]["rows"] + event["files"]["new"]["rows"]
    return changed


def capture_before(db, catalog, namespace, table, candidate, replaced_result_rids, partition_date):
    """Capture exactly the cohort replaced by stage_partition, at its old pin."""
    from procurement.common.silver_gold_contract import INPUT_TABLES, projection
    from procurement.silver.domain.schema import get_partition_spec, get_primary_key

    if table not in INPUT_TABLES:
        return None
    relation = f"lake.{identifier(namespace)}.{identifier(table)}"
    metadata = catalog.table(namespace, table)["metadata"]
    snapshot = metadata.get("current-snapshot-id")
    snapshot = snapshot if snapshot is not None and snapshot >= 0 else None
    pk = get_primary_key(table)
    if table in {"award_lot", "award_contractor"} and replaced_result_rids:
        predicate = (
            "revision_id IN (" + ",".join(literal(r) for r in sorted(replaced_result_rids)) + ")"
        )
    elif pk and candidate is not None and len(candidate):
        # Literal keys make the predicate durable across Arrow unregister/retry.
        keys = sorted({str(k) for k in candidate[pk].to_pylist() if k is not None})
        predicate = pk + " IN (" + ",".join(literal(k) for k in keys) + ")" if keys else "FALSE"
    elif get_partition_spec(table) and partition_date is not None:
        # Match the existing writer's empty explicit-partition deletion exactly.
        predicate = get_partition_spec(table) + "=" + literal(str(partition_date))
    else:
        predicate = "FALSE"
    schema = db.execute("DESCRIBE " + relation).fetchall()
    columns = [r[0] for r in schema]
    select = ",".join(projection(columns, table))
    pinned = relation + (f" AT (VERSION => {snapshot})" if snapshot is not None else "")
    return {
        "before": snapshot,
        "table_uuid": metadata.get("table-uuid"),
        "predicate": predicate,
        "select": select,
        "old_query": f"SELECT {select} FROM {pinned} WHERE "
        + (predicate if snapshot is not None else "FALSE"),
        "candidate": candidate,
        "schema": schema,
    }


def capture_after(db, root, namespace, table, before, metadata):
    """Only a verified direct parent edge is authoritative; otherwise record a gap."""
    if before is None:
        return {"status": "not_tracked"}
    after = metadata.get("current-snapshot-id")
    if after == before["before"]:
        return {"status": "unchanged"}
    snapshot = next((s for s in metadata.get("snapshots", []) if s["snapshot-id"] == after), None)
    if (
        not snapshot
        or snapshot.get("parent-snapshot-id") != before["before"]
        or metadata.get("table-uuid") != before["table_uuid"]
        or after is None
    ):
        return {"status": "gap", "reason": "unproven_direct_parent_or_table_identity"}
    relation = f"lake.{identifier(namespace)}.{identifier(table)} AT (VERSION => {int(after)})"
    candidate = before["candidate"]
    if candidate is not None and all(c in candidate.column_names for c, *_ in before["schema"]):
        # The actual INSERT applies these same target casts. For complete typed
        # candidates no additional S3 read is needed to recover its after image.
        db.register("_change_candidate", candidate)
        try:
            fields = ",".join(f"CAST({c} AS {t}) AS {c}" for c, t, *_ in before["schema"])
            new = db.execute(
                f"SELECT {before['select']} FROM (SELECT {fields} FROM _change_candidate)"
            ).to_arrow_table()
        finally:
            db.unregister("_change_candidate")
        after_origin = "typed_writer_candidate"
    else:
        new = db.execute(
            f"SELECT {before['select']} FROM {relation} WHERE {before['predicate']}"
        ).to_arrow_table()
        after_origin = "pinned_source_cohort"
    if snapshot.get("summary", {}).get("operation") == "append":
        # An append-only direct parent edge proves no existing rows were
        # removed by the writer's preceding DELETE. Do not rescan old history.
        old = new.slice(0, 0)
        before_origin = "verified_append_empty_before_image"
    else:
        old = db.execute(before["old_query"]).to_arrow_table()
        before_origin = "pinned_source_cohort"
    path = record(root, namespace, table, before["before"], after, before["table_uuid"], old, new)
    return {
        "status": "recorded",
        "event": str(path),
        "before": before["before"],
        "after": after,
        "before_image_origin": before_origin,
        "after_image_origin": after_origin,
        "old_rows": len(old),
        "new_rows": len(new),
    }
