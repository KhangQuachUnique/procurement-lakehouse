"""Bounded physical-partition INSERTs, exact per-chunk ACK/reconciliation.

Private source is not Gold-ready until the complete table vector is sealed.
Chunk commits are disjoint row sets, never new business grain or data selection.
"""

import contextlib
import json
from pathlib import Path

from procurement.storage.iceberg import identifier, literal
from procurement.tools.gold_history_candidate import save


def _sid(meta):
    value = meta.get("current-snapshot-id")
    return value if value is not None and value >= 0 else None


def load_partitioned(
    db,
    catalog,
    namespace,
    name,
    expected,
    output,
    identity,
    field,
    *,
    partitions_per_insert=8,
    max_rows=50000,
    after_chunk=lambda *_: None,
):
    for value in (namespace, name, expected, field):
        identifier(value)
    if not 1 <= partitions_per_insert <= 32:
        raise ValueError("Partition batch bound required")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    table = f"lake.{namespace}.{name}"
    meta = catalog.table(namespace, name)["metadata"]
    uuid = meta["table-uuid"]
    partitions = db.execute(
        f"SELECT {field},count(*) FROM {expected} GROUP BY {field} ORDER BY {field} NULLS FIRST LIMIT 10001"
    ).fetchall()
    if len(partitions) > 10000:
        raise ValueError("Partition inventory exceeds bounded envelope")
    units, counts = [], []
    pending, weight = [], 0

    def flush():
        nonlocal pending, weight
        if pending:
            units.append(
                f"{field} BETWEEN CAST({literal(str(pending[0]))} AS DATE) AND CAST({literal(str(pending[-1]))} AS DATE)"
            )
            counts.append(weight)
            pending, weight = [], 0

    for value, rows in partitions:
        condition = (
            f"{field} IS NULL" if value is None else f"{field}=CAST({literal(str(value))} AS DATE)"
        )
        if rows > max_rows:
            flush()
            from procurement.silver.domain.schema import get_primary_key

            key = identifier(get_primary_key(name))
            cursor = db.execute(
                f"SELECT {key},count(*) FROM {expected} WHERE {condition} GROUP BY {key} ORDER BY {key}"
            )
            first = last = None
            used = 0

            def emit_hot(condition, key, first, last, used):
                if used:
                    units.append(
                        f"({condition}) AND {key} BETWEEN {literal(str(first))} AND {literal(str(last))}"
                    )
                    counts.append(used)

            while page := cursor.fetchmany(1024):
                for pk, multiplicity in page:
                    if pk is None or multiplicity > max_rows:
                        raise ValueError("Single source key exceeds row guard or is NULL")
                    if used and used + multiplicity > max_rows:
                        emit_hot(condition, key, first, last, used)
                        first = last = None
                        used = 0
                    if not used:
                        first = pk
                    last = pk
                    used += multiplicity
            emit_hot(condition, key, first, last, used)
        elif value is None:
            flush()
            units.append(condition)
            counts.append(rows)
        else:
            if pending and (len(pending) >= partitions_per_insert or weight + rows > max_rows):
                flush()
            pending.append(value)
            weight += rows
    flush()
    # The heaviest row-count batch first makes the bounded operator gate useful;
    # source selection/multiplicity and final table contents are order-independent.
    ordered = sorted(zip(units, counts), key=lambda item: (-item[1], item[0]))
    units = [item[0] for item in ordered]
    counts = [item[1] for item in ordered]
    if len(units) > 10000:
        raise ValueError("Physical batch plan exceeds bounded envelope")
    plan = {
        "identity": identity,
        "uuid": uuid,
        "field": field,
        "partitions_per_insert": partitions_per_insert,
        "max_rows": max_rows,
        "units": units,
        "rows": counts,
    }
    path = output / "plan.json"
    if path.exists() and json.loads(path.read_text()) != plan:
        raise ValueError("Partitioned INSERT plan changed")
    if not path.exists():
        if _sid(meta) is not None:
            raise ValueError("Unknown nonempty partitioned table")
        save(path, plan)
    base = None
    total = 0
    for i, where in enumerate(units):
        rows = counts[i]
        if rows > max_rows:
            raise ValueError(f"Physical batch exceeds row guard: {name}/{i}/{rows}")
        intent = {
            "identity": identity,
            "uuid": uuid,
            "index": i,
            "where": where,
            "rows": rows,
            "base_snapshot": base,
        }
        ip = output / f"{i:05d}.intent.json"
        ack = output / f"{i:05d}.json"
        if ack.exists():
            saved = json.loads(ack.read_text())
            if any(saved.get(k) != v for k, v in intent.items()):
                raise ValueError("Chunk ACK identity changed")
            base = saved["snapshot_id"]
            total += rows
            continue
        meta = catalog.table(namespace, name)["metadata"]
        current = _sid(meta)
        if meta["table-uuid"] != uuid:
            raise ValueError("Partition target UUID changed")
        if ip.exists():
            if json.loads(ip.read_text()) != intent:
                raise ValueError("Chunk intent changed")
        else:
            if current != base:
                raise ValueError("Unknown snapshot before chunk intent")
            save(ip, intent)
        recovered = current != base
        if recovered:
            snap = next((s for s in meta.get("snapshots", []) if s["snapshot-id"] == current), None)
            if snap is None or snap.get("parent-snapshot-id") != base:
                raise ValueError("Uncertain chunk snapshot chain")
        else:
            db.execute("BEGIN")
            try:
                db.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {expected} WHERE {where}")
                db.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    db.execute("ROLLBACK")
                raise
            after_chunk(i, "committed_before_ack")
            meta = catalog.table(namespace, name)["metadata"]
            current = _sid(meta)
            if meta["table-uuid"] != uuid:
                raise ValueError("Target changed during chunk publication")
        # An uncertain commit also rechecks every earlier bounded prefix unit;
        # counts alone cannot detect another writer replacing old values.
        bad = 0
        for predicate in units[: i + 1] if recovered else [where]:
            bad += db.execute(
                f"SELECT count(*) FROM ((SELECT * FROM {table} WHERE {predicate} EXCEPT ALL SELECT * FROM {expected} WHERE {predicate}) UNION ALL (SELECT * FROM {expected} WHERE {predicate} EXCEPT ALL SELECT * FROM {table} WHERE {predicate}))"
            ).fetchone()[0]
        actual = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if bad or actual != total + rows:
            raise ValueError("Chunk publication/uncertain commit differs from expected prefix")
        save(ack, intent | {"snapshot_id": current, "reconciled_ack_loss": recovered})
        total += rows
        base = current
        after_chunk(i, "acknowledged")
    final = catalog.table(namespace, name)["metadata"]
    if final["table-uuid"] != uuid or _sid(final) != base:
        raise ValueError("Final partitioned snapshot moved")
    if db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] != total:
        raise ValueError("Final table count changed")
    return {
        "identity": identity,
        "table_uuid": uuid,
        "snapshot_id": base,
        "operation": "bounded_partition_insert",
        "chunks": len(units),
        "rows": total,
    }
