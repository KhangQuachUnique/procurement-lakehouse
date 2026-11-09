"""Bounded, conflict-free co-publication using the unchanged canonical writer.

No new upsert semantics: never combine cohorts sharing a primary key, a result
owner or a governance observation. Ambiguous/unsupported inputs stay singleton.
A group is a compute/commit unit, NOT a new business grain or daily snapshot.
"""

import json
from datetime import date

import pyarrow as pa

from procurement.readiness_with_exception import validate_report
from procurement.silver.application.readiness import complete_day_publication
from procurement.silver.domain.schema import get_partition_spec, get_primary_key
from procurement.tools.gold_history_candidate import (
    canonical,
    digest,
    file_hash,
    save,
    verify_checkpoint,
)

SUPPORTED = {
    "observation",
    "quality_issue",
    "quarantine",
    "plan_revision",
    "package_revision",
    "package_location",
    "package_lot",
    "notice_revision",
    "notice_location",
    "notice_lot",
    "notice_reoffer_submission",
    "result_revision",
    "award_lot",
    "award_contractor",
}


def keys(tables):
    result = {}
    exclusive = False
    for name, table in tables.items():
        pk = get_primary_key(name)
        if name not in SUPPORTED or not pk:
            exclusive = True
            continue
        values = table[pk].to_pylist()
        if None in values or (not len(table) and get_partition_spec(name)):
            exclusive = True
        result[name] = set(values)
        if name in {"result_revision", "award_lot", "award_contractor"}:
            result.setdefault("@result_owner", set()).update(table["revision_id"].to_pylist())
        if name in {"observation", "quality_issue", "quarantine"}:
            result.setdefault("@observation", set()).update(table["observation_id"].to_pylist())
    return result, exclusive


def groups(items, *, max_days=7, max_rows=250000, max_bytes=256 * 1024**2):
    if min(max_days, max_rows, max_bytes) < 1:
        raise ValueError("Positive batch bounds required")
    pending, seen, rows, size = [], {}, 0, 0
    for item in items:
        if item.get("existing") is not None:
            if pending:
                yield pending
                pending, seen, rows, size = [], {}, 0, 0
            yield [item]
            continue
        tables = item["tables"]
        incoming, exclusive = keys(tables)
        day = date.fromisoformat(item["report"]["days"][0]["date"])
        added_rows = sum(len(t) for t in tables.values())
        added_size = sum(t.nbytes for t in tables.values())
        if pending:
            first = date.fromisoformat(pending[0]["report"]["days"][0]["date"])
            if (
                exclusive
                or (day - first).days >= max_days
                or day < first
                or (first <= date(2024, 12, 31) < day)
                or rows + added_rows > max_rows
                or size + added_size > max_bytes
                or any(seen.get(k, set()) & v for k, v in incoming.items())
            ):
                yield pending
                pending, seen, rows, size = [], {}, 0, 0
        pending.append(item)
        rows += added_rows
        size += added_size
        for name, values in incoming.items():
            seen.setdefault(name, set()).update(values)
        # A larger-than-cap original cohort is not split/reinterpreted. It uses
        # exactly the original singleton path under the same external RAM cap.
        if exclusive or max_days == 1 or rows >= max_rows or size >= max_bytes:
            yield pending
            pending, seen, rows, size = [], {}, 0, 0
    if pending:
        yield pending


def items(paths, output, publication, read_tables):
    for checkpoint in paths:
        key = checkpoint.parent.parent.name + "/" + checkpoint.parent.name
        accepted = json.loads(checkpoint.read_text())
        verify_checkpoint(checkpoint, accepted["identity"])
        report = json.loads((checkpoint.parent / "canonical-report.json").read_text())
        validate_report(report, null_policy=True)
        if report["status"] != accepted["status"]:
            raise ValueError("Source report/checkpoint status mismatch")
        ack = output / "days" / key / "published.json"
        identity = digest(
            canonical({"checkpoint": file_hash(checkpoint), "publication": publication}).encode()
        )
        existing = json.loads(ack.read_text()) if ack.exists() else None
        if existing is not None and existing["identity"] != identity:
            raise ValueError("Published day identity differs")
        yield {
            "checkpoint": checkpoint,
            "key": key,
            "report": report,
            "ack": ack,
            "identity": identity,
            "existing": existing,
            "tables": {} if existing is not None else read_tables(report),
        }


def replay(
    paths,
    output,
    publication,
    connection,
    catalog,
    read_tables,
    writer,
    *,
    max_days=1,
    max_rows=250000,
    max_bytes=256 * 1024**2,
    before_group=lambda _: None,
):
    namespace = publication["namespace"]
    limits = {"max_days": max_days, "max_rows": max_rows, "max_bytes": max_bytes}
    for group in groups(items(paths, output, publication, read_tables), **limits):
        before_group(group)
        first = group[0]
        if first["existing"] is not None:
            published = first["existing"]["report"]
            for table, pin in published["days"][0]["iceberg"].items():
                meta = catalog.table(namespace, table)["metadata"]
                if meta["table-uuid"] != pin["table_uuid"]:
                    raise ValueError("Historical source identity changed")
                if pin["snapshot_id"] is not None and pin["snapshot_id"] not in {
                    s["snapshot-id"] for s in meta.get("snapshots", [])
                }:
                    raise ValueError("Historical source pin expired")
            yield first["checkpoint"], published
            continue
        combined = {}
        for item in group:
            for name, table in item["tables"].items():
                combined.setdefault(name, []).append(table)
        combined = {name: pa.concat_tables(tables) for name, tables in combined.items()}
        batch = None
        if max_days > 1:
            plan = {
                "version": 1,
                "limits": limits,
                "members": [{"key": i["key"], "identity": i["identity"]} for i in group],
            }
            bid = digest(canonical(plan).encode())
            destination = output / "batches" / (bid + ".json")
            if destination.exists() and json.loads(destination.read_text()) != plan:
                raise ValueError("Conflicting batch lineage")
            save(destination, plan)
            batch = {
                "id": bid,
                "cohorts": len(group),
                "first": first["key"],
                "last": group[-1]["key"],
                "semantics": "co-published shared snapshot, not individual day snapshot",
            }
        snapshots = (
            writer(
                connection,
                catalog,
                namespace,
                combined,
                partition_date=first["report"]["days"][0]["date"] if len(group) == 1 else None,
            )
            if combined
            else {}
        )
        # No day is acknowledged until ALL tables in the group have committed.
        # A crash during table writes or ACKs replays unacknowledged inputs using
        # the original idempotent writer; successful earlier ACKs remain retained.
        for item in group:
            report = item["report"]
            resource = report["resource"]
            entry = report["days"][0]
            pins = {n: p for n, p in snapshots.items() if n in item["tables"]}
            if batch is not None:
                entry = entry | {"publication_batch": batch}
            completed = complete_day_publication(
                entry | {"iceberg": pins},
                resource=resource,
                namespace=namespace,
                catalog=catalog,
            )
            published = report | {
                "run_id": publication["run_id"] + "_" + resource,
                "iceberg_enabled": True,
                "iceberg_namespace": namespace,
                "days": [completed],
            }
            save(item["ack"], {"identity": item["identity"], "report": published})
            yield item["checkpoint"], published
