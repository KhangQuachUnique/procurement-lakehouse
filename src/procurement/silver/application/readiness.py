"""Seal coherent Silver inputs from completed resource/day publication evidence.

No Gold, Dagster or CLI imports. Does not infer coverage from nonzero table counts.
Receipt is valid only while its UUID/snapshot vector remains retained. Existing
unattested tables cannot be retroactively certified just by supplying date bounds.
"""

import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from procurement.common.silver_gold_contract import INPUT_TABLES, PROJECTION_VERSION
from procurement.silver.application.resources import get_silver_resource
from procurement.storage.iceberg import identifier

RESOURCES = ("khlcnt", "notify_contractor", "contractor_result")
OWNED = {
    "khlcnt": ("plan_revision", "package_revision", "package_location"),
    "notify_contractor": ("notice_revision",),
    "contractor_result": ("result_revision", "award_lot", "award_contractor"),
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _normalized_resource(value):
    return get_silver_resource(value).resource


def validate_evidence(reports, start, end):
    if start > end:
        raise ValueError("Inverted source coverage")
    selected = {}
    versions = {}
    for report in reports:
        resource = _normalized_resource(report["resource"])
        if resource not in RESOURCES:
            continue
        expected = get_silver_resource(resource)
        if report.get("mapping_version") != expected.mapping_version:
            raise ValueError("Unaccepted Silver mapping version: " + resource)
        if report.get("family") != "all" or not report.get("iceberg_enabled"):
            raise ValueError("Readiness requires complete resource Iceberg publication")
        versions[resource] = report["mapping_version"]
        for entry in report.get("days", []):
            day = date.fromisoformat(entry["date"])
            if not start <= day <= end:
                continue
            key = (resource, day)
            if key in selected:
                raise ValueError("Ambiguous duplicate resource/day evidence: " + str(key))
            if entry.get("status") != "completed":
                raise ValueError("Incomplete/skipped/needs-review Silver day: " + str(key))
            quality = entry.get("quality", {})
            if any(
                not isinstance(quality.get(k), int)
                or isinstance(quality.get(k), bool)
                or quality[k] < 0
                for k in ("accepted", "quarantined", "blocking_issues")
            ):
                raise ValueError("Missing/invalid quality outcome")
            if quality["quarantined"] or quality["blocking_issues"]:
                raise ValueError("Unaccepted Silver quality")
            if not entry.get("input_run_id"):
                raise ValueError("Missing committed Bronze lineage")
            files = entry.get("input_files")
            if not isinstance(files, list) or (quality["accepted"] and not files):
                raise ValueError("Missing source-file lineage")
            if any(not f.get("key") or not f.get("sha256") or not f.get("table") for f in files):
                raise ValueError("Incomplete committed source-file lineage")
            snapshots = entry.get("iceberg")
            if not isinstance(snapshots, dict):
                raise ValueError("Missing explicit publication outcome")  # noqa: TRY004
            # Empty domain is allowed only with an explicit per-table outcome.
            # No rows in a file is not a receipt that an empty refresh was published.
            for table in (*OWNED[resource], "observation"):
                if table not in snapshots or "snapshot_id" not in snapshots[table]:
                    raise ValueError("Missing table publication outcome: " + table)
            selected[key] = {
                "run_id": report["run_id"],
                "resource": resource,
                "mapping_version": report["mapping_version"],
                "entry": entry,
            }
    day = start
    while day <= end:
        for resource in RESOURCES:
            if (resource, day) not in selected:
                raise ValueError("Missing accepted source day: " + resource + "/" + str(day))
        day += timedelta(days=1)
    return selected, versions


def _ancestry(metadata, ancestor, current):
    by_id = {s["snapshot-id"]: s for s in metadata.get("snapshots", [])}
    seen = set()
    while current is not None and current not in seen:
        if current == ancestor:
            return True
        seen.add(current)
        if current not in by_id:
            return False
        current = by_id[current].get("parent-snapshot-id")
    return ancestor is None and current is None


def seal_readiness(reports, *, start, end, namespace, catalog, count_rows, completed_at=None):
    """Read-only source barrier. Caller owns single-writer source publication fence.

    count_rows(table,snapshot_id) must read the exact pin, not live/latest rows.
    Older evidence must be on the retained current ancestry; expire/drop/recreate
    without an authoritative replacement coverage proof fails closed.
    """
    identifier(namespace)
    selected, versions = validate_evidence(reports, start, end)
    vector = {}
    tables = []
    for table in INPUT_TABLES:
        metadata = catalog.table(namespace, table)["metadata"]
        current = metadata.get("current-snapshot-id")
        current = current if current is not None and current >= 0 else None
        uuid = metadata.get("table-uuid")
        if not uuid:
            raise ValueError("Missing Silver table identity")
        for item in selected.values():
            old = item["entry"]["iceberg"].get(table)
            if old is not None:
                if old.get("table_uuid") != uuid:
                    raise ValueError("Publication evidence lacks/mismatches table UUID: " + table)
                if not _ancestry(metadata, old["snapshot_id"], current):
                    raise ValueError("Publication cohort not in retained source ancestry: " + table)
        rows = count_rows(table, current)
        if not isinstance(rows, int) or rows < 0 or (current is None and rows):
            raise ValueError("Invalid pinned row count")
        vector[table] = (uuid, current)
        tables.append({"table": table, "table_uuid": uuid, "snapshot_id": current, "rows": rows})
    # This does not create a cross-table Iceberg transaction. It detects source
    # drift under the caller's resource cohort completion/publication fence.
    for table, (uuid, snapshot) in vector.items():
        meta = catalog.table(namespace, table)["metadata"]
        current = meta.get("current-snapshot-id")
        current = current if current is not None and current >= 0 else None
        if (meta.get("table-uuid"), current) != (uuid, snapshot):
            raise ValueError("Silver changed during readiness sealing")
    evidence = [selected[k] for k in sorted(selected)]
    at = completed_at or datetime.now(UTC)
    if at.utcoffset() is None:
        raise ValueError("Readiness completion requires timezone")
    receipt = {
        "receipt_version": 1,
        "cohort_id": digest(evidence),
        "namespace": namespace,
        "mapping_version": digest(versions),
        "projection_version": PROJECTION_VERSION,
        "completed_at": at.isoformat(),
        "source_start": str(start),
        "source_end": str(end),
        "source_complete": True,
        "quarantined": 0,
        "blocking_issues": 0,
        "tables": tables,
    }
    # Observation time is metadata, not logical input identity.
    receipt["input_id"] = digest({k: v for k, v in receipt.items() if k != "completed_at"})
    return {"receipt": receipt, "evidence": evidence, "mapping_versions": versions}


def save_bundle(bundle, destination):
    """Atomic immutable receipt+evidence. Never overwrite a competing attestation."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical(bundle)
    if destination.exists():
        if destination.read_text() != raw:
            raise ValueError("Conflicting immutable readiness bundle")
        return
    temporary = destination.with_name(destination.name + "." + uuid4().hex + ".tmp")
    with temporary.open("x") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, destination)
    except FileExistsError:
        if destination.read_text() != raw:
            raise ValueError("Competing readiness evidence")
    finally:
        temporary.unlink()


def complete_day_publication(entry, *, resource, namespace, catalog):
    """Called by Silver writer after successful publish, before final run report.

    Annotates unchanged/empty tables with their observed vector, never overwrites
    a writer-returned snapshot with a newer concurrent writer's result.
    No table/data creation, deletion or source completeness inference occurs here.
    """
    resource = _normalized_resource(resource)
    if resource not in OWNED:
        return entry
    snapshots = dict(entry.get("iceberg") or {})
    for table in (*OWNED[resource], "observation"):
        metadata = catalog.table(namespace, table)["metadata"]
        snapshot = metadata.get("current-snapshot-id")
        snapshot = snapshot if snapshot is not None and snapshot >= 0 else None
        uuid = metadata.get("table-uuid")
        if not uuid:
            raise ValueError("Missing published table identity")
        old = snapshots.get(table)
        if old is not None and old.get("snapshot_id") != snapshot:
            raise ValueError("Concurrent Silver writer changed table during day completion")
        snapshots[table] = (
            old or {"snapshot_id": snapshot, "operation": "unchanged_empty_candidate"}
        ) | {"table_uuid": uuid}
    return entry | {"iceberg": snapshots}


def main():
    """Operator boundary: seal trusted local writer reports against live catalog."""
    import argparse

    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, nargs="+", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--namespace", default="silver")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reports = [json.loads(path.read_text()) for path in args.reports]
    # Reject bad/incomplete coverage before any connection or counting work.
    validate_evidence(reports, args.start, args.end)
    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as db, RestCatalog(config) as catalog:

        def count_rows(table, snapshot):
            if snapshot is None:
                return 0
            return db.execute(
                f"SELECT count(*) FROM lake.{identifier(args.namespace)}.{identifier(table)} AT (VERSION => {snapshot})"
            ).fetchone()[0]

        bundle = seal_readiness(
            reports,
            start=args.start,
            end=args.end,
            namespace=args.namespace,
            catalog=catalog,
            count_rows=count_rows,
        )
    save_bundle(bundle, args.output)
    print(
        canonical(
            {
                "ready": True,
                "input_id": bundle["receipt"]["input_id"],
                "tables": len(bundle["receipt"]["tables"]),
            }
        )
    )


if __name__ == "__main__":
    main()
