"""Readiness with proven source exceptions: null-policy v3, historical v1/v2 retained."""

from datetime import UTC, date, datetime, timedelta

from procurement.common.silver_gold_contract import INPUT_TABLES, PROJECTION_VERSION
from procurement.silver.application.readiness import (
    OWNED,
    RESOURCES,
    _ancestry,
    digest,
    seal_readiness,
)
from procurement.silver.application.resources import get_silver_resource
from procurement.source_quality import (
    APPROVAL_DIGEST,
    NULL_POLICY_DIGEST,
    verify_exception_report,
    verify_null_report,
)
from procurement.storage.iceberg import identifier


def validate_report(report, *, null_policy=False):
    name = report["resource"]
    if (
        name not in RESOURCES
        or report["mapping_version"] != get_silver_resource(name).mapping_version
    ):
        raise ValueError("Unknown resource/mapper")
    proofs = []
    for entry in report["days"]:
        q = entry["quality"]
        if any(
            type(q.get(k)) is not int or q[k] < 0
            for k in ("accepted", "quarantined", "blocking_issues")
        ):
            raise ValueError("Invalid quality counters")
        if entry["status"] == "completed" and not q["quarantined"] and not q["blocking_issues"]:
            continue
        single = report | {"days": [entry]}
        if null_policy:
            proofs.extend(verify_null_report(single))
        else:
            proofs.append(verify_exception_report(single))
    return proofs


def seal_with_exception(
    reports, *, start, end, namespace, catalog, count_rows, completed_at=None, null_policy=False
):
    if not null_policy and not any(
        e["quality"]["quarantined"] or e["quality"]["blocking_issues"]
        for r in reports
        for e in r["days"]
    ):
        return seal_readiness(
            reports,
            start=start,
            end=end,
            namespace=namespace,
            catalog=catalog,
            count_rows=count_rows,
            completed_at=completed_at,
        )
    identifier(namespace)
    if start > end:
        raise ValueError("Inverted source range")
    selected = {}
    versions = {}
    exceptions = []
    for report in reports:
        name = report["resource"]
        if (
            name not in RESOURCES
            or report.get("family") != "all"
            or not report.get("iceberg_enabled")
        ):
            raise ValueError("Incomplete resource publication")
        proofs = validate_report(report, null_policy=null_policy)
        versions[name] = report["mapping_version"]
        for entry in report["days"]:
            day = date.fromisoformat(entry["date"])
            if not start <= day <= end:
                continue
            if (name, day) in selected:
                raise ValueError("Duplicate resource/day")
            if not entry.get("input_run_id") or not isinstance(entry.get("input_files"), list):
                raise ValueError("Missing Bronze lineage")
            if entry["quality"]["accepted"] and not entry["input_files"]:
                raise ValueError("Missing nonempty source files")
            if any(
                not f.get("table") or not f.get("key") or not f.get("sha256")
                for f in entry["input_files"]
            ):
                raise ValueError("Incomplete source lineage")
            for table in (*OWNED[name], "observation"):
                if (
                    table not in entry.get("iceberg", {})
                    or "snapshot_id" not in entry["iceberg"][table]
                ):
                    raise ValueError("Missing explicit table publication")
            if entry["status"] == "needs_review":
                exceptions.extend(p for p in proofs if p.source_date == entry["date"])
            selected[name, day] = {
                "run_id": report["run_id"],
                "resource": name,
                "mapping_version": report["mapping_version"],
                "entry": entry,
            }
    if not null_policy and len(exceptions) != 1:
        raise ValueError("Exactly one approved source exception required")
    day = start
    while day <= end:
        if any((r, day) not in selected for r in RESOURCES):
            raise ValueError("Missing covered source day")
        day += timedelta(days=1)
    tables = []
    vector = {}
    for table in INPUT_TABLES:
        meta = catalog.table(namespace, table)["metadata"]
        uuid = meta.get("table-uuid")
        current = meta.get("current-snapshot-id")
        current = current if current is not None and current >= 0 else None
        if not uuid:
            raise ValueError("Missing source identity")
        for item in selected.values():
            old = item["entry"]["iceberg"].get(table)
            if old is not None and (
                old.get("table_uuid") != uuid or not _ancestry(meta, old["snapshot_id"], current)
            ):
                raise ValueError("Unretained/mismatched publication ancestry")
        rows = count_rows(table, current)
        if type(rows) is not int or rows < 0 or current is None and rows:
            raise ValueError("Invalid source count")
        tables.append({"table": table, "table_uuid": uuid, "snapshot_id": current, "rows": rows})
        vector[table] = (uuid, current)
    for table, expected in vector.items():
        meta = catalog.table(namespace, table)["metadata"]
        s = meta.get("current-snapshot-id")
        s = s if s is not None and s >= 0 else None
        if (meta.get("table-uuid"), s) != expected:
            raise ValueError("Source drift during sealing")
    at = completed_at or datetime.now(UTC)
    if at.utcoffset() is None:
        raise ValueError("Completion time needs timezone")
    evidence = [selected[k] for k in sorted(selected)]
    receipt = {
        "receipt_version": 3 if null_policy else 2,
        "cohort_id": digest(evidence),
        "namespace": namespace,
        "mapping_version": digest(versions),
        "projection_version": PROJECTION_VERSION,
        "completed_at": at.isoformat(),
        "source_start": str(start),
        "source_end": str(end),
        "source_complete": True,
        "quarantined": len(exceptions),
        "blocking_issues": len(exceptions),
        "tables": tables,
        "approved_exceptions": [e.model_dump() for e in exceptions],
        "approval_digest": NULL_POLICY_DIGEST if null_policy else APPROVAL_DIGEST,
        "quality_status": "ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION" if exceptions else "STRICT",
    }
    receipt["input_id"] = digest({k: v for k, v in receipt.items() if k != "completed_at"})
    return {"receipt": receipt, "evidence": evidence, "mapping_versions": versions}


def seal_with_null_policy(reports, **kwargs):
    return seal_with_exception(reports, null_policy=True, **kwargs)
