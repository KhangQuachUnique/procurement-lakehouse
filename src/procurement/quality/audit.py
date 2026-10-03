"""Frozen committed selections, shared record assessment and resumable quality reports."""

import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.contracts import (
    TABLES,
    WORKFLOW_FIELDS,
    DetailValidationError,
    cohort_warnings,
    finish,
    issue,
    resolve_route,
    validate_detail,
)
from procurement.quality.files import now, read_json, safe_error, write_json
from procurement.quality.storage import read_quality_contexts
from procurement.storage.committed import CommittedDay, iter_committed_records


def frozen_selection(fs, resource, start, end):
    definition = get_resource(resource)
    result = []
    for day in read_coverage(fs, definition.identity, start, end):
        item = {"resource": resource, "date": str(day.source_date), "run_id": None,
                "expected_records": 0, "files": []}
        if day.effective:
            item.update(run_id=day.effective.run_id, expected_records=day.effective.bronze_records)
            for table in definition.tables:
                prefix = (f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{definition.identity.source}/"
                          f"{table}/source_date={day.source_date}/run_id={day.effective.run_id}")
                item["files"].extend([table, key] for key in sorted(fs.glob(f"{prefix}/*.parquet")))
        result.append(item)
    return result


def selection_day(selection):
    return CommittedDay(date.fromisoformat(selection["date"]), selection["run_id"],
                        selection["expected_records"], tuple(tuple(f) for f in selection["files"]))


def record_key(record):
    return record["source_id"], record.get("source_version")


def context_index(items):
    indexed = defaultdict(dict)
    for item in items:
        key = item.get("notifyNo"), item.get("notifyVersion")
        indexed[key][calculate_content_hash(item)] = item
    return indexed


def find_context(record, primary, fallback):
    key = record_key(record)
    matches = list(primary.get(key, {}).values()) or list(fallback.get(key, {}).values())
    if len(matches) != 1:
        return None, "ambiguous_search_context" if matches else "missing_search_context"
    return matches[0], None


def assess_record(table, record, context, config, *, context_error=None):
    payload = record["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    expected = dict(context or {})
    # Envelope values can be compared offline, but never substitute a missing request ID.
    expected.setdefault("notifyNo", record["source_id"])
    expected.setdefault("notifyVersion", record.get("source_version"))
    route, route_error = None, None
    try:
        route = resolve_route(expected, config)
    except DetailValidationError as exc:
        route_error = str(exc)
    kind = route.contract if route else next((k for k, v in TABLES.items() if v == table), None)
    if kind not in config.contracts:
        result = finish({"issues": [issue("unknown_contract", "unresolved")],
                         "metrics": {}, "identity": {}})
    else:
        result = validate_detail(payload, expected, config.contracts[kind],
                                 thresholds=config.thresholds)
    if route_error:
        result["issues"].append(issue(route_error, "unresolved"))
    if context_error:
        result["issues"].append(issue(context_error, "unresolved"))
    if route and TABLES[kind] != table:
        result["issues"].append(issue("wrong_detail_table", "fail", expected=TABLES[kind],
                                       actual=table))
    if calculate_content_hash(payload) != record["content_hash"]:
        result["issues"].append(issue("content_hash_mismatch", "fail"))
    finish(result)
    return {
        "source_id": record["source_id"], "source_version": record.get("source_version"),
        "content_hash": record["content_hash"], "table": table,
        "contract": kind, "context": context,
        "workflow": {key: expected.get(key) for key in WORKFLOW_FIELDS},
        "route": route.model_dump() if route else None, "result": result,
    }


def audit_day(fs, selection, config, search_items):
    if not selection["run_id"]:
        return {"selection": selection, "status": "missing", "rows": []}
    definition = get_resource(selection["resource"])
    sidecars = context_index(read_quality_contexts(
        fs, definition.identity, selection["run_id"], selection["date"],
    ))
    search = context_index(search_items)
    rows = []
    seen = set()
    report = {"selection": selection, "status": "complete", "rows": rows}
    try:
        for table, record in iter_committed_records(fs, (selection_day(selection),), verify_hash=True):
            context, error = find_context(record, sidecars, search)
            row = assess_record(table, record, context, config, context_error=error)
            row.update(date=selection["date"], run_id=selection["run_id"],
                       resource=selection["resource"])
            if record_key(record) in seen:
                row["result"]["issues"].append(issue("duplicate_identity", "fail"))
                finish(row["result"])
                report["status"] = "integrity_failed"
            seen.add(record_key(record))
            rows.append(row)
    except (ValueError, KeyError, TypeError) as exc:
        report.update(status="integrity_failed", error=safe_error(exc))
    return report


def run_audit(fs, *, resource, year, config, output, search_days=None, snapshot_hash=None, resume=False):
    output = Path(output)
    state_path = output / "selection.json"
    namespace = calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])
    metadata = {"schema_version": 1, "resource": resource, "year": year,
                "config_hash": config.fingerprint, "snapshot_hash": snapshot_hash,
                "storage_namespace": namespace}
    if state_path.exists() and not resume:
        raise ValueError("Audit output exists; use --resume")
    if resume:
        state = read_json(state_path)
        if any(state.get(key) != value for key, value in metadata.items()):
            raise ValueError("Audit config, snapshot or storage changed; start a new report")
    else:
        state = {**metadata, "created_at": now(), "status": "running", "completed": {},
                 "selection": frozen_selection(fs, resource, date(year, 1, 1), date(year, 12, 31))}
        write_json(state_path, state)
    state["status"] = "running"
    write_json(state_path, state)
    reports = []
    try:
        for selection in state["selection"]:
            key = selection["date"]
            checkpoint = output / "days" / f"{key}.json"
            if key in state["completed"]:
                day = read_json(checkpoint)
                if calculate_content_hash(day) != state["completed"][key]:
                    raise ValueError(f"Audit checkpoint changed: {key}")
            else:
                day = audit_day(fs, selection, config, (search_days or {}).get(key, []))
                write_json(checkpoint, day)
                state["completed"][key] = calculate_content_hash(day)
                write_json(state_path, state)
            reports.append(day)
            print(f"quality {key}: {day['status']} rows={len(day['rows'])}", flush=True)
    except BaseException:
        state["status"] = "incomplete"
        write_json(state_path, state)
        raise
    rows = [row for day in reports for row in day["rows"]]
    counts = cohort_warnings(rows, config.thresholds)
    workflows = {}
    patterns = {}
    for row in rows:
        key = calculate_content_hash({"contract": row["contract"], "workflow": row["workflow"]})
        group = workflows.setdefault(key, {"contract": row["contract"], "workflow": row["workflow"],
                                           "records": 0, "status": Counter()})
        group["records"] += 1
        group["status"][row["result"]["status"]] += 1
        metrics = row["result"]["metrics"]
        if metrics:
            pattern_key = calculate_content_hash([key, metrics["shape_hash"], metrics["null_pattern_hash"]])
            pattern = patterns.setdefault(pattern_key, {
                "workflow_key": key, "shape_hash": metrics["shape_hash"],
                "null_pattern_hash": metrics["null_pattern_hash"], "records": 0, "samples": [],
            })
            pattern["records"] += 1
            if len(pattern["samples"]) < 5:
                pattern["samples"].append({"source_id": row["source_id"], "date": row["date"]})
    summary = {
        **metadata, "status": "complete", "completed_at": now(), "records": len(rows),
        "quality_status": dict(counts),
        "days": dict(Counter(day["status"] for day in reports)),
        "tables": dict(Counter(row["table"] for row in rows)),
        "rules": dict(Counter(i["code"] for row in rows for i in row["result"]["issues"])),
        "rule_records": dict(Counter(code for row in rows for code in {
            finding["code"] for finding in row["result"]["issues"]
        })),
        "day_quality": {day["selection"]["date"]: {
            "status": day["status"], "records": len(day["rows"]),
            "quality_status": dict(Counter(row["result"]["status"] for row in day["rows"])),
        } for day in reports},
        "affected_dates": sorted({row["date"] for row in rows if row["result"]["status"] != "pass"}),
        "workflow_count": len({calculate_content_hash(row["workflow"]) for row in rows}),
        "workflows": workflows,
        "fully_verified": all(day["status"] == "complete" for day in reports)
                          and not counts["fail"] and not counts["unresolved"],
    }
    temporary = output / "records.jsonl.tmp"
    with temporary.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    temporary.replace(output / "records.jsonl")
    findings = output / "findings.jsonl.tmp"
    with findings.open("w", encoding="utf-8") as stream:
        for row in rows:
            if row["result"]["status"] != "pass":
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    findings.replace(output / "findings.jsonl")
    write_json(output / "patterns.json", patterns)
    write_json(output / "summary.json", summary)
    (output / "summary.md").write_text(
        f"# Bronze quality {resource} {year}\n\n"
        f"Records: {len(rows)}. Status: {dict(counts)}.\n\n"
        f"Days: {summary['days']}. Fully verified: {summary['fully_verified']}.\n\n"
        "Warnings are not confirmed errors. Missing days/context are not passes.\n\n"
        "| Rule | Records |\n| --- | ---: |\n" +
        "".join(f"| {code} | {count} |\n" for code, count in summary["rule_records"].items()) +
        "\n| Table | Records |\n| --- | ---: |\n" +
        "".join(f"| {table} | {count} |\n" for table, count in summary["tables"].items()) +
        "\n## Incomplete or damaged days\n\n" +
        "".join(f"- {day['selection']['date']}: {day['status']}\n"
                for day in reports if day["status"] != "complete"),
        encoding="utf-8",
    )
    state["status"] = "complete"
    write_json(state_path, state)
    return summary
