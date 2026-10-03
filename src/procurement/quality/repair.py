"""Selective refetch, full-day reconstruction, immutable-attempt publication."""

import json
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.daily_runner import _create_pipeline
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.models import ResourceSpec
from procurement.jobs.lock import execution_lock
from procurement.models.bronze import BronzeRecord
from procurement.models.control import (
    DayManifest,
    DayStatus,
    PageManifest,
    PageStatus,
    RunManifest,
    RunStatus,
)
from procurement.quality.audit import assess_record, record_key, selection_day
from procurement.quality.contracts import ENDPOINTS, TABLES, resolve_route, validate_detail
from procurement.quality.coverage import CoverageGuard
from procurement.quality.files import now, read_json, safe_error, write_json
from procurement.quality.storage import quality_prefix, save_quality_page
from procurement.storage.bronze import DltBronzeWriter
from procurement.storage.committed import CommittedDay, iter_committed_records
from procurement.storage.control import (
    DayCommitUncertainError,
    commit_day_manifest,
    read_day_manifest,
    read_run_manifest,
    write_day_manifest,
    write_page_manifest,
    write_run_manifest,
)
from procurement.storage.execution import ExecutionHeartbeat
from procurement.storage.io import write_json as write_storage_json


def create_plan(audit_directory, config, output):
    directory = Path(audit_directory)
    state = read_json(directory / "selection.json")
    if state["status"] != "complete" or state["config_hash"] != config.fingerprint:
        raise ValueError("Repair requires a complete audit using the same reviewed config")
    if state["resource"] != "notify_contractor":
        raise ValueError("Selective repair currently supports notify_contractor only")
    plan = {"schema_version": 1, "resource": state["resource"], "year": state["year"],
            "config_hash": config.fingerprint, "storage_namespace": state["storage_namespace"],
            "created_at": now(), "days": [], "blocked": []}
    for selection in state["selection"]:
        day = read_json(directory / "days" / f"{selection['date']}.json")
        if calculate_content_hash(day) != state["completed"][selection["date"]]:
            raise ValueError("Audit checkpoint changed")
        if day["status"] != "complete":
            plan["blocked"].append({"date": selection["date"], "reason": day["status"]})
            continue
        targets = [row for row in day["rows"] if row["result"]["status"] == "fail"]
        if not targets:
            continue
        if any(row["route"] is None or row["context"] is None
               or row["result"]["status"] == "unresolved" for row in day["rows"]):
            plan["blocked"].append({"date": selection["date"], "reason": "unresolved_context_or_route"})
            continue
        plan["days"].append({"selection": selection, "records": [
            {key: row[key] for key in ("source_id", "source_version", "table", "content_hash",
                                      "context", "route")} | {
                "refetch": row in targets,
                "confirmed_errors": [i["code"] for i in row["result"]["issues"]
                                     if i["severity"] == "fail"],
            } for row in day["rows"]
        ]})
    plan["estimated_detail_requests"] = sum(row["refetch"] for d in plan["days"] for row in d["records"])
    plan["plan_hash"] = calculate_content_hash(plan)
    if Path(output).exists():
        raise ValueError("Repair plan exists; choose another output")
    write_json(output, plan)
    return plan


def validate_plan(plan, config):
    expected_hash = calculate_content_hash({key: value for key, value in plan.items() if key != "plan_hash"})
    if plan.get("plan_hash") != expected_hash or plan.get("config_hash") != config.fingerprint:
        raise ValueError("Repair plan or config changed")
    namespace = calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])
    if plan.get("storage_namespace") != namespace or plan.get("resource") != "notify_contractor":
        raise ValueError("Repair storage/resource mismatch")
    dates = set()
    for day in plan["days"]:
        selection = day["selection"]
        if selection["date"] in dates or selection["resource"] != plan["resource"]:
            raise ValueError("Duplicate day or mismatched resource in plan")
        dates.add(selection["date"])
        for row in day["records"]:
            route = resolve_route(row["context"], config)
            if route.model_dump() != row["route"]:
                raise ValueError("Repair routing no longer matches reviewed plan")


def assert_baseline(fs, identity, selection, coverage_reader=None):
    day = date.fromisoformat(selection["date"])
    coverage = coverage_reader(day) if coverage_reader else read_coverage(fs, identity, day, day)[0]
    if coverage.active_run_ids:
        raise ValueError(f"Another active run exists for the repair date: {coverage.active_run_ids}")
    if coverage.effective is None or coverage.effective.run_id != selection["run_id"]:
        raise ValueError("Effective baseline changed; create a new audit/plan")


def read_baseline(fs, selection, records):
    expected = {record_key(row): row for row in records}
    if len(expected) != len(records) or len(records) != selection["expected_records"]:
        raise ValueError("Duplicate identities or inconsistent baseline count")
    baseline = list(iter_committed_records(fs, (selection_day(selection),), verify_hash=True))
    seen = set()
    for table, record in baseline:
        key = record_key(record)
        reference = expected.get(key)
        if (key in seen or reference is None or reference["table"] != table
                or reference["content_hash"] != record["content_hash"]):
            raise ValueError("Baseline identity/table/hash changed")
        seen.add(key)
    if seen != set(expected):
        raise ValueError("Baseline identity set changed")
    return baseline


def fetch_replacement(client, row, config, cache):
    context = row["context"]
    route = resolve_route(context, config)
    cache_key = calculate_content_hash({"row": row, "config": config.fingerprint})
    path = Path(cache) / f"{cache_key}.json"
    if path.exists():
        saved = read_json(path)
        if saved["key"] != cache_key or calculate_content_hash(saved["payload"]) != saved["payload_hash"]:
            raise ValueError("Replacement checkpoint changed")
        payload = saved["payload"]
    else:
        try:
            payload = client.post(ENDPOINTS[route.contract], {"id": context["id"]})
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            exc.diagnostics = {**getattr(exc, "diagnostics", {}),
                               "endpoint": ENDPOINTS[route.contract], "request_id": context["id"],
                               "source_id": row["source_id"], "source_version": row["source_version"]}
            raise
    result = validate_detail(payload, context, config.contracts[route.contract], thresholds=config.thresholds)
    actual = result["identity"]
    if actual.get("notifyVersion") != row["source_version"]:
        raise ValueError("source_changed: replacement version differs from the baseline")
    if result["status"] not in {"pass", "warn"} or actual.get("notifyNo") != row["source_id"]:
        raise ValueError("Replacement failed detail contract/identity checks")
    if not path.exists():
        write_json(path, {"key": cache_key, "payload_hash": calculate_content_hash(payload),
                          "payload": payload, "context": context, "config_hash": config.fingerprint,
                          "observed_at": now()})
    return payload, result, route


def reconstruct(client, baseline, references, config, run_id, cache, *, detail_workers=1):
    planned = {record_key(row): row for row in references}
    replacements = {}
    targets = [row for row in references if row["refetch"]]
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="quality-detail") as pool:
        futures = {pool.submit(fetch_replacement, client, row, config, cache): record_key(row)
                   for row in targets}
        try:
            for future in as_completed(futures):
                replacements[futures[future]] = future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            raise
    tables, observations = defaultdict(list), []
    counts = Counter(copied=0, refetched=0, moved=0)
    for table, record in baseline:
        row = planned[record_key(record)]
        old_payload = record["payload"]
        if isinstance(old_payload, str):
            old_payload = json.loads(old_payload)
        payload, target = old_payload, table
        ingested_at = record["ingested_at"]
        if row["refetch"]:
            payload, result, route = replacements[record_key(row)]
            target = TABLES[route.contract]
            ingested_at = datetime.now(UTC)
            counts["refetched"] += 1
            counts["moved"] += target != table
        else:
            assessment = assess_record(table, record, row["context"], config)
            result = assessment["result"]
            if result["status"] not in {"pass", "warn"}:
                raise ValueError("Unselected baseline record fails validation; create a new plan")
            counts["copied"] += 1
        replacement = BronzeRecord(
            source_id=record["source_id"], source_version=record.get("source_version"),
            source_date=record["source_date"], run_id=run_id, ingested_at=ingested_at,
            content_hash=calculate_content_hash(payload), payload=payload,
        )
        tables[target].append(replacement)
        observations.append({"context": row["context"], "result": result,
                             "config_hash": config.fingerprint, "rule_version": config.version,
                             "contract": row["route"]["contract"],
                             "endpoint": ENDPOINTS[row["route"]["contract"]],
                             "baseline_run_id": record["run_id"], "baseline_hash": record["content_hash"],
                             "action": "refetch" if row["refetch"] else "copy"})
    return dict(tables), observations, dict(counts)


def verify_output(fs, identity, day, run_id, count, expected, config):
    files = []
    for table in get_resource(identity.resource).tables:
        prefix = (f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{identity.source}/{table}/"
                  f"source_date={day}/run_id={run_id}")
        files.extend((table, key) for key in sorted(fs.glob(f"{prefix}/*.parquet")))
    selection = CommittedDay(day, run_id, count, tuple(files))
    observed = set()
    for table, record in iter_committed_records(fs, (selection,), verify_hash=True):
        key = record_key(record)
        if key in observed or key not in expected:
            raise ValueError("Duplicate/unexpected repaired identity")
        observed.add(key)
        if expected[key].get("output_hash") != record["content_hash"]:
            raise ValueError("Written repair payload differs from validated replacement")
        result = assess_record(table, record, expected[key]["context"], config)["result"]
        if result["status"] not in {"pass", "warn"}:
            raise ValueError("Written repair record failed post-write validation")
    if observed != set(expected):
        raise ValueError("Repaired identity set changed")


def apply_day(fs, client, plan, planned_day, config, work, *, writer_factory=None,
              coverage_reader=None, detail_workers=1):
    selection = planned_day["selection"]
    identity = get_resource(plan["resource"]).identity
    day = date.fromisoformat(selection["date"])
    checkpoint = Path(work) / f"{day}.json"
    expected = {record_key(row): dict(row) for row in planned_day["records"]}
    if checkpoint.exists():
        previous = read_json(checkpoint)
        if previous["plan_hash"] != plan["plan_hash"]:
            raise ValueError("Repair checkpoint belongs to another plan")
        manifest = read_day_manifest(fs, identity, previous["run_id"], day)
        if manifest is not None and manifest.status is DayStatus.SUCCESS:
            for row in previous["outputs"]:
                expected[record_key(row)]["output_hash"] = row["output_hash"]
            verify_output(fs, identity, day, previous["run_id"], len(expected), expected, config)
            previous_run = read_run_manifest(fs, identity, previous["run_id"])
            if previous_run and previous_run.status is not RunStatus.SUCCESS:
                write_run_manifest(fs, identity, previous_run.model_copy(update={
                    "status": RunStatus.SUCCESS, "success_dates": 1, "failed_dates": 0,
                    "completed_at": datetime.now(UTC),
                }))
            previous["status"] = "success"
            write_json(checkpoint, previous)
            return previous
        if previous.get("status") in {"running", "commit_uncertain"}:
            raise ValueError("Previous repair worker/commit is unconfirmed; inspect before retry")
    coverage_reader = coverage_reader or CoverageGuard(fs, identity).read
    assert_baseline(fs, identity, selection, coverage_reader)
    baseline = read_baseline(fs, selection, planned_day["records"])
    run_id = uuid.uuid4().hex
    receipt = {"plan_hash": plan["plan_hash"], "run_id": run_id, "date": str(day), "status": "running"}
    write_json(checkpoint, receipt)
    started = datetime.now(UTC)
    run = RunManifest(run_id=run_id, source=identity.source, resource=identity.resource,
                      start_date=day, end_date=day, total_dates=1,
                      status=RunStatus.RUNNING, started_at=started)
    manifest = DayManifest(run_id=run_id, source=identity.source, resource=identity.resource,
                          source_date=day, status=DayStatus.RUNNING, started_at=started)
    write_run_manifest(fs, identity, run)
    write_day_manifest(fs, identity, manifest)
    stage = "fetch_replacements"
    try:
        with ExecutionHeartbeat(fs, identity, run_id):
            tables, observations, counts = reconstruct(
                client, baseline, planned_day["records"], config, run_id,
                Path(work) / "replacements",
                detail_workers=detail_workers,
            )
            # Our own RUNNING attempt is excluded; other attempts block publication.
            stage = "verify_baseline"
            coverage = coverage_reader(day)
            if (coverage.effective is None or coverage.effective.run_id != selection["run_id"]
                    or set(coverage.active_run_ids) - {run_id}):
                raise ValueError("Baseline changed or another worker started")
            read_baseline(fs, selection, planned_day["records"])
            spec = ResourceSpec(identity, "quality_repair", identity.source,
                                lambda **_: {}, lambda **_: iter(()))
            writer = (writer_factory(run_id, day) if writer_factory else
                      DltBronzeWriter(lambda: _create_pipeline(spec, day, run_id)))
            stage = "write_bronze"
            count = writer.write_page(tables)
            if count != len(expected):
                raise ValueError("Repair writer count mismatch")
            stage = "write_quality_sidecar"
            save_quality_page(fs, identity, run_id, day, 0, config.fingerprint, observations)
            receipt.update(counts=counts, config_hash=config.fingerprint)
            receipt["outputs"] = []
            for records in tables.values():
                for record in records:
                    key = record.source_id, record.source_version
                    expected[key]["output_hash"] = record.content_hash
                    receipt["outputs"].append({"source_id": key[0], "source_version": key[1],
                                               "output_hash": record.content_hash})
            write_storage_json(fs, f"{quality_prefix(identity, run_id, day)}/repair.json", receipt)
            stage = "verify_output"
            verify_output(fs, identity, day, run_id, count, expected, config)
            write_page_manifest(fs, identity, PageManifest(
                run_id=run_id, source_date=day, page_number=0, page_size=max(1, count),
                search_items=count, bronze_records=count, status=PageStatus.SUCCESS,
                started_at=started, completed_at=datetime.now(UTC),
            ))
            # Verify again after writes, immediately before the commit marker.
            stage = "verify_baseline_before_commit"
            coverage = coverage_reader(day)
            if (coverage.effective is None or coverage.effective.run_id != selection["run_id"]
                    or set(coverage.active_run_ids) - {run_id}):
                raise ValueError("Baseline changed before commit")
            read_baseline(fs, selection, planned_day["records"])
            completed = manifest.model_copy(update={
                "status": DayStatus.SUCCESS, "bronze_records": count, "search_items": count,
                "expected_pages": 1, "completed_pages": 1, "completed_at": datetime.now(UTC),
            })
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            stage = "commit"
            commit_day_manifest(fs, identity, completed)
    except DayCommitUncertainError:
        raise  # never downgrade a commit whose acknowledgement was lost
    except BaseException as exc:
        # A failure after commit must never replace SUCCESS with FAILED.
        persisted = read_day_manifest(fs, identity, run_id, day)
        if persisted is not None and persisted.status is DayStatus.SUCCESS:
            raise
        write_day_manifest(fs, identity, manifest.model_copy(update={
            "status": DayStatus.FAILED, "error_count": 1, "completed_at": datetime.now(UTC),
        }))
        write_run_manifest(fs, identity, run.model_copy(update={
            "status": RunStatus.FAILED, "failed_dates": 1, "completed_at": datetime.now(UTC),
        }))
        receipt.update(status="failed", stage=stage, error=safe_error(exc))
        write_json(checkpoint, receipt)
        raise
    write_run_manifest(fs, identity, run.model_copy(update={
        "status": RunStatus.SUCCESS, "success_dates": 1, "completed_at": datetime.now(UTC),
    }))
    receipt["status"] = "success"
    write_json(checkpoint, receipt)
    return receipt


def apply_plan(fs, client, plan, config, work, *, limit=None, writer_factory=None,
               detail_workers=1, dates=None, continue_on_error=False):
    validate_plan(plan, config)
    if not 1 <= detail_workers <= 32:
        raise ValueError("detail_workers must be between 1 and 32")
    receipts = []
    selected = plan["days"]
    if dates is not None:
        requested = {str(day) for day in dates}
        available = {day["selection"]["date"] for day in selected}
        if requested - available:
            raise ValueError("Requested date is absent from the repair plan")
        selected = [day for day in selected if day["selection"]["date"] in requested]
    with execution_lock(Path(settings.INGESTION_LOCK_DIR)):
        guard = CoverageGuard(fs, get_resource(plan["resource"]).identity)
        progress = {}
        def compact(receipt):
            return {key: receipt[key] for key in ("date", "run_id", "status", "counts", "stage", "error")
                    if key in receipt}
        def save_progress(receipt):
            progress[receipt["date"]] = compact(receipt)
            completed = sum(item["status"] == "success" for item in progress.values())
            write_json(Path(work) / "summary.json", {
                "plan_hash": plan["plan_hash"], "days": [progress[key] for key in sorted(progress)],
                "planned_days": len(plan["days"]), "successful_days": completed,
                "updated_at": now(), "status": "complete" if completed == len(plan["days"]) else "partial",
            })
        for day in plan["days"]:
            path = Path(work) / f"{day['selection']['date']}.json"
            if path.exists():
                previous = read_json(path)
                if previous.get("plan_hash") != plan["plan_hash"]:
                    raise ValueError("Repair checkpoint belongs to another plan")
                progress[previous["date"]] = compact(previous)
        for index, day in enumerate(selected[:limit], 1):
            previous = progress.get(day["selection"]["date"], {})
            action = ("verifying committed attempt (no detail fetch)" if previous.get("status") == "success"
                      else "checking baseline and replacement cache")
            print(f"repair [{index}/{len(selected[:limit])}] {day['selection']['date']}: "
                  f"{action}", flush=True)
            try:
                receipt = apply_day(fs, client, plan, day, config, work,
                                    writer_factory=writer_factory, coverage_reader=guard.read,
                                    detail_workers=detail_workers)
            except BaseException as exc:
                receipt = {"date": day["selection"]["date"], "status": "blocked", "error": safe_error(exc)}
                path = Path(work) / f"{receipt['date']}.json"
                if path.exists():
                    saved = read_json(path)
                    receipt.update({key: saved[key] for key in ("run_id", "stage") if key in saved})
                    if saved.get("status") in {"commit_uncertain", "running", "failed"}:
                        receipt["status"] = saved["status"]
                save_progress(receipt)
                if not isinstance(exc, ValueError) or not continue_on_error:
                    raise
            receipts.append(compact(receipt))
            print(f"repair {day['selection']['date']}: {receipts[-1]['status']} "
                  f"{receipts[-1].get('counts', {})}", flush=True)
            save_progress(receipt)
    return receipts
