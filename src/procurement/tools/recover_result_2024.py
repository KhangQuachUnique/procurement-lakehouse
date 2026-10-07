"""One-off recovery of three September 2024 result days; normal ingestion stays strict."""

import argparse
import json
import logging
import signal
from dataclasses import replace
from datetime import date
from pathlib import Path
from uuid import uuid4

from procurement.common.catalog import get_resource
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.ingestion.batch_runner import run_batch_range
from procurement.ingestion.engine.daily_runner import run_daily_resource
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.contractor_result.resource import (
    create_contractor_result_spec,
)
from procurement.jobs.ingest import _parser as ingest_parser
from procurement.jobs.ingest import execute_flow
from procurement.jobs.lock import execution_lock
from procurement.quality.files import cli, safe_error, write_json
from procurement.quality.storage import quality_prefix
from procurement.storage.io import read_json as read_storage_json
from procurement.storage.io import write_json as write_storage_json
from procurement.storage.object_store import create_s3_filesystem

logger = logging.getLogger(__name__)
DAYS = (date(2024, 9, 9), date(2024, 9, 12), date(2024, 9, 16))
APPROVED = {
    (date(2024, 9, 9), "IB2400333221"),
    (date(2024, 9, 12), "IB2400340694"),
    (date(2024, 9, 16), "IB2400347507"),
}
POLICY = {
    "job": "recover_result_2024", "resource": "contractor_result", "http_status": 500,
    "approved": [{"date": str(day), "notify_no": number} for day, number in sorted(APPROVED)],
    "reason": "Operator checked the website and accepted missing details for these exact notices when HTTP 500 persists.",
}


def recovery_spec(client, fs, directory, job_id):
    """Adapt extraction only for this job; evidence must be persisted before page commit."""
    base = create_contractor_result_spec(client)
    policy_hash = calculate_content_hash(POLICY)

    def records(*, search_items, run_id, source_date, search_page, errors, stats):
        if source_date not in DAYS:
            raise ValueError("Recovery is restricted to the three approved dates")
        observed_errors = []
        rows = list(base.records(search_items=search_items, run_id=run_id, source_date=source_date,
                                 search_page=search_page, errors=observed_errors, stats=stats))
        excluded = []
        for error in observed_errors:
            contexts = [item for item in search_items if item.get("notifyNo") == error.source_id]
            accepted = (
                (source_date, error.source_id) in APPROVED and error.http_status == 500
                and error.error_type == "HTTPStatusError" and error.stage == "result_detail"
                and len(contexts) == 1 and bool(contexts[0].get("inputResultId"))
            )
            if not accepted:
                errors.append(error)
                continue
            context = {key: contexts[0].get(key) for key in ("id", "notifyNo", "inputResultId")}
            evidence = {"kind": "source_exclusion", "context": context,
                        "source_date": str(source_date), "page_number": search_page,
                        "reason": POLICY["reason"], "policy_hash": policy_hash,
                        "error": error.model_dump(mode="json")}
            excluded.append(evidence)
            stats.quality_observations.append(evidence)
            logger.warning("recovery_excluded date=%s notify_no=%s http_status=500", source_date, error.source_id)
        if not errors and len(rows) + len(excluded) != len(search_items):
            raise ValueError("Recovery search/record/exclusion counts do not reconcile")
        evidence = {
            "schema_version": 1, "job_id": job_id, "run_id": run_id, "date": str(source_date),
            "page_number": search_page, "policy": POLICY, "policy_hash": policy_hash,
            "state": "observed_before_commit", "search_items": len(search_items),
            "collected_records": len(rows), "excluded_records": len(excluded),
            "unhandled_errors": [e.model_dump(mode="json") for e in errors], "excluded": excluded,
        }
        # Quality sidecars follow the selected run and are copied by offline compaction.
        key = f"{quality_prefix(base.identity, run_id, source_date)}/recovery-{search_page:06d}.json"
        write_storage_json(fs, key, evidence)
        if read_storage_json(fs, key) != evidence:
            raise ValueError("Recovery evidence readback mismatch")
        write_json(Path(directory) / "runs" / run_id / f"page-{search_page:06d}.json", evidence)
        yield from rows

    return replace(base, iter_records=records, quality_config_hash=policy_hash)


def runner(fs, directory, job_id):
    def run_day(resource, source_date, *, page_size, request_budget, **_):
        if resource != "contractor_result" or source_date not in DAYS:
            raise ValueError("Recovery is restricted to contractor_result on three fixed dates")
        with MuasamcongClient(token=settings.MUASAMCONG_TOKEN,
                             max_attempts=settings.MUASAMCONG_MAX_ATTEMPTS,
                             max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
                             request_budget=request_budget) as client:
            spec = recovery_spec(client, fs, directory, job_id)
            return run_batch_range(
                source_date, source_date, fs=fs, identity=spec.identity, heartbeat=True,
                run_day=lambda run_id, day: run_daily_resource(
                    fs=fs, spec=spec, run_id=run_id, source_date=day, page_size=page_size,
                    check_cancelled=request_budget.check_cancelled))
    return run_day


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-request-interval", type=float, default=0.075)
    parser.add_argument("--source-max-inflight", type=int, default=1)
    parser.add_argument("--retry-stale", action="store_true",
                        help="Confirm previous workers stopped; live heartbeats still block recovery")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("exports/result-recovery-2024"))
    parser.add_argument("--lock-dir", type=Path, default=Path(settings.INGESTION_LOCK_DIR))
    args = parser.parse_args(argv)
    # Reuse validation, active-worker handling, committed verification and stop policy.
    from procurement.common.dates import today_vn
    from procurement.jobs.ingest import resolve_dates
    flows = []
    for day in DAYS:
        flags = ["backfill", "--resource", "contractor_result", "--start-date", str(day),
                 "--end-date", str(day), "--source-request-interval", str(args.source_request_interval),
                 "--source-max-inflight", str(args.source_max_inflight)]
        flags += [flag for flag, enabled in (("--retry-stale", args.retry_stale),
                  ("--continue-on-error", args.continue_on_error), ("--dry-run", args.dry_run)) if enabled]
        flow = ingest_parser().parse_args(flags)
        resolve_dates(flow, today=today_vn())
        flows.append(flow)
    configure_logging()
    job_id = uuid4().hex
    directory = args.output_dir / job_id
    report = {"job_id": job_id, "policy": POLICY, "dry_run": args.dry_run, "days": [], "complete": False}
    fs = create_s3_filesystem()
    run_day = runner(fs, directory, job_id)
    def terminate(*_):
        raise KeyboardInterrupt("Termination requested")
    previous_handler = signal.signal(signal.SIGTERM, terminate)
    try:
        with execution_lock(args.lock_dir):
            for flow in flows:
                result, code = execute_flow(flow, fs=fs, run_day=run_day)
                report["days"].append(result)
                for group in result["resources"]:
                    for entry in group["dates"]:
                        entry["recorded_exclusions"] = []
                        if entry["effective_run_id"]:
                            prefix = quality_prefix(get_resource("contractor_result").identity,
                                                    entry["effective_run_id"], entry["date"])
                            for key in sorted(fs.glob(f"{prefix}/recovery-*.json")):
                                entry["recorded_exclusions"].extend(read_storage_json(fs, key)["excluded"])
                write_json(directory / "report.json", report)
                if code and (not args.continue_on_error or not result["execution_errors"]
                             or any(e.get("reason") != "source_failure" for e in result["execution_errors"])):
                    break
        report["complete"] = (len(report["days"]) == len(DAYS) and all(
            not result["execution_errors"] and all(
                entry["status"] == "success" for group in result["resources"] for entry in group["dates"])
            for result in report["days"]))
    except BaseException as exc:
        report["error"] = safe_error(exc)
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
        report["recorded_excluded_records"] = sum(
            len(entry.get("recorded_exclusions", [])) for result in report["days"]
            for group in result["resources"] for entry in group["dates"])
        write_json(directory / "report.json", report)
    print(json.dumps({"complete": report["complete"], "dry_run": args.dry_run,
                      "recorded_excluded_records": report["recorded_excluded_records"],
                      "report": str(directory / "report.json")}, ensure_ascii=False))
    return 0 if args.dry_run or report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(cli(main))
