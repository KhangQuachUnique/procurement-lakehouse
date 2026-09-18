"""Operational entry point: plan -> execute -> check coverage -> verify committed data."""

import argparse
import json
import signal
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from procurement.common.catalog import SUPPORTED_RESOURCES, get_resource
from procurement.common.dates import today_vn, validate_closed_range, validate_page_size
from procurement.common.errors import sanitize_error_message
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.jobs.failures import classify_day_failure
from procurement.jobs.lock import execution_lock
from procurement.jobs.runner import run_resource_day
from procurement.models.control import RunStatus
from procurement.storage.committed import select_committed_days, verify_committed
from procurement.storage.control import read_run_manifest
from procurement.storage.execution import read_execution
from procurement.storage.object_store import create_s3_filesystem


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("daily", "backfill", "repair", "status", "verify"))
    parser.add_argument("--resource", choices=("all", *SUPPORTED_RESOURCES), default="all")
    parser.add_argument("--year", type=int, help="Process one fully closed calendar year")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--lookback-days", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument(
        "--resource-workers", type=int, default=settings.INGESTION_RESOURCE_WORKERS,
        help="Concurrent resources (1-4); days within a resource remain sequential",
    )
    parser.add_argument(
        "--khlcnt-package-workers", type=int, default=settings.KHLCNT_PACKAGE_WORKERS,
        help="Concurrent packages within one KHLCNT plan (1-32)",
    )
    parser.add_argument(
        "--source-max-inflight", type=int, default=settings.MUASAMCONG_MAX_INFLIGHT,
        help="Shared HTTP request limit across all resources in this flow (1-32)",
    )
    parser.add_argument(
        "--max-days", type=int, help="Date limit (default: 31, or the full year with --year)"
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue after recorded source failures; final exit code remains nonzero",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--refresh", action="store_true", help="Also recrawl committed dates")
    parser.add_argument(
        "--retry-stale",
        action="store_true",
        help="Retry stale/interrupted attempts after confirming their worker stopped",
    )
    parser.add_argument("--stale-after-minutes", type=int, default=10)
    parser.add_argument("--lock-dir", type=Path, default=Path(settings.INGESTION_LOCK_DIR))
    return parser


def resolve_dates(args, *, today: date) -> tuple[date, date]:
    validate_page_size(args.page_size)
    for flag, value, maximum in (
        ("resource-workers", args.resource_workers, 4),
        ("khlcnt-package-workers", args.khlcnt_package_workers, 32),
        ("source-max-inflight", args.source_max_inflight, 32),
    ):
        if not 1 <= value <= maximum:
            raise ValueError(f"--{flag} must be between 1 and {maximum}")
    if (args.max_days is not None and args.max_days < 1) or args.stale_after_minutes < 1:
        raise ValueError("max-days and stale-after-minutes must be positive")
    if args.continue_on_error and args.mode in {"status", "verify"}:
        raise ValueError("--continue-on-error only applies to daily/backfill/repair")
    if args.mode == "daily":
        if args.year is not None or args.start_date is not None or args.end_date is not None:
            raise ValueError("daily uses --lookback-days, not explicit dates")
        if args.lookback_days < 1:
            raise ValueError("lookback-days must be positive")
        end = today - timedelta(days=1)
        start = end - timedelta(days=args.lookback_days - 1)
    elif args.year is not None:
        if args.start_date is not None or args.end_date is not None:
            raise ValueError("--year cannot be combined with --start-date/--end-date")
        if not 1 <= args.year < today.year:
            raise ValueError("--year must be a fully closed calendar year")
        start, end = date(args.year, 1, 1), date(args.year, 12, 31)
    else:
        if args.start_date is None or args.end_date is None:
            raise ValueError("--start-date and --end-date are required")
        start, end = args.start_date, args.end_date
    validate_closed_range(start, end, today=today)
    max_days = args.max_days if args.max_days is not None else (366 if args.year else 31)
    if (end - start).days + 1 > max_days:
        raise ValueError(
            "Date window exceeds --max-days; split the range or raise the explicit limit"
        )
    return start, end


def _liveness(fs, identity, run_id: str, threshold: timedelta) -> str:
    try:
        execution = read_execution(fs, identity, run_id)
        if execution is None:
            return "unknown"  # never infer worker death solely from an old run start time
        if execution["state"] in {"finished", "interrupted"}:
            return "interrupted"
        heartbeat = datetime.fromisoformat(execution["heartbeat_at"])
        return "stale" if datetime.now(UTC) - heartbeat > threshold else "running"
    except Exception:  # noqa: BLE001 -- sidecar is optional; uncertainty blocks automatic retry
        return "unknown"


def _execute_plan(args, fs, plan, report, run_day):
    budget = RequestBudget(args.source_max_inflight)

    def attempt(resource, source_date):
        try:
            run_id = run_day(
                resource, source_date, page_size=args.page_size,
                request_budget=budget, khlcnt_package_workers=args.khlcnt_package_workers,
            )
            identity = get_resource(resource).identity
            manifest = read_run_manifest(fs, identity, run_id)
            if manifest is not None and manifest.status is RunStatus.SUCCESS:
                return None
            reason = "unconfirmed_failure"
            if manifest is not None and manifest.status is RunStatus.FAILED:
                reason = classify_day_failure(fs, identity, run_id, source_date)
            return {
                "resource": resource, "date": str(source_date), "run_id": run_id,
                "error": "run_not_successful", "reason": reason,
            }
        except Exception as exc:  # noqa: BLE001 -- preserve diagnostics for every started day
            return {
                "resource": resource, "date": str(source_date),
                "error": sanitize_error_message(str(exc)), "reason": "execution_exception",
            }

    dates = {}
    for resource, source_date in plan:
        dates.setdefault(resource, deque()).append(source_date)
    ready = deque(dates)
    pending = {}
    stopping = False
    with ThreadPoolExecutor(
        max_workers=args.resource_workers, thread_name_prefix="ingestion-resource"
    ) as pool:
        try:
            while ready or pending:
                while ready and not stopping and len(pending) < args.resource_workers:
                    resource = ready.popleft()
                    source_date = dates[resource].popleft()
                    pending[pool.submit(attempt, resource, source_date)] = resource
                    report["attempted_days"] += 1
                if not pending:
                    break
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                # Inspect every completed result before admitting any more work.
                for future in done:
                    resource = pending.pop(future)
                    error = future.result()
                    if error is not None:
                        report["execution_errors"].append(error)
                        if not (
                            args.continue_on_error and error["reason"] == "source_failure"
                        ):
                            stopping = True
                    if dates[resource]:
                        # Another resource gets its turn before this one's next day.
                        ready.append(resource)
        except BaseException:
            # Drain started attempts before releasing the host lock, but stop new HTTP calls.
            budget.cancel()
            raise
    report["stopped_early"] = report["attempted_days"] < len(plan)


def execute_flow(args, *, fs=None, run_day=run_resource_day) -> tuple[dict, int]:
    start, end = resolve_dates(args, today=today_vn())
    fs = fs if fs is not None else create_s3_filesystem()
    resources = SUPPORTED_RESOURCES if args.resource == "all" else (args.resource,)
    report = {
        "mode": args.mode,
        "start_date": str(start),
        "end_date": str(end),
        "resources": [],
        "execution_errors": [],
        "stopped_early": False,
        "attempted_days": 0,
        "continue_on_error": args.continue_on_error,
        "concurrency": {
            "resource_workers": args.resource_workers,
            "khlcnt_package_workers": args.khlcnt_package_workers,
            "source_max_inflight": args.source_max_inflight,
        },
    }
    threshold = timedelta(minutes=args.stale_after_minutes)
    # Read every selected resource before making any write. Bad credentials/storage fail preflight.
    plan = []
    for resource in resources:
        definition = get_resource(resource)
        days = read_coverage(fs, definition.identity, start, end)
        entries = []
        liveness = {}
        for day in days:
            for run_id in day.active_run_ids:
                if run_id not in liveness:
                    liveness[run_id] = _liveness(fs, definition.identity, run_id, threshold)
            blockers = {run: liveness[run] for run in day.active_run_ids}
            can_retry = all(
                state in {"stale", "interrupted", "unknown"} for state in blockers.values()
            )
            blocked = bool(blockers) and not (args.retry_stale and can_retry)
            selected = (day.effective is None or args.refresh) and not blocked
            entry = {
                "date": str(day.source_date),
                "status": day.status,
                "effective_run_id": day.effective.run_id if day.effective else None,
                "active_runs": blockers,
                "planned": selected,
                "blocked": blocked,
            }
            entries.append(entry)
            if selected:
                plan.append((resource, day.source_date))
        report["resources"].append({"resource": resource, "dates": entries})

    report["planned_days"] = len(plan)
    if args.dry_run or args.mode == "status":
        gaps = any(
            day["status"] != "success" for item in report["resources"] for day in item["dates"]
        )
        return report, 1 if args.mode == "status" and gaps else 0

    if args.mode != "verify":
        if plan and not settings.MUASAMCONG_TOKEN:
            raise RuntimeError("Configure MUASAMCONG_TOKEN before executing ingestion")
        if plan:
            _execute_plan(args, fs, plan, report, run_day)

    for item in report["resources"]:
        definition = get_resource(item["resource"])
        coverage = read_coverage(fs, definition.identity, start, end)
        for entry, day in zip(item["dates"], coverage, strict=True):
            entry["status_before"] = entry["status"]
            entry["status"] = day.status
            entry["effective_run_id"] = day.effective.run_id if day.effective else None
        item["missing_dates"] = [str(day.source_date) for day in coverage if day.effective is None]
        item["blocked_dates"] = [
            entry["date"]
            for entry in item["dates"]
            if entry["blocked"] and (args.refresh or entry["status"] != "success")
        ]
        if item["missing_dates"]:
            continue
        try:
            selection = select_committed_days(fs, definition, start, end)
            item["verified"] = verify_committed(fs, selection)
        except Exception as exc:  # noqa: BLE001 -- broken committed data must make the flow fail
            report["execution_errors"].append(
                {"resource": item["resource"], "error": sanitize_error_message(str(exc))}
            )
    failed = bool(report["execution_errors"]) or any(
        item["missing_dates"] or (item["blocked_dates"] and args.mode != "verify")
        for item in report["resources"]
    )
    return report, int(failed)


def main():
    configure_logging()
    parser = _parser()
    args = parser.parse_args()
    try:
        resolve_dates(args, today=today_vn())
    except ValueError as exc:
        parser.error(str(exc))

    def terminate(_signum, _frame):
        raise KeyboardInterrupt("Termination requested")

    signal.signal(signal.SIGTERM, terminate)
    try:
        if args.mode in {"status", "verify"} or args.dry_run:
            report, code = execute_flow(args)
        else:
            with execution_lock(args.lock_dir):
                report, code = execute_flow(args)
        print(json.dumps(report, ensure_ascii=False, indent=2))
    except KeyboardInterrupt:
        print(json.dumps({"error": "interrupted; inspect manifests before retry"}))
        raise SystemExit(130) from None
    except Exception as exc:  # noqa: BLE001 -- CLI diagnostics must not expose credentials
        print(
            json.dumps({"error": type(exc).__name__, "message": sanitize_error_message(str(exc))})
        )
        raise SystemExit(1) from None
    raise SystemExit(code)


if __name__ == "__main__":
    main()
