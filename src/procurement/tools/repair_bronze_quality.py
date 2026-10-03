"""Plan or apply selective record repairs as new complete Bronze day attempts."""

import argparse
from datetime import date
from math import isfinite
from pathlib import Path

from procurement.common.dates import today_vn
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.quality.comparison import compare_audits
from procurement.quality.contracts import load_config
from procurement.quality.files import cli, read_json
from procurement.quality.repair import apply_plan, create_plan
from procurement.quality.workflow import run_workflow
from procurement.storage.object_store import create_s3_filesystem


def repair_client(args):
    print(f"Fetch: {args.detail_workers} worker(s), start interval {args.request_interval}s, "
          f"up to {args.max_attempts} attempts", flush=True)
    return MuasamcongClient(
        token=settings.MUASAMCONG_TOKEN, max_attempts=args.max_attempts,
        max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
        request_budget=RequestBudget(min(args.detail_workers, settings.MUASAMCONG_MAX_INFLIGHT),
                                     min_interval=args.request_interval),
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    run = commands.add_parser("run", help="Audit, plan, repair and verify; automatically resume the same job")
    run.add_argument("--year", type=int, required=True)
    run.add_argument("--work-dir", type=Path)
    run.add_argument("--config", type=Path)
    plan = commands.add_parser("plan")
    plan.add_argument("--audit", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--config", type=Path)
    apply = commands.add_parser("apply")
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--work-dir", type=Path, required=True)
    apply.add_argument("--config", type=Path)
    apply.add_argument("--limit-days", type=int)
    apply.add_argument("--date", type=date.fromisoformat, action="append", dest="dates")
    apply.add_argument("--continue-on-error", action="store_true",
                       help="Continue past validation/baseline blockers; storage/auth failures stop")
    report = commands.add_parser("report")
    report.add_argument("--before", type=Path, required=True)
    report.add_argument("--after", type=Path, required=True)
    report.add_argument("--plan", type=Path, required=True)
    report.add_argument("--work-dir", type=Path, required=True)
    report.add_argument("--output", type=Path, required=True)
    for command in (run, apply):
        command.add_argument("--detail-workers", type=int, default=settings.MUASAMCONG_MAX_INFLIGHT)
        command.add_argument("--request-interval", type=float, default=settings.MUASAMCONG_REQUEST_INTERVAL_SECONDS,
                             help="Minimum seconds between request starts across all workers (from settings)")
        command.add_argument("--max-attempts", type=int, default=settings.MUASAMCONG_MAX_ATTEMPTS)
    args = parser.parse_args(argv)
    if args.mode in {"run", "apply"}:
        if not 1 <= args.detail_workers <= 32:
            parser.error("--detail-workers must be between 1 and 32")
        if not isfinite(args.request_interval) or args.request_interval < 0:
            parser.error("--request-interval must be finite and non-negative")
        if not 1 <= args.max_attempts <= 10:
            parser.error("--max-attempts must be between 1 and 10")
    if args.mode == "run":
        if not 1 <= args.year < today_vn().year:
            parser.error("--year must be a closed year")
        directory = args.work_dir or Path(f"exports/notify-quality-job-{args.year}")
        with repair_client(args) as client:
            result = run_workflow(create_s3_filesystem(), client, year=args.year, directory=directory,
                                  config_path=args.config, detail_workers=args.detail_workers)
        print(f"Job: {result['status']}; {result['counts']}")
        return 0 if result["status"] == "complete" else 2
    if args.mode == "report":
        result = compare_audits(args.before, args.after, read_json(args.plan),
                                args.work_dir, args.output)
        print(f"Repair: {result['counts']}; fully verified: {result['fully_verified']}")
        return 0 if result["fully_verified"] else 2
    config = load_config(args.config)
    if args.mode == "plan":
        result = create_plan(args.audit, config, args.output)
        print(f"Days: {len(result['days'])}; detail requests: {result['estimated_detail_requests']}; "
              f"blocked days: {len(result['blocked'])}")
        return
    if args.limit_days is not None and args.limit_days < 1:
        parser.error("--limit-days must be positive")
    with repair_client(args) as client:
        results = apply_plan(create_s3_filesystem(), client, read_json(args.plan), config,
                             args.work_dir, limit=args.limit_days, detail_workers=args.detail_workers,
                             dates=args.dates, continue_on_error=args.continue_on_error)
    successful = sum(result["status"] == "success" for result in results)
    print(f"Verified repaired days: {successful}; blocked: {len(results) - successful}")
    return 0 if successful == len(results) else 2


if __name__ == "__main__":
    raise SystemExit(cli(main))
