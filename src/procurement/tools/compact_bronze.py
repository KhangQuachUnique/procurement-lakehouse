"""Compact old committed Bronze days into new SUCCESS attempts without calling source APIs."""

import argparse
import json
from datetime import date
from pathlib import Path

from procurement.common.catalog import SUPPORTED_RESOURCES
from procurement.common.dates import today_vn
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.quality.files import cli, read_json, write_json
from procurement.storage.compaction import create_plan, run_plan, validate_plan
from procurement.storage.object_store import create_s3_filesystem


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="Inspect committed days and save a frozen compaction plan")
    plan.add_argument("--resource", choices=("all", *SUPPORTED_RESOURCES), required=True)
    plan.add_argument("--year", type=int)
    plan.add_argument("--start-date", type=date.fromisoformat)
    plan.add_argument("--end-date", type=date.fromisoformat)
    plan.add_argument("--target-mib", type=int, default=128)
    plan.add_argument("--output-dir", type=Path, default=Path("exports/bronze-compaction"))
    run = commands.add_parser("run", help="Apply/resume a saved plan under the ingestion execution lock")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--continue-on-error", action="store_true")
    run.add_argument("--lock-dir", type=Path, default=Path(settings.INGESTION_LOCK_DIR))
    args = parser.parse_args(argv)
    configure_logging()
    if args.command == "plan":
        if args.year is not None:
            if args.start_date or args.end_date or not 1 <= args.year < today_vn().year:
                parser.error("--year requires a closed calendar year and cannot be combined with dates")
            start, end = date(args.year, 1, 1), date(args.year, 12, 31)
        else:
            if not args.start_date or not args.end_date:
                parser.error("Specify --year or both --start-date and --end-date")
            start, end = args.start_date, args.end_date
        if args.target_mib <= 0:
            parser.error("--target-mib must be positive")
        from procurement.common.dates import validate_closed_range
        validate_closed_range(start, end, today=today_vn())
        result = create_plan(create_s3_filesystem(), resource=args.resource, start=start, end=end,
                             target_bytes=args.target_mib * 1024**2)
        path = args.output_dir / result["plan_id"] / "plan.json"
        write_json(path, result)
        print(json.dumps({"plan": str(path), "candidate_days": len(result["days"]),
                          "skipped_days": len(result["skipped"]),
                          "input_files": sum(len(d["files"]) for d in result["days"]),
                          "input_bytes": sum(d["input_bytes"] for d in result["days"])}))
    else:
        result = read_json(args.plan)
        validate_plan(result)  # Reject modified/foreign plans before connecting to storage.
        report = run_plan(create_s3_filesystem(), result, args.plan.parent,
                          continue_on_error=args.continue_on_error, lock_dir=args.lock_dir)
        print(json.dumps({"complete": report["complete"], "days": len(report["days"]),
                          "report": str(args.plan.parent / "report.json")}))
        return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(cli(main))
