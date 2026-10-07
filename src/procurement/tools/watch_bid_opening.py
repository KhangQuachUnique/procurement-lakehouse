"""CLI tool to seed/check late bid openings from committed TBMT."""
import argparse
import json
from contextlib import nullcontext
from datetime import date
from pathlib import Path

from procurement.common.settings import settings
from procurement.jobs.lock import execution_lock
from procurement.quality.files import cli
from procurement.storage.object_store import create_s3_filesystem
from procurement.watcher import run_watch


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "check", "status"))
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--max-days", type=int, default=settings.BID_OPENING_WATCH_MAX_DAYS)
    parser.add_argument("--state", type=Path, default=Path(settings.BID_OPENING_WATCH_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bid-opening-detail-workers", type=int, default=settings.BID_OPENING_DETAIL_WORKERS)
    args = parser.parse_args(argv)
    if not 1 <= args.bid_opening_detail_workers <= 32:
        parser.error("--bid-opening-detail-workers must be between 1 and 32")
    if args.max_days < 1 or (args.start_date and args.end_date and args.start_date > args.end_date):
        parser.error("Invalid max-days/date range")
    # Seed only reads pinned Bronze and has its own state lock; only check publishes Bronze.
    lock = execution_lock(Path(settings.INGESTION_LOCK_DIR)) if args.mode == "check" and not args.dry_run else nullcontext()
    with lock:
        result = run_watch(
            None if args.mode == "status" else create_s3_filesystem(),
            mode=args.mode,
            path=args.state,
            start=args.start_date,
            end=args.end_date,
            max_days=args.max_days,
            dry_run=args.dry_run,
            detail_workers=args.bid_opening_detail_workers,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(cli(main))
