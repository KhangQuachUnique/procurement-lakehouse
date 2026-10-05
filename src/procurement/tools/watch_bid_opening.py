"""Seed/check late bid openings from committed TBMT; status and dry runs never change state."""
import argparse
import json
from contextlib import nullcontext
from datetime import date
from pathlib import Path

from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import settings
from procurement.ingestion.bid_opening_watch import WatchStore, check, seed
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.jobs.lock import execution_lock
from procurement.quality.files import cli
from procurement.storage.object_store import create_s3_filesystem


def run_watch(fs, *, mode="check", path=None, start=None, end=None, max_days=None,
              dry_run=False, request_budget=None, detail_workers=None):
    workers = settings.BID_OPENING_DETAIL_WORKERS if detail_workers is None else detail_workers
    if not 1 <= workers <= 32:
        raise ValueError("bid opening detail_workers must be between 1 and 32")
    path = Path(path or settings.BID_OPENING_WATCH_PATH)
    readonly = dry_run or mode == "status"
    lock = nullcontext() if readonly else exclusive_file_lock(path.with_suffix(".lock"))
    with lock:
        store = WatchStore(path, dry_run=readonly, read_only=mode == "status")
        try:
            if mode == "status":
                return store.status(start=start, end=end)
            report = {"seed": seed(fs, store, start=start, end=end)}
            if mode == "check":
                budget = request_budget or RequestBudget(
                    settings.BID_OPENING_MAX_INFLIGHT,
                    min_interval=settings.BID_OPENING_REQUEST_INTERVAL_SECONDS)
                if dry_run:
                    report["check"] = check(fs, store, None, start=start, end=end,
                        max_days=max_days or settings.BID_OPENING_WATCH_MAX_DAYS, dry_run=True)
                else:
                    with MuasamcongClient(token=settings.MUASAMCONG_TOKEN,
                        max_attempts=settings.MUASAMCONG_MAX_ATTEMPTS,
                        max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
                        request_budget=budget) as client:
                        report["check"] = check(fs, store, client, start=start, end=end,
                            max_days=max_days or settings.BID_OPENING_WATCH_MAX_DAYS,
                            request_budget=budget, detail_workers=workers)
            return report
        finally:
            store.close()


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
        result = run_watch(None if args.mode == "status" else create_s3_filesystem(),
            mode=args.mode, path=args.state, start=args.start_date, end=args.end_date,
            max_days=args.max_days, dry_run=args.dry_run, detail_workers=args.bid_opening_detail_workers)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(cli(main))
