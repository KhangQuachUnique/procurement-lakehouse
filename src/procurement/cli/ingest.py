"""CLI entry point for ingesting partition days using the new IngestionService."""

import argparse
import json
import sys
from datetime import date, timedelta
from uuid import UUID, uuid4

from procurement.bootstrap import get_ingestion_service
from procurement.common.catalog import DEFAULT_SOURCE, SUPPORTED_RESOURCES
from procurement.ingestion.contracts import MaterializeDayRequest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize partition day(s) for a given resource using IngestionService."
    )
    parser.add_argument(
        "--resource",
        required=True,
        choices=SUPPORTED_RESOURCES,
        help="Target procurement resource name.",
    )
    parser.add_argument(
        "--date",
        "--source-date",
        dest="source_date",
        type=date.fromisoformat,
        default=None,
        help="Target partition date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        help="Start partition date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=None,
        help="End partition date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Full calendar year to ingest (e.g. 2025).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        default=False,
        help="Continue processing subsequent days if a partition day fails.",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Data source name (default: {DEFAULT_SOURCE}).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        default=False,
        help="Force re-ingestion even if valid commit exists.",
    )
    parser.add_argument(
        "--request-id",
        type=UUID,
        default=None,
        help="Idempotency request UUID (default: generate random UUID).",
    )
    parser.add_argument(
        "--owner-id",
        type=UUID,
        default=None,
        help="Worker owner UUID (default: generate random UUID).",
    )
    parser.add_argument(
        "--dagster-run-id",
        default=None,
        help="Correlation Dagster run ID if running in pipeline.",
    )
    return parser


def resolve_dates(args: argparse.Namespace) -> list[date]:
    if args.source_date is not None:
        if args.year is not None or args.start_date is not None or args.end_date is not None:
            raise ValueError("--date cannot be combined with --year or --start-date/--end-date")
        return [args.source_date]
    if args.year is not None:
        if args.start_date is not None or args.end_date is not None:
            raise ValueError("--year cannot be combined with --start-date/--end-date")
        start = date(args.year, 1, 1)
        end = date(args.year, 12, 31)
    elif args.start_date is not None and args.end_date is not None:
        start = args.start_date
        end = args.end_date
    else:
        raise ValueError("Must specify --date, --year, or both --start-date and --end-date")

    if start > end:
        raise ValueError(f"Start date {start} must be <= end date {end}")

    days: list[date] = []
    curr = start
    while curr <= end:
        days.append(curr)
        curr += timedelta(days=1)
    return days


def run_ingest(args: argparse.Namespace) -> dict:
    dates = resolve_dates(args)
    service = get_ingestion_service()
    reports = []

    for day in dates:
        req = MaterializeDayRequest(
            source_date=day,
            source=args.source,
            resource=args.resource,
            refresh=args.refresh,
            request_id=args.request_id or uuid4(),
            owner_id=args.owner_id or uuid4(),
            dagster_run_id=args.dagster_run_id,
        )
        result = service.materialize_day(req)
        rep = {
            "source": result.source,
            "resource": result.resource,
            "source_date": str(result.source_date),
            "status": result.status,
            "reused": result.reused,
            "commit_id": str(result.commit_id) if result.commit_id else None,
            "attempt_id": str(result.attempt_id) if result.attempt_id else None,
            "record_count": result.record_count,
            "file_count": result.file_count,
            "metrics": result.metrics,
        }
        reports.append(rep)
        if result.status != "success" and not args.continue_on_error and len(dates) > 1:
            break

    if len(dates) == 1 and args.source_date is not None:
        return reports[0]

    return {
        "resource": args.resource,
        "total_days": len(dates),
        "processed_days": len(reports),
        "success_days": sum(1 for r in reports if r["status"] == "success"),
        "failed_days": sum(1 for r in reports if r["status"] != "success"),
        "results": reports,
        "status": "success" if all(r["status"] == "success" for r in reports) else "failed",
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        report = run_ingest(args)
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "success" else 1
    except Exception as exc:  # noqa: BLE001
        err_report = {
            "source": args.source,
            "resource": args.resource,
            "source_date": str(args.source_date) if args.source_date else None,
            "status": "failed",
            "error": str(exc),
        }
        print(json.dumps(err_report, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

