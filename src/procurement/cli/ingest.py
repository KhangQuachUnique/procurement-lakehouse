"""CLI entry point for ingesting partition days using the new IngestionService."""

import argparse
import json
import sys
from datetime import date, timedelta
from uuid import UUID, uuid4

from dotenv import load_dotenv

from procurement.bootstrap import get_ingestion_service, get_metadata_service
from procurement.common.catalog import DEFAULT_SOURCE, SUPPORTED_RESOURCES
from procurement.common.dates import today_vn, validate_closed_range
from procurement.common.errors import sanitize_error_message
from procurement.ingestion.contracts import MaterializeDayRequest
from procurement.metadata.models import PartitionIdentity
from procurement.tools.ui import (
    is_rich_enabled,
    render_ingestion_summary,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize partition day(s) for a given resource using IngestionService."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read PostgreSQL commits and show missing dates without crawling or writing.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON instead of interactive UI",
    )
    parser.add_argument(
        "--resource",
        required=True,
        choices=("all", *SUPPORTED_RESOURCES),
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
        help="Calendar year; the current year ends at yesterday in Vietnam.",
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
        validate_closed_range(args.source_date, args.source_date, today=today_vn())
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

    if args.year == today_vn().year:
        end = today_vn() - timedelta(days=1)
    validate_closed_range(start, end, today=today_vn())

    days: list[date] = []
    curr = start
    while curr <= end:
        days.append(curr)
        curr += timedelta(days=1)
    return days


def run_ingest(args: argparse.Namespace) -> dict:
    dates = resolve_dates(args)
    resources = SUPPORTED_RESOURCES if args.resource == "all" else (args.resource,)
    if (args.request_id is not None or args.owner_id is not None) and len(dates) * len(resources) > 1:
        raise ValueError("--request-id/--owner-id require a single resource and date")
    if args.dry_run:
        metadata = get_metadata_service()
        plans = []
        for resource in resources:
            snapshots = metadata.get_snapshot([
                PartitionIdentity(args.source, resource, day) for day in dates
            ])
            committed = {view.partition.identity.source_date for view in snapshots}
            plans.append({
                "resource": resource,
                "committed_days": len(committed),
                "reused_days": 0 if args.refresh else len(committed),
                "planned_dates": [str(day) for day in dates if args.refresh or day not in committed],
            })
        return {
            "status": "success", "dry_run": True, "metadata": "postgresql",
            "start_date": str(dates[0]), "end_date": str(dates[-1]),
            "total_days": len(dates), "resources": plans,
        }

    service = get_ingestion_service()
    reports = []
    stopped = False
    for resource in resources:
        for day in dates:
            req = MaterializeDayRequest(
                source_date=day, source=args.source, resource=resource,
                refresh=args.refresh, request_id=args.request_id or uuid4(),
                owner_id=args.owner_id or uuid4(), dagster_run_id=args.dagster_run_id,
            )
            result = service.materialize_day(req)
            reports.append({
                "source": result.source, "resource": result.resource,
                "source_date": str(result.source_date), "status": result.status,
                "reused": result.reused,
                "commit_id": str(result.commit_id) if result.commit_id else None,
                "attempt_id": str(result.attempt_id) if result.attempt_id else None,
                "record_count": result.record_count, "file_count": result.file_count,
                "metrics": result.metrics,
            })
            print(
                f"{resource}/{day}: {result.status} "
                f"({'reused' if result.reused else 'ingested'}, {result.record_count} records)",
                file=sys.stderr, flush=True,
            )
            if result.status != "success" and not args.continue_on_error:
                stopped = True
                break
        if stopped:
            break
    if len(dates) == 1 and args.source_date is not None and len(resources) == 1:
        return reports[0]
    return {
        "resource": args.resource, "metadata": "postgresql",
        "total_days": len(dates) * len(resources), "processed_days": len(reports),
        "success_days": sum(r["status"] == "success" for r in reports),
        "failed_days": sum(r["status"] != "success" for r in reports),
        "reused_days": sum(r["reused"] for r in reports),
        "stopped_early": len(reports) < len(dates) * len(resources),
        "results": reports,
        "status": "success" if all(r["status"] == "success" for r in reports) else "failed",
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    load_dotenv()

    try:
        report = run_ingest(args)
        if is_rich_enabled(args) and not args.dry_run:
            render_ingestion_summary(report)
        else:
            print(json.dumps(report, indent=2))
        return 0 if report["status"] == "success" else 1
    except Exception as exc:  # noqa: BLE001
        err_report = {
            "source": args.source,
            "resource": args.resource,
            "source_date": str(args.source_date) if args.source_date else None,
            "status": "failed",
            "error": sanitize_error_message(str(exc)),
        }
        print(json.dumps(err_report, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

