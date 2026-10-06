"""CLI entry point for ingesting a partition day using the new IngestionService."""

import argparse
import json
import sys
from datetime import date
from uuid import UUID, uuid4

from procurement.bootstrap import get_ingestion_service
from procurement.common.catalog import DEFAULT_SOURCE, SUPPORTED_RESOURCES
from procurement.ingestion.contracts import MaterializeDayRequest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize a single partition day for a given resource."
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
        required=True,
        help="Target partition date (YYYY-MM-DD).",
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


def run_ingest(args: argparse.Namespace) -> dict:
    req = MaterializeDayRequest(
        source_date=args.source_date,
        source=args.source,
        resource=args.resource,
        refresh=args.refresh,
        request_id=args.request_id or uuid4(),
        owner_id=args.owner_id or uuid4(),
        dagster_run_id=args.dagster_run_id,
    )

    service = get_ingestion_service()
    result = service.materialize_day(req)

    return {
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        report = run_ingest(args)
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "success" else 1
    except Exception as exc:
        err_report = {
            "source": args.source,
            "resource": args.resource,
            "source_date": str(args.source_date),
            "status": "failed",
            "error": str(exc),
        }
        print(json.dumps(err_report, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
