"""Gated 2025 backfill: weekly/daily reconciliation, Bronze commits, then Silver/Iceberg.

Run once per source account/environment; 20 request starts/s is a per-process cap.
Artifacts contain operational counts and identifiers, never source payloads or credentials.
"""

import argparse
import json
import logging
import os
import sys
from collections import Counter
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Lock
from time import sleep
from typing import Any

import httpx

from procurement.bootstrap import bootstrap_services
from procurement.common.dates import api_day_window
from procurement.common.errors import sanitize_error_message
from procurement.common.settings import settings
from procurement.infrastructure.database import create_application_engine
from procurement.ingestion.contracts import MaterializeDayRequest
from procurement.ingestion.engine.pagination import iter_search_pages
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient, is_source_rate_limit
from procurement.ingestion.sources.muasamcong.contract_performance.resource import (
    ContractPerformanceApi,
    report_key,
)
from procurement.silver.adapters.driven.bronze.arrow_adapter import ArrowBronzeRepositoryAdapter
from procurement.silver.service import run_resource

RESOURCE = "contract_performance"
PILOT_START, PILOT_END = date(2025, 10, 17), date(2025, 10, 23)


def days(start: date, end: date):
    while start <= end:
        yield start
        start += timedelta(days=1)


def weekly_search(client: MuasamcongClient) -> dict[tuple[str, str], str]:
    found = {}
    for _, response in iter_search_pages(
        ContractPerformanceApi(client).search,
        window_from=api_day_window(PILOT_START)[0],
        window_to=api_day_window(PILOT_END)[1],
        search_key=report_key,
    ):
        for item in response["page"]["content"]:
            found[report_key(item)] = str(item["publicDate"])[:10]
    return found


def verify_pilot(repo, expected: dict[tuple[str, str], str]) -> dict[str, Any]:
    partitions = repo.select_committed_partitions([RESOURCE], PILOT_START, PILOT_END)
    if len(partitions) != 7:
        raise ValueError("Pilot requires seven committed publication days")
    found = {}
    for partition in partitions:
        for file, _, record in repo.stream_partition_records(
            partition,
            selected_tables=("contract_performance_detail",),
            verify_content_hash=True,
        ):
            if file["table"] != "contract_performance_detail":
                continue
            payload = record["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            root = payload["performance"]
            key = report_key(root)
            if key in found:
                raise ValueError("Pilot found duplicate report ID/version across daily partitions")
            found[key] = str(root["publicDate"])[:10]
            if found[key] != partition.source_date:
                raise ValueError("Pilot publication date differs from its source partition")
    if found != expected:
        raise ValueError(f"Pilot weekly/daily mismatch: weekly={len(expected)}, daily={len(found)}")
    return {
        "status": "passed",
        "weekly_reports": len(expected),
        "daily_reports": len(found),
        "publication_date_matches_partition": True,
    }


def silver_from_commits(output: Path) -> int:
    """Finish available data without constructing any source HTTP client."""
    from procurement.storage.object_store import create_s3_filesystem

    report = (
        json.loads(output.read_text())
        if output.exists()
        else {
            "resource": RESOURCE,
            "year": 2025,
            "days": {},
            "mapping_version": "contract-performance-1",
        }
    )
    if report.get("resource") != RESOURCE or report.get("year") != 2025:
        raise ValueError("Output report must belong to contract_performance/2025")
    report.update(status="running", phase="silver_from_commits", source_requests_in_silver_only=0)
    engine = create_application_engine()

    def save():
        report["updated_at"] = datetime.now(UTC).isoformat()
        report["progress"] = {
            "planned_days": 365,
            "bronze_days": len(report["days"]),
            "silver_days": sum(
                v.get("silver_status") == "completed" for v in report["days"].values()
            ),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        temp = output.with_suffix(".tmp")
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
        temp.replace(output)

    try:
        fs, bucket = create_s3_filesystem(), settings.OBJECT_STORAGE_BUCKET
        repo = ArrowBronzeRepositoryAdapter(fs=fs, db_engine=engine, verify_files=True)
        partitions = repo.select_committed_partitions(
            [RESOURCE], date(2025, 1, 1), date(2025, 12, 31)
        )
        dates = [date.fromisoformat(p.source_date) for p in partitions]
        for p in partitions:
            entry = report["days"].setdefault(p.source_date, {})
            entry.update(
                source_date=p.source_date, status="success", record_count=p.expected_records
            )
        ranges = []
        for day in dates:
            if ranges and day == ranges[-1][1] + timedelta(days=1):
                ranges[-1] = (ranges[-1][0], day)
            else:
                ranges.append((day, day))

        def progress(item):
            key = item["date"]
            entry = report["days"][key]
            report["current_date"] = key
            entry.update(
                silver_status=item["status"],
                silver_quality=item.get("quality", {}),
                iceberg_tables=sorted((item.get("iceberg") or {}).keys()),
            )
            save()
            if item["status"] != "completed":
                raise ValueError(f"Silver failed for {key}; inspect its stored report")
            if not {"contract_revision", "contract_performance_revision"} <= set(
                entry["iceberg_tables"]
            ):
                raise ValueError(f"Missing Iceberg domain confirmation for {key}")
            print(f"{key} silver=completed source_requests=0", flush=True)

        for first, last in ranges:
            result = run_resource(
                fs,
                bucket,
                RESOURCE,
                first,
                last,
                db_engine=engine,
                iceberg=True,
                on_progress=progress,
            )
            for item in result["days"]:
                report["days"][item["date"]]["silver_report"] = (
                    result["output_prefix"] + "/report.json"
                )
            save()
        if len(partitions) == 365:
            report.update(status="completed", phase="year_verified", verified_committed_days=365)
        else:
            report.update(
                status="blocked" if report.get("source_error") else "partial",
                phase="available_silver_ready",
            )
        report["verified_committed_days"] = len(partitions)
        report["pending_dates"] = [
            str(d) for d in days(date(2025, 1, 1), date(2025, 12, 31)) if d not in set(dates)
        ]
        save()
        return 0
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001 — persist sanitized failure
        report.update(
            status="failed", error_type=type(exc).__name__, error=sanitize_error_message(str(exc))
        )
        save()
        print(f"Silver stopped: {report['error_type']}: {report['error']}", file=sys.stderr)
        return 1
    finally:
        engine.dispose()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, choices=[2025], default=2025)
    parser.add_argument("--pilot-only", action="store_true")
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Publish only committed days; never contact the source website.",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("docs/reports/contract-performance-2025-run.json")
    )
    args = parser.parse_args(argv)
    if args.silver_only:
        if args.pilot_only:
            parser.error("--silver-only cannot be combined with --pilot-only")
        return silver_from_commits(args.output)
    os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    statuses: Counter[str] = Counter()
    latencies = []
    observer_lock = Lock()
    report: dict[str, Any] = {
        "resource": RESOURCE,
        "year": args.year,
        "status": "running",
        "phase": "pilot",
        "started_at": datetime.now(UTC).isoformat(),
        "pilot_only": args.pilot_only,
        "mapping_version": "contract-performance-1",
        "request_policy": {
            "max_starts_per_second": 20,
            "max_inflight": 4,
            "max_attempts": 3,
            "stop_on": [401, 403, 429, "HTML challenge"],
        },
        "days": {},
    }

    def observe(event):
        if event["kind"] == "attempt":
            with observer_lock:
                statuses[str(event["http_status"] or event["error"])] += 1
                latencies.append(event["latency_ms"])

    def save():
        report["updated_at"] = datetime.now(UTC).isoformat()
        report["progress"] = {
            "planned_days": 7 if args.pilot_only else 365,
            "bronze_days": len(report["days"]),
            "silver_days": sum(
                v.get("silver_status") == "completed" for v in report["days"].values()
            ),
        }
        with observer_lock:
            report["http"] = {
                "attempts": sum(statuses.values()),
                "statuses": dict(statuses),
                "max_latency_ms": max(latencies, default=0),
            }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temp = args.output.with_suffix(".tmp")
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
        temp.replace(args.output)

    client = MuasamcongClient(
        token=settings.MUASAMCONG_TOKEN or "placeholder-token", observer=observe
    )
    reader = client.contract_performance_reader()
    engine = None
    try:
        engine = create_application_engine()
        service, _ = bootstrap_services(client=client)
        # Keep DLT working state in the workspace, independently of the developer's home.
        service.pipelines_dir = str(Path(".tmp/contract-performance-dlt").resolve())
        repo = ArrowBronzeRepositoryAdapter(fs=service.fs, db_engine=engine)
        expected = weekly_search(reader)
        save()
        sleep(0.05)

        def publish(first, last):
            def progress(item):
                key = item["date"]
                entry = report["days"][key]
                report["current_date"] = key
                entry.update(
                    silver_status=item["status"],
                    silver_quality=item.get("quality", {}),
                    silver_report=f"{service.bucket}/silver/_reports/{RESOURCE}/latest.json",
                    iceberg_tables=sorted((item.get("iceberg") or {}).keys()),
                )
                save()
                if item["status"] != "completed":
                    raise ValueError(f"Silver did not complete for {key}; see its stored report")
                if not {"contract_revision", "contract_performance_revision"} <= set(
                    entry["iceberg_tables"]
                ):
                    raise ValueError(f"Iceberg domain publication not confirmed for {key}")
                print(f"{key} silver=completed", flush=True)

            silver = run_resource(
                service.fs,
                service.bucket,
                RESOURCE,
                first,
                last,
                db_engine=engine,
                iceberg=True,
                on_progress=progress,
            )
            for item in silver["days"]:
                report["days"][item["date"]]["silver_report"] = (
                    silver["output_prefix"] + "/report.json"
                )
            save()
            if silver["status"] != "completed":
                raise ValueError("Silver range did not complete")

        def materialize(day, *, publish_silver=True):
            key = str(day)
            report["current_date"] = key
            save()
            bronze = service.materialize_day(MaterializeDayRequest(day, resource=RESOURCE))
            report["days"][key] = asdict(bronze)
            save()
            print(f"{day} bronze={bronze.record_count} reused={bronze.reused}", flush=True)
            if publish_silver:
                publish(day, day)

        for day in days(PILOT_START, PILOT_END):
            materialize(day)
        sleep(0.05)
        after = weekly_search(reader)
        if after != expected:
            raise ValueError("Source weekly search changed during pilot; reconcile before backfill")
        report["pilot"] = verify_pilot(repo, after)
        save()
        print("pilot passed: " + str(report["pilot"]["daily_reports"]) + " reports", flush=True)
        if not args.pilot_only:
            report["phase"] = "backfill_bronze"
            for day in days(date(2025, 1, 1), date(2025, 12, 31)):
                if not PILOT_START <= day <= PILOT_END:
                    materialize(day, publish_silver=False)
            report["phase"] = "backfill_silver"
            publish(date(2025, 1, 1), date(2025, 12, 31))
            committed = repo.select_committed_partitions(
                [RESOURCE],
                date(2025, 1, 1),
                date(2025, 12, 31),
            )
            if len(committed) != 365:
                raise ValueError("Year verification requires all 365 committed days")
            report["verified_committed_days"] = len(committed)
            if sum(v.get("silver_status") == "completed" for v in report["days"].values()) != 365:
                raise ValueError("Year verification requires all 365 completed Silver days")
        report["status"] = "completed"
        report["phase"] = "pilot_verified" if args.pilot_only else "year_verified"
        save()
        return 0
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001 — durable sanitized operational report
        report.update(
            status="failed", error_type=type(exc).__name__, error=sanitize_error_message(str(exc))
        )
        diagnostics = getattr(exc, "diagnostics", None)
        if isinstance(exc, httpx.HTTPStatusError):
            report["source_error"] = {
                "http_status": exc.response.status_code,
                "endpoint": exc.request.url.path,
                "retry_after": sanitize_error_message(exc.response.headers.get("Retry-After", "")),
            }
            if is_source_rate_limit(exc.response):
                report["source_error"]["effective_status"] = 429
                report["source_blocked_date"] = report.get("current_date")
                report["status"] = "blocked"
            if isinstance(diagnostics, dict):
                report["source_error"].update(diagnostics)
        elif isinstance(diagnostics, dict):
            report["source_error"] = diagnostics
        save()
        print(f"Stopped: {report['error_type']}: {report['error']}", file=sys.stderr)
        return 1
    finally:
        reader.close()
        client.close()
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
