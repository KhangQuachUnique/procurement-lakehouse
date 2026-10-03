"""Inventory all notify workflows and separately flag suspected detail routing gaps."""

import argparse
import json
from collections import Counter
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from procurement.common.dates import api_day_window, today_vn, validate_closed_range
from procurement.common.errors import sanitize_error_message
from procurement.common.settings import settings
from procurement.ingestion.engine.pagination import iter_search_pages
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.notify_contractor.resource import (
    REOFFER_DETAIL_PATH,
    STANDARD_DETAIL_PATH,
    NotifyContractorApi,
)
from procurement.ingestion.sources.muasamcong.notify_contractor.router import (
    DetailKind,
    UnsupportedNotifyWorkflowError,
    resolve_detail_kind,
)
from procurement.ingestion.sources.muasamcong.search import search_document_key
from procurement.quality.contracts import (
    ENDPOINTS,
    DetailValidationError,
    load_config,
    resolve_route,
)

WORKFLOW_FIELDS = ("stepCode", "processApply", "bidForm", "isInternet", "bidMode")
SAMPLE_LIMIT = 5


def legacy_route(item):
    step = item.get("stepCode")
    try:
        kind = resolve_detail_kind(str(step) if step is not None else None)
    except UnsupportedNotifyWorkflowError:
        return {"kind": "unsupported", "endpoint": None}
    return {
        "kind": kind.value,
        "endpoint": STANDARD_DETAIL_PATH if kind is DetailKind.STANDARD else REOFFER_DETAIL_PATH,
    }


@lru_cache(maxsize=1)
def _routing_config():
    return load_config()


def current_route(item):
    try:
        route = resolve_route(item, _routing_config())
    except DetailValidationError:
        return {"kind": "unsupported", "endpoint": None}
    return {"kind": route.contract, "endpoint": ENDPOINTS[route.contract]}


def workflow_key(fields):
    # Preserve raw values and types, including null, rather than assuming upstream enums.
    return json.dumps(fields, sort_keys=True, ensure_ascii=False)


def add_samples(target, samples):
    for sample in samples:
        if len(target) >= SAMPLE_LIMIT:
            break
        if sample not in target:
            target.append(sample)


def routing_reasons(item):
    """Heuristics for investigation, not a verified replacement routing policy."""
    reasons = []
    if not item.get("id"):
        reasons.append("missing_id")
    kind = legacy_route(item)["kind"]
    if kind == "unsupported":
        reasons.append("unsupported_step_code")
    if kind == DetailKind.STANDARD:
        if item.get("bidForm") == "CGTTRG":
            reasons.append("cgttrg_routed_standard")
        if item.get("processApply") == "KHAC":
            reasons.append("khac_routed_ldt")
        if str(item.get("isInternet")) == "0":
            reasons.append("offline_needs_verification")
    return reasons


def scan_day(api, source_date, page_size):
    start, end = api_day_window(source_date)
    candidates = []
    scanned = 0
    reasons_count = Counter()
    workflows = {}
    for _, response in iter_search_pages(
        api.search, window_from=start, window_to=end,
        page_size=page_size, search_key=search_document_key,
    ):
        for item in response["page"]["content"]:
            scanned += 1
            reasons = routing_reasons(item)
            fields = {key: item.get(key) for key in WORKFLOW_FIELDS}
            group = workflows.setdefault(workflow_key(fields), {
                "fields": fields, "current_route": current_route(item),
                "records": 0, "suspected_records": 0, "samples": [],
            })
            group["records"] += 1
            group["suspected_records"] += bool(reasons)
            add_samples(group["samples"], [{
                "id": item.get("id"), "notifyNo": item.get("notifyNo"),
                "notifyVersion": item.get("notifyVersion"), "date": source_date.isoformat(),
            }])
            if reasons:
                reasons_count.update(reasons)
                candidates.append({
                    **{key: item.get(key) for key in (
                        "id", "notifyNo", "notifyVersion", "publicDate",
                        *WORKFLOW_FIELDS,
                    )},
                    "reasons": reasons,
                })
    return {
        "date": source_date.isoformat(), "scanned": scanned,
        "suspected": len(candidates), "reasons": dict(reasons_count),
        "candidates": candidates,
        "workflows": list(workflows.values()),
    }


def save_report(path, report):
    days = report["days"]
    reasons = Counter()
    workflows = {}
    for day in days:
        reasons.update(day["reasons"])
        for daily in day["workflows"]:
            group = workflows.setdefault(workflow_key(daily["fields"]), {
                "fields": daily["fields"], "current_route": daily["current_route"],
                "records": 0, "suspected_records": 0, "dates": [], "samples": [],
            })
            group["records"] += daily["records"]
            group["suspected_records"] += daily["suspected_records"]
            group["dates"].append({"date": day["date"], "records": daily["records"]})
            add_samples(group["samples"], daily["samples"])
    report["workflows"] = sorted(workflows.values(), key=lambda group: -group["records"])
    report["summary"] = {
        "completed_days": len(days),
        "scanned_records": sum(day["scanned"] for day in days),
        "suspected_records": sum(day["suspected"] for day in days),
        "affected_dates": [day["date"] for day in days if day["suspected"]],
        "reasons": dict(reasons),
        "workflow_count": len(workflows),
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def load_resume(path, year):
    report = json.loads(path.read_text(encoding="utf-8-sig"))
    if report.get("schema_version") != 2 or report.get("year") != year:
        raise ValueError("Resume report must use schema 2 and match --year")
    expected = date(year, 1, 1)
    for day in report["days"]:
        if day["date"] != expected.isoformat() or "workflows" not in day:
            raise ValueError("Resume report must contain consecutive, complete days from January 1")
        if sum(group["records"] for group in day["workflows"]) != day["scanned"]:
            raise ValueError("Resume workflow counts do not match scanned records")
        expected += timedelta(days=1)
    if len(report["days"]) > (date(year, 12, 31) - date(year, 1, 1)).days + 1:
        raise ValueError("Resume report contains dates outside the requested year")
    for key in ("failed_date", "error", "error_message"):
        report.pop(key, None)
    report["status"] = "running"
    return report, expected


def safe_error(exc):
    message = sanitize_error_message(str(exc))
    if settings.MUASAMCONG_TOKEN:
        message = message.replace(settings.MUASAMCONG_TOKEN, "[REDACTED]")
    return message


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--resume", action="store_true", help="Continue the existing output report")
    args = parser.parse_args(argv)
    try:
        start, end = date(args.year, 1, 1), date(args.year, 12, 31)
        validate_closed_range(start, end, today=today_vn())
        if args.page_size <= 0:
            raise ValueError("page-size must be positive")
    except ValueError as exc:
        parser.error(str(exc))
    path = args.output or Path(f"exports/notify-routing-{args.year}.json")
    if path.exists() and not args.resume:
        parser.error(f"Report already exists: {path}; choose another --output")
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": 2,
        "year": args.year, "status": "running",
        "scope": "Current search results by publicDate; no Bronze or detail verification",
        "note": (
            "All workflow combinations are inventoried, including unflagged and unknown values. "
            "Unflagged does not mean verified correct. Suspected cases are heuristics only. "
            "Reason counts can overlap; suspected counts do not. current_route reflects the "
            "reviewed workflow registry; missing IDs prevent calls. Reasons describe risks in "
            "the legacy stepCode router, not confirmed errors in the current implementation. "
            "Counts are search records per date window, not distinct notices across the year."
        ),
        "days": [],
    }
    current = start
    if args.resume:
        try:
            report, current = load_resume(path, args.year)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            parser.error(str(exc))
    save_report(path, report)
    try:
        with MuasamcongClient(token=settings.MUASAMCONG_TOKEN) as client:
            api = NotifyContractorApi(client)
            while current <= end:
                result = scan_day(api, current, args.page_size)
                report["days"].append(result)
                save_report(path, report)
                print(
                    f"{current}: scanned={result['scanned']} suspected={result['suspected']} "
                    f"reasons={result['reasons']}", flush=True,
                )
                if current == end:
                    break
                current += timedelta(days=1)
        report["status"] = "complete"
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001 -- redact CLI errors, save progress
        report.update(status="incomplete", failed_date=str(current), error=type(exc).__name__,
                      error_message=safe_error(exc))
        save_report(path, report)
        print(f"Stopped at {current}: {type(exc).__name__}. Partial report: {path}", flush=True)
        print(report["error_message"], flush=True)
        return 130 if isinstance(exc, KeyboardInterrupt) else 1
    save_report(path, report)
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    print(f"Report: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
