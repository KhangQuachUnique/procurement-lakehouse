"""Search snapshots and bounded endpoint comparisons. No automatic rule promotion."""

from collections import defaultdict
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import httpx

from procurement.common.dates import api_day_window, today_vn, validate_closed_range
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.pagination import iter_search_pages
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.ingestion.sources.muasamcong.search import search_document_key
from procurement.quality.contracts import ENDPOINTS, WORKFLOW_FIELDS, validate_detail
from procurement.quality.files import now, read_json, safe_error, write_json

CONTEXT_FIELDS = ("id", "notifyId", "notifyNo", "notifyVersion", "publicDate",
                  "publicDateKqmt", "bidOpenDate", "bidRealityOpenDate", *WORKFLOW_FIELDS)


def snapshot(api, year, directory, *, resume=False, page_size=50):
    directory = Path(directory)
    path = directory / "index.json"
    start, end = date(year, 1, 1), date(year, 12, 31)
    validate_closed_range(start, end, today=today_vn())
    if path.exists() and not resume:
        raise ValueError("Snapshot exists; use --resume")
    index = read_json(path) if resume else {
        "schema_version": 1, "year": year, "days": {}, "created_at": now(),
    }
    if index["year"] != year or index["schema_version"] != 1:
        raise ValueError("Snapshot year/schema mismatch")
    index.update(status="running")
    write_json(path, index)
    current = start
    try:
        while current <= end:
            key = str(current)
            day_path = directory / "days" / f"{key}.json"
            if key in index["days"]:
                if calculate_content_hash(read_json(day_path)) != index["days"][key]:
                    raise ValueError(f"Snapshot checkpoint changed: {key}")
            else:
                window_from, window_to = api_day_window(current)
                items = []
                for _, response in iter_search_pages(
                    api.search, window_from=window_from, window_to=window_to,
                    page_size=page_size, search_key=search_document_key,
                ):
                    items.extend({field: item.get(field) for field in CONTEXT_FIELDS}
                                 for item in response["page"]["content"])
                day = {"date": key, "observed_at": now(), "items": items}
                write_json(day_path, day)
                index["days"][key] = calculate_content_hash(day)
                write_json(path, index)
                print(f"snapshot {key}: {len(items)} records", flush=True)
            current += timedelta(days=1)
    except BaseException:
        index.update(status="incomplete", failed_date=str(current))
        write_json(path, index)
        raise
    index.update(status="complete", completed_at=now())
    index.pop("failed_date", None)
    write_json(path, index)
    return index


def load_snapshot(directory, *, require_complete=True):
    directory = Path(directory)
    index = read_json(directory / "index.json")
    if require_complete and index["status"] != "complete":
        raise ValueError("Search snapshot is incomplete")
    if require_complete:
        first, last = date(index["year"], 1, 1), date(index["year"], 12, 31)
        expected = {str(first + timedelta(days=offset)) for offset in range((last - first).days + 1)}
        if set(index["days"]) != expected:
            raise ValueError("Completed search snapshot does not contain every day of its year")
    days = {}
    for source_date, digest in sorted(index["days"].items()):
        if str(date.fromisoformat(source_date)) != source_date:
            raise ValueError("Invalid snapshot date")
        day = read_json(directory / "days" / f"{source_date}.json")
        if calculate_content_hash(day) != digest or day["date"] != source_date:
            raise ValueError(f"Search snapshot changed: {source_date}")
        days[source_date] = day["items"]
    return index, days


def sample_workflows(days):
    groups = defaultdict(list)
    for source_date, items in sorted(days.items()):
        for item in items:
            workflow = {key: item.get(key) for key in WORKFLOW_FIELDS}
            groups[calculate_content_hash(workflow)].append({**item, "source_date": source_date})
    result = []
    for key, items in sorted(groups.items()):
        unique = {}
        for item in sorted(items, key=lambda row: (row["source_date"], str(row.get("id")))):
            unique.setdefault(item.get("id"), item)
        ordered = list(unique.values())
        positions = sorted({0, len(ordered) // 2, len(ordered) - 1})
        result.append({"key": key, "workflow": {f: ordered[0].get(f) for f in WORKFLOW_FIELDS},
                       "records": len(items), "samples": [ordered[i] for i in positions]})
    return result


class ProbeLimitError(RuntimeError):
    pass


class ProbeBudget(RequestBudget):
    """Count physical attempts, including client retries, before sending each request."""

    def __init__(self, limit, *, used=0, checkpoint=lambda used: None):
        super().__init__(1)
        self.limit, self.used, self.checkpoint = limit, used, checkpoint

    @contextmanager
    def request(self):
        with super().request():
            if self.used >= self.limit:
                raise ProbeLimitError("Endpoint evidence request budget exhausted")
            self.used += 1
            self.checkpoint(self.used)
            yield


def classify_sample(checks):
    if set(checks) != set(ENDPOINTS) or any(
        "error" in check and check.get("http_status") not in {400, 404, 405, 422, 500}
        for check in checks.values()
    ):
        return "unresolved", None
    # Verification is positive evidence on the sampled ID, not a claim that
    # alternatives can never work. HTTP errors are retained, never treated as
    # successful contracts. Transport/auth/rate-limit uncertainty blocks promotion.
    valid = [kind for kind, check in checks.items()
             if check.get("result", {}).get("status") in {"pass", "warn"}]
    return (("verified", valid[0]) if len(valid) == 1 else
            ("ambiguous", None) if valid else ("unresolved", None))


def probe(client, groups, config, path, report):
    """Persist after every endpoint; HTTP/auth errors are not evidence against a route."""
    report["status"] = "running"
    report["verification_scope"] = (
        "Exactly one endpoint returned a contract-valid detail on each sampled ID. "
        "HTTP errors on alternatives are retained and do not prove endpoint exclusivity."
    )
    for group in groups:
        report["workflows"].setdefault(group["key"], {
            **group, "checks": {}, "status": "unresolved", "contract": None,
        })
    try:
        for group in groups:
            target = report["workflows"].setdefault(group["key"], {
                **group, "checks": {}, "status": "unresolved", "contract": None,
            })
            for sample in group["samples"]:
                sample_key = calculate_content_hash(sample)
                checks = target["checks"].setdefault(sample_key, {"context": sample, "endpoints": {}})
                for kind, endpoint in ENDPOINTS.items():
                    if kind not in config.contracts:
                        continue
                    previous = checks["endpoints"].get(kind)
                    if previous and ("error" not in previous or previous.get("http_status") in {
                        400, 404, 405, 422, 500,
                    }):
                        continue
                    entry = {"endpoint": endpoint, "request_id": sample.get("id"), "at": now()}
                    try:
                        payload = client.post(endpoint, {"id": sample["id"]})
                        entry.update(result=validate_detail(
                            payload, sample, config.contracts[kind], thresholds=config.thresholds,
                        ), response_keys=sorted(payload), payload=payload)
                    except (httpx.HTTPError, ValueError, KeyError) as exc:
                        entry["error"] = safe_error(exc)
                        checks["endpoints"][kind] = entry
                        write_json(path, report)
                        if isinstance(exc, httpx.HTTPStatusError):
                            entry["http_status"] = exc.response.status_code
                            entry["response_excerpt"] = safe_error(ValueError(exc.response.text[:1000]))["message"]
                            write_json(path, report)
                        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code not in {
                            401, 403, 429,
                        }:
                            continue  # an HTTP error remains unresolved, not a negative contract match
                        raise
                    checks["endpoints"][kind] = entry
                    write_json(path, report)
                checks["status"], checks["contract"] = classify_sample(checks["endpoints"])
            outcomes = {(check.get("status"), check.get("contract"))
                        for check in target["checks"].values()}
            if len(outcomes) == 1 and next(iter(outcomes))[0] == "verified":
                target["status"], target["contract"] = next(iter(outcomes))
            elif any(status == "ambiguous" for status, _ in outcomes) or len(outcomes) > 1:
                target["status"], target["contract"] = "ambiguous", None
            write_json(path, report)
            print(f"workflow {group['key'][:8]}: {target['status']}", flush=True)
    except BaseException:
        report["status"] = "incomplete"
        write_json(path, report)
        raise
    report.update(status="complete", completed_at=now())
    write_json(path, report)
    return report


def revalidate_evidence(report, config):
    """Reassess saved raw responses after validator changes, with no network requests."""
    report["config_hash"] = config.fingerprint
    for group in report["workflows"].values():
        group.update(status="unresolved", contract=None)
        for check in group["checks"].values():
            for kind, entry in check["endpoints"].items():
                if "payload" in entry:
                    entry["result"] = validate_detail(
                        entry["payload"], check["context"], config.contracts[kind],
                        thresholds=config.thresholds,
                    )
            check["status"], check["contract"] = classify_sample(check["endpoints"])
        outcomes = {(check.get("status"), check.get("contract")) for check in group["checks"].values()}
        if len(group["checks"]) == len(group["samples"]) and len(outcomes) == 1:
            group["status"], group["contract"] = next(iter(outcomes))
        elif any(status == "ambiguous" for status, _ in outcomes) or len(outcomes) > 1:
            group["status"] = "ambiguous"
    return report


def export_config(report, config, output):
    """Explicit export of exact observed workflow rules; no broad wildcard inference."""
    import json

    from procurement.quality.contracts import (
        DetailValidationError,
        QualityConfig,
        Route,
        resolve_route,
    )

    if report["status"] != "complete" or report["config_hash"] != config.fingerprint:
        raise ValueError("Completed evidence using this config is required")
    result = config.model_copy(deep=True)
    result.version = config.version + "+evidence-" + calculate_content_hash(report)[:12]
    for key, group in report["workflows"].items():
        if group["status"] != "verified" or any(value is None for value in group["workflow"].values()):
            continue
        try:
            existing = resolve_route(group["workflow"], result)
        except DetailValidationError as exc:
            if str(exc) != "unresolved_route":
                raise
        else:
            if existing.contract != group["contract"]:
                raise ValueError("Evidence conflicts with configured route; review the family rule")
            # A broad family rule already covers this exact workflow. Avoid overlap.
            continue
        route = Route(name=f"observed-{key[:16]}", contract=group["contract"],
                      evidence=f"sha256:{calculate_content_hash(report)}:workflow:{key}",
                      match=group["workflow"])
        # Remove only same-endpoint narrower rules now covered by this exact workflow.
        result.routes = [old for old in result.routes if not (
            old.contract == route.contract and all(old.match.get(k) == v for k, v in route.match.items())
        )]
        result.routes.append(route)
    result = QualityConfig.model_validate(result.model_dump())
    lines = [f"version = {json.dumps(result.version)}", f"resource = {json.dumps(result.resource)}"]
    for section, values in [("thresholds", result.thresholds.model_dump()), *[
        (f"contracts.{name}", contract.model_dump()) for name, contract in result.contracts.items()
    ]]:
        lines.extend(["", f"[{section}]"])
        lines.extend(f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in values.items())
    for route in result.routes:
        lines.extend(["", "[[routes]]", f"name = {json.dumps(route.name)}",
                      f"contract = {json.dumps(route.contract)}",
                      f"evidence = {json.dumps(route.evidence)}", "[routes.match]"])
        lines.extend(f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in route.match.items())
        if route.exclude:
            lines.append("[routes.exclude]")
            lines.extend(f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in route.exclude.items())
    path = Path(output)
    if path.exists():
        raise ValueError("Config output exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result
