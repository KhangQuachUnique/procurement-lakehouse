"""Reconstruction of Bronze partitions combining cached refetches and validated records."""

import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.models.bronze import BronzeRecord
from procurement.quality.adapters import fetch_detail
from procurement.quality.audit import assess_record, record_key
from procurement.quality.contracts import RESOURCE_ENDPOINTS as ENDPOINTS
from procurement.quality.contracts import TABLES, resolve_route, validate_detail
from procurement.quality.files import now, read_json, write_json


def fetch_replacement(client: Any, row: dict[str, Any], config: Any, cache: Path | str) -> tuple[Any, dict[str, Any], Any]:
    """Fetch or load cached replacement detail payload and validate against contract."""
    context = row["context"]
    route = resolve_route(context, config)
    cache_key = calculate_content_hash({"row": row, "config": config.fingerprint})
    path = Path(cache) / f"{cache_key}.json"
    evidence: dict[str, Any] = {}
    if path.exists():
        saved = read_json(path)
        evidence = saved.get("collection", {})
        if saved["key"] != cache_key or calculate_content_hash(saved["payload"]) != saved["payload_hash"]:
            raise ValueError("Replacement checkpoint changed")
        payload = saved["payload"]
    else:
        try:
            payload = fetch_detail(client, route, context, evidence)
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            setattr(  # noqa: B010
                exc,
                "diagnostics",
                {
                    **getattr(exc, "diagnostics", {}),
                    "endpoint": ENDPOINTS[route.contract],
                    "request_id": context["id"],
                    "source_id": row["source_id"],
                    "source_version": row["source_version"],
                },
            )
            raise
    result = validate_detail(payload, context, config.contracts[route.contract], thresholds=config.thresholds)
    result["collection"] = evidence
    actual = result["identity"]
    if actual.get("notifyVersion") != row["source_version"]:
        raise ValueError("source_changed: replacement version differs from the baseline")
    if result["status"] not in {"pass", "warn"} or actual.get("notifyNo") != row["source_id"]:
        raise ValueError("Replacement failed detail contract/identity checks")
    if not path.exists():
        write_json(
            path,
            {
                "key": cache_key,
                "payload_hash": calculate_content_hash(payload),
                "payload": payload,
                "context": context,
                "config_hash": config.fingerprint,
                "observed_at": now(),
                "collection": evidence,
            },
        )
    return payload, result, route


def reconstruct(
    client: Any,
    baseline: list[tuple[str, dict[str, Any]]],
    references: list[dict[str, Any]],
    config: Any,
    run_id: str,
    cache: Path | str,
    *,
    detail_workers: int = 1,
) -> tuple[dict[str, list[BronzeRecord]], list[dict[str, Any]], dict[str, int]]:
    """Assemble reconstructed tables and audit observations for a repaired partition."""
    planned = {record_key(row): row for row in references}
    replacements: dict[tuple[Any, Any], tuple[Any, dict[str, Any], Any]] = {}
    targets = [row for row in references if row["refetch"]]
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="quality-detail") as pool:
        futures = {pool.submit(fetch_replacement, client, row, config, cache): record_key(row) for row in targets}
        try:
            for future in as_completed(futures):
                replacements[futures[future]] = future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            raise
    tables: dict[str, list[BronzeRecord]] = defaultdict(list)
    observations: list[dict[str, Any]] = []
    counts: Counter[str] = Counter(copied=0, refetched=0, moved=0)
    for table, record in baseline:
        row = planned[record_key(record)]
        old_payload = record["payload"]
        if isinstance(old_payload, str):
            old_payload = json.loads(old_payload)
        payload, target = old_payload, table
        ingested_at = record["ingested_at"]
        if row["refetch"]:
            payload, result, route = replacements[record_key(row)]
            target = TABLES[route.contract]
            ingested_at = datetime.now(UTC)
            counts["refetched"] += 1
            counts["moved"] += int(target != table)
        else:
            assessment = assess_record(table, record, row["context"], config)
            result = assessment["result"]
            if result["status"] not in {"pass", "warn"}:
                raise ValueError("Unselected baseline record fails validation; create a new plan")
            counts["copied"] += 1
        replacement = BronzeRecord(
            source_id=record["source_id"],
            source_version=record.get("source_version"),
            source_date=record["source_date"],
            run_id=run_id,
            ingested_at=ingested_at,
            content_hash=calculate_content_hash(payload),
            payload=payload,
        )
        tables[target].append(replacement)
        observations.append(
            {
                "context": row["context"],
                "result": result,
                "config_hash": config.fingerprint,
                "rule_version": config.version,
                "contract": row["route"]["contract"],
                "endpoint": ENDPOINTS[row["route"]["contract"]],
                "baseline_run_id": record["run_id"],
                "baseline_hash": record["content_hash"],
                "action": "refetch" if row["refetch"] else "copy",
            }
        )
    return dict(tables), observations, dict(counts)
