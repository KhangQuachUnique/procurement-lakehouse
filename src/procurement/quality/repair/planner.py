"""Planning and plan validation for Bronze quality repair."""

from pathlib import Path
from typing import Any

from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.adapters import QUALITY_RESOURCES
from procurement.quality.contracts import resolve_route
from procurement.quality.files import now, read_json, write_json


def create_plan(audit_directory: Path | str, config: Any, output: Path | str) -> dict[str, Any]:
    """Build a deterministic repair plan from a completed audit directory."""
    directory = Path(audit_directory)
    state = read_json(directory / "selection.json")
    if state["status"] != "complete" or state["config_hash"] != config.fingerprint:
        raise ValueError("Repair requires a complete audit using the same reviewed config")
    if state["resource"] not in QUALITY_RESOURCES:
        raise ValueError("Unsupported selective repair resource")
    plan: dict[str, Any] = {
        "schema_version": 1,
        "resource": state["resource"],
        "year": state["year"],
        "config_hash": config.fingerprint,
        "storage_namespace": state["storage_namespace"],
        "created_at": now(),
        "days": [],
        "blocked": [],
    }
    for selection in state["selection"]:
        day = read_json(directory / "days" / f"{selection['date']}.json")
        if calculate_content_hash(day) != state["completed"][selection["date"]]:
            raise ValueError("Audit checkpoint changed")
        if day["status"] != "complete":
            plan["blocked"].append({"date": selection["date"], "reason": day["status"]})
            continue
        targets = [row for row in day["rows"] if row["result"]["status"] == "fail"]
        if not targets:
            continue
        if any(
            row["route"] is None or row["context"] is None or row["result"]["status"] == "unresolved"
            for row in day["rows"]
        ):
            plan["blocked"].append({"date": selection["date"], "reason": "unresolved_context_or_route"})
            continue
        plan["days"].append(
            {
                "selection": selection,
                "records": [
                    {key: row[key] for key in ("source_id", "source_version", "table", "content_hash", "context", "route")}
                    | {
                        "refetch": row in targets,
                        "confirmed_errors": [i["code"] for i in row["result"]["issues"] if i["severity"] == "fail"],
                    }
                    for row in day["rows"]
                ],
            }
        )
    plan["estimated_detail_requests"] = sum(
        (6 if row["context"].get("bidMode") == "1_HTHS" else 4) if config.resource == "bid_opening" else 1
        for d in plan["days"]
        for row in d["records"]
        if row["refetch"]
    )
    plan["plan_hash"] = calculate_content_hash(plan)
    if Path(output).exists():
        raise ValueError("Repair plan exists; choose another output")
    write_json(output, plan)
    return plan


def validate_plan(plan: dict[str, Any], config: Any) -> None:
    """Validate that plan has not changed and matches current reviewed config and namespace."""
    expected_hash = calculate_content_hash({key: value for key, value in plan.items() if key != "plan_hash"})
    if plan.get("plan_hash") != expected_hash or plan.get("config_hash") != config.fingerprint:
        raise ValueError("Repair plan or config changed")
    namespace = calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])
    if (
        plan.get("storage_namespace") != namespace
        or plan.get("resource") != config.resource
        or config.resource not in QUALITY_RESOURCES
    ):
        raise ValueError("Repair storage/resource mismatch")
    dates: set[str] = set()
    for day in plan["days"]:
        selection = day["selection"]
        if selection["date"] in dates or selection["resource"] != plan["resource"]:
            raise ValueError("Duplicate day or mismatched resource in plan")
        dates.add(selection["date"])
        for row in day["records"]:
            route = resolve_route(row["context"], config)
            if route.model_dump() != row["route"]:
                raise ValueError("Repair routing no longer matches reviewed plan")
