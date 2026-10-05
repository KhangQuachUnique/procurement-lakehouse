"""Minimal detail contracts; never repair source payloads or infer success from size."""

import json
import tomllib
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from procurement.ingestion.engine.metadata import calculate_content_hash

WORKFLOW_FIELDS = ("stepCode", "processApply", "bidForm", "isInternet", "bidMode")
ENGINE_VERSION = "quality-3"
ENDPOINTS = {
    "standard": "/o/egp-portal-contractor-selection-v2/services/lcnt_tbmt_ttc_ldt",
    "reoffer": "/o/egp-portal-contractor-selection-v2/services/online-reoffer/detail",
    "vk_adb": "/o/egp-portal-contractor-selection-v2/services/lcnt_tbmt_ttc_vk_adb",
}
RESOURCE_ENDPOINTS = ENDPOINTS | {
    "bid_opening": "/o/egp-portal-contractor-selection-v2/services/exposeldtkqmt/bid-notification-p/notify",
}
TABLES = {kind: f"notify_contractor_{kind}_detail" for kind in ENDPOINTS}
TABLES["bid_opening"] = "bid_opening_detail"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Thresholds(StrictModel):
    empty_ratio: float = Field(default=.8, ge=0, le=1)
    min_fields: int = Field(default=10, ge=1)
    small_fraction: float = Field(default=.25, gt=0, lt=1)
    min_cohort: int = Field(default=30, ge=2)
    min_cluster: int = Field(default=10, ge=2)


class Contract(StrictModel):
    assembly: bool = False
    roots: list[str] = Field(min_length=1)
    id_fields: list[str] = Field(default_factory=lambda: ["id"], min_length=1)
    number_fields: list[str] = Field(default_factory=lambda: ["notifyNo"], min_length=1)
    version_fields: list[str] = Field(default_factory=lambda: ["notifyVersion"], min_length=1)
    allow_search_version: bool = False
    fields: list[str] = Field(default_factory=lambda: ["*"])


class Route(StrictModel):
    name: str = Field(min_length=1)
    contract: str
    evidence: str = Field(min_length=1)
    match: dict[str, Any] = Field(min_length=1)
    # Excluded fields must be present with the expected type, not missing/unknown.
    exclude: dict[str, Any] = Field(default_factory=dict)


class QualityConfig(StrictModel):
    version: str = Field(min_length=1)
    resource: str = "notify_contractor"
    thresholds: Thresholds = Field(default_factory=Thresholds)
    contracts: dict[str, Contract]
    routes: list[Route] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_routes(self):
        names = set()
        for route in self.routes:
            if route.name in names or route.contract not in self.contracts:
                raise ValueError("Duplicate route name or missing contract")
            if route.contract not in RESOURCE_ENDPOINTS:
                raise ValueError("Route endpoint is not in the known read-only endpoint registry")
            if (set(route.match) | set(route.exclude)) - {*WORKFLOW_FIELDS, "id", "notifyNo", "notifyVersion"}:
                raise ValueError("Unsupported routing field")
            names.add(route.name)
        return self

    @property
    def fingerprint(self):
        data = self.model_dump()
        for contract in data["contracts"].values():
            if not contract["assembly"]:
                contract.pop("assembly")
        return calculate_content_hash({"engine": ENGINE_VERSION, "config": data})


def load_config(path=None, *, resource="notify_contractor"):
    from procurement.common.settings import settings

    selected = path or (settings.NOTIFY_QUALITY_CONFIG if resource == "notify_contractor" else None)
    default = "bid_opening.toml" if resource == "bid_opening" else "notify.toml"
    file = Path(selected) if selected else Path(__file__).with_name(default)
    return QualityConfig.model_validate(tomllib.loads(file.read_text(encoding="utf-8")))


def resolve_route(context, config):
    matches = [route for route in config.routes if all(
        key in context and type(context[key]) is type(value) and context[key] == value
        for key, value in route.match.items()
    ) and all(
        key in context and type(context[key]) is type(value)
        and context[key] != "" and context[key] != value
        for key, value in route.exclude.items()
    )]
    if len(matches) != 1:
        raise DetailValidationError("ambiguous_route" if matches else "unresolved_route")
    return matches[0]


class DetailValidationError(ValueError):
    pass


def lookup(value, path):
    for key in path.split(".") if path else []:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def empty(value):
    return value is None or value == "" or value == [] or value == {}


def issue(code, severity, path="", **evidence):
    return {"code": code, "severity": severity, "path": path, "evidence": evidence}


def finish(result):
    severities = {item["severity"] for item in result["issues"]}
    result["status"] = next((s for s in ("fail", "unresolved", "warn") if s in severities), "pass")
    return result


def _shape(value, *, nulls=False):
    if isinstance(value, dict):
        return {key: _shape(item, nulls=nulls) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return sorted({json.dumps(_shape(item, nulls=nulls), sort_keys=True) for item in value})
    return ("empty" if empty(value) else "value") if nulls else type(value).__name__


def validate_detail(payload, context, contract, *, thresholds=None):
    if contract.assembly:
        from procurement.quality.bid_opening import validate_assembly
        return validate_assembly(payload, context, contract, thresholds=thresholds)
    thresholds = thresholds or Thresholds()
    result = {"issues": [], "metrics": {}, "identity": {}}
    issues = result["issues"]
    if not isinstance(payload, dict) or not payload:
        issues.append(issue("empty_payload", "fail"))
        return finish(result)
    roots = [(path, lookup(payload, path)) for path in contract.roots]
    usable = [(path, root) for path, root in roots if isinstance(root, dict) and root]
    if not usable:
        issues.append(issue("missing_business_root", "fail", expected=contract.roots))
        return finish(result)
    # LDT legitimately repeats the same notice in both supported roots. Check
    # consistency instead of rejecting the duplicate representation outright.
    for field, aliases in (("id", contract.id_fields), ("notifyNo", contract.number_fields),
                           ("notifyVersion", contract.version_fields)):
        values = {str(lookup(candidate, key)) for _, candidate in usable for key in aliases
                  if not empty(lookup(candidate, key))}
        if len(values) > 1:
            issues.append(issue("conflicting_business_roots", "fail", field))
    path, root = usable[0]
    for field, aliases in (("id", contract.id_fields), ("notifyNo", contract.number_fields),
                           ("notifyVersion", contract.version_fields)):
        values = [str(lookup(root, key)) for key in aliases if not empty(lookup(root, key))]
        if len(set(values)) > 1:
            issues.append(issue("conflicting_identity_aliases", "fail", f"{path}.{field}"))
        actual = values[0] if values else None
        expected = context.get(field)
        if actual is None and field != "notifyVersion":
            issues.append(issue("missing_detail_identity", "fail", f"{path}.{field}"))
        elif actual is None and not contract.allow_search_version:
            issues.append(issue("missing_detail_version", "fail", f"{path}.{field}"))
        if actual is not None and not empty(expected) and actual != str(expected):
            issues.append(issue("identity_mismatch", "fail", f"{path}.{field}",
                                expected=str(expected), actual=actual))
        if field != "notifyVersion" and empty(expected):
            issues.append(issue("missing_identity_context", "unresolved", field))
        result["identity"][field] = actual or (
            str(expected) if field == "notifyVersion" and not empty(expected) else None
        )
    if payload.get("error") or payload.get("success") is False:
        issues.append(issue("error_envelope", "fail"))
    fields = list(root.values()) if contract.fields == ["*"] else [
        lookup(root, field) for field in contract.fields
    ]
    count = len(fields)
    ratio = sum(empty(value) for value in fields) / count if count else 0
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    result["metrics"] = {
        "payload_bytes": len(canonical.encode("utf-8")), "fields": count,
        "populated_fields": sum(not empty(value) for value in fields),
        "null_ratio": sum(value is None for value in fields) / count if count else 0,
        "empty_ratio": ratio, "content_hash": calculate_content_hash(payload),
        "shape_hash": calculate_content_hash(_shape(root)),
        "null_pattern_hash": calculate_content_hash(_shape(root, nulls=True)),
    }
    if count >= thresholds.min_fields and ratio >= thresholds.empty_ratio:
        issues.append(issue("high_empty_ratio", "warn", path, ratio=ratio))
    return finish(result)


def cohort_warnings(rows, thresholds):
    """Second pass over reports, scoped by contract and exact workflow; warnings only."""
    cohorts = defaultdict(list)
    for row in rows:
        if row["result"]["status"] in {"pass", "warn"}:
            key = (row.get("contract"), json.dumps(row.get("workflow", {}), sort_keys=True))
            cohorts[key].append(row)
    for group in cohorts.values():
        sizes = [row["result"]["metrics"]["payload_bytes"] for row in group]
        baseline = median(sizes) if len(sizes) >= thresholds.min_cohort else None
        identities = defaultdict(set)
        small_sizes = defaultdict(set)
        for row in group:
            metrics = row["result"]["metrics"]
            key = (row["source_id"], row.get("source_version"))
            identities[metrics["content_hash"]].add(key)
            if (baseline and metrics["payload_bytes"] < baseline * thresholds.small_fraction
                    and metrics["empty_ratio"] >= thresholds.empty_ratio):
                small_sizes[metrics["payload_bytes"]].add(key)
        for row in group:
            result = row["result"]
            metrics = result["metrics"]
            if baseline and metrics["payload_bytes"] < baseline * thresholds.small_fraction:
                result["issues"].append(issue("small_payload", "warn", median_bytes=baseline))
            if len(small_sizes[metrics["payload_bytes"]]) >= thresholds.min_cluster:
                result["issues"].append(issue("small_empty_cluster", "warn"))
            if len(identities[metrics["content_hash"]]) > 1:
                result["issues"].append(issue("shared_payload_across_identities", "warn"))
            finish(result)
    return Counter(row["result"]["status"] for row in rows)
