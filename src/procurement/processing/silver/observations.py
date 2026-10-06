"""Preserve occurrences independently from revision identity and typed projections."""

import json
from datetime import datetime
from decimal import Decimal

from procurement.processing.silver.contracts import (
    MAPPINGS,
    REFERENCES,
    VERSION,
    lookup,
    money,
    text,
)
from procurement.processing.silver.selection import digest


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def transform(namespace, partition, file, ordinal, record):
    raw = record["payload"]
    payload = json.loads(raw, parse_float=Decimal) if isinstance(raw, str) else json.loads(canonical(raw), parse_float=Decimal)
    observation_id = digest([namespace, file["key"], file["sha256"], ordinal])
    observed_at = record["ingested_at"]
    if isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at)
    if observed_at.tzinfo is None:
        raise ValueError("Observation time must have a timezone")
    observation = {
        "observation_id": observation_id, "resource": partition["resource"],
        "source_date": partition["source_date"], "run_id": record["run_id"],
        "table_name": file["table"], "file_key": file["key"], "file_sha256": file["sha256"],
        "row_ordinal": ordinal, "observed_at": observed_at.isoformat(),
        "source_id": record["source_id"], "source_version": record.get("source_version"),
        "content_hash": record["content_hash"], "payload_json": raw if isinstance(raw, str) else canonical(raw),
    }
    result = {"observation": observation, "entity": None, "revision": None,
              "typed": None, "children": [], "references": [], "issues": [], "quarantine": None}

    def issue(code, path, severity="warn"):
        result["issues"].append({"issue_id": digest([observation_id, code, path]),
                                 "observation_id": observation_id, "code": code,
                                 "path": path, "severity": severity})

    kind, roots, scheme, id_field, number, title, amount, currency = MAPPINGS[file["table"]]
    candidates = [value for path in roots if isinstance(value := lookup(payload, path), dict)]
    identities = {text(root.get(id_field)) for root in candidates} - {None}
    if kind == "notice" and file["table"] == "notify_contractor_reoffer_detail":
        identities |= {text(root.get("reofferNo")) for root in candidates} - {None}
    if not candidates or len(identities) != 1:
        code = "missing_root" if not candidates else "missing_or_conflicting_identity"
        result["quarantine"] = {"observation_id": observation_id, "reason": code}
        issue(code, id_field, "error")
        return result
    root = candidates[0]
    identity = identities.pop()
    entity_id = digest([namespace, kind, scheme, identity])
    semantic_hash = digest(payload)
    version = None if kind == "package" else text(record.get("source_version"))
    revision_id = digest([entity_id, version, semantic_hash, VERSION])
    result["entity"] = {"entity_id": entity_id, "namespace": namespace, "entity_type": kind,
                         "id_scheme": scheme, "source_identity": identity}
    result["revision"] = {"revision_id": revision_id, "entity_id": entity_id,
                           "source_version": version, "semantic_hash": semantic_hash,
                           "mapping_version": VERSION["mapping"], "payload_json": canonical(payload)}
    parsed_amount = None
    if amount:
        try:
            parsed_amount = money(root.get(amount))
        except ValueError:
            issue("invalid_money", amount)
    currency_value = text(root.get(currency)) if currency else None
    if parsed_amount is not None and currency_value is None:
        issue("unknown_currency", currency or "currency")
    result["typed"] = {"revision_id": revision_id, "entity_id": entity_id, "entity_type": kind,
                        "business_number": text(root.get(number)), "title": text(root.get(title)),
                        "buyer_id": text(root.get("investorCode")), "buyer_name": text(root.get("investorName")),
                        "amount": parsed_amount, "currency": currency_value,
                        "public_date_raw": text(root.get("publicDate")),
                        "status_raw": text(root.get("status")), "root_json": canonical(root)}
    for target, target_scheme, field in REFERENCES.get(kind, ()):
        value = text(root.get(field))
        result["references"].append({"relationship_id": digest([revision_id, target, field]),
                                     "from_revision_id": revision_id, "target_type": target,
                                     "id_scheme": target_scheme, "target_identity": value,
                                     "field": field})
    # Preserve every list member and its role/path, including nested lots and consortium members.
    def walk(value, path=""):
        if isinstance(value, dict):
            for field, child in value.items():
                walk(child, f"{path}.{field}" if path else field)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                role = path.rsplit(".", 1)[-1]
                child_kind = ("lot" if "lot" in role.lower() else
                              "participant" if any(x in role.lower() for x in ("contractor", "participant", "bidder"))
                              else "item")
                child_id = text(child.get("id")) if isinstance(child, dict) else None
                if not child_id:
                    issue("child_without_source_id", f"{path}[{index}]")
                result["children"].append({"child_id": digest([revision_id, path, index, child]),
                    "revision_id": revision_id, "kind": child_kind, "role": role,
                    "path": path, "ordinal": index, "source_id": child_id,
                    "payload_json": canonical(child)})
                walk(child, f"{path}[{index}]")
    walk(payload)
    return result
