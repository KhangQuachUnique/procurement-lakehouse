"""Deterministic reconciliation across partitions; pure relational models with zero graph overhead."""

from collections import defaultdict
from datetime import datetime
from typing import Any

from procurement.silver.domain.schema import get_child_schemas, get_silver_schemas
from procurement.silver.domain.selection import digest

SCHEMAS = get_silver_schemas()
CHILD_SCHEMAS = get_child_schemas()
TABLES = tuple(SCHEMAS)


def assemble(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Assemble transformed records into clean relational Silver tables.

    No intermediate graph tables (entity, entity_revision, relationship, child).
    Revisions directly store observation_id and is_current.
    """
    tables: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in TABLES}
    observations = defaultdict(list)
    version_revisions = defaultdict(set)

    for row in rows:
        observation = row["observation"]
        oid = observation["observation_id"]
        if oid in tables["observation"]:
            raise ValueError("Duplicate physical observation")
        tables["observation"][oid] = observation

        for issue in row["issues"]:
            tables["quality_issue"][issue["issue_id"]] = issue

        if row["quarantine"]:
            tables["quarantine"][oid] = row["quarantine"]
            continue

        entity = row["entity"]
        revision = row["revision"]
        typed = row["typed"]
        eid = entity["entity_id"]
        rid = revision["revision_id"]
        target_table = f"{entity['entity_type']}_revision"

        typed["observation_id"] = oid
        if target_table in tables:
            tables[target_table][rid] = typed

        for child in row["children"]:
            target = child.get("typed_table")
            if not target:
                continue
            if target not in CHILD_SCHEMAS:
                raise ValueError(f"Unsupported typed child table: {target}")
            typed_child = child.get("typed_attributes")
            child_id_key = (
                typed_child.get(f"{target}_id")
                or typed_child.get("child_id")
                or typed_child.get("location_id")
                or typed_child.get("lot_id")
                if isinstance(typed_child, dict)
                else None
            )
            if not isinstance(typed_child, dict) or (
                child_id_key != child["child_id"] or typed_child.get("revision_id") != rid
            ):
                raise ValueError("Typed child identity must match its source child")
            if target in tables:
                tables[target][child["child_id"]] = typed_child

        observations[eid].append((datetime.fromisoformat(observation["observed_at"]), rid))
        version_revisions[(eid, revision["source_version"])].add(rid)

    # Determine latest active snapshot (is_current) per entity
    current_rids = set()
    for observed in observations.values():
        latest = max(time for time, _ in observed)
        current = {rid for time, rid in observed if time == latest}
        if len(current) == 1:
            current_rids.add(next(iter(current)))

    for table_name, table_rows in tables.items():
        if table_name.endswith("_revision"):
            for rid, row_dict in table_rows.items():
                row_dict["is_current"] = rid in current_rids

    # Quality check for conflicting versions
    for (eid, version), revisions in version_revisions.items():
        if len(revisions) > 1:
            iid = digest([eid, version, "conflicting_revisions"])
            tables["quality_issue"][iid] = {
                "issue_id": iid,
                "observation_id": None,
                "code": "conflicting_revisions",
                "path": eid,
                "severity": "warn",
            }

    return {table: [rows_map[k] for k in sorted(rows_map)] for table, rows_map in tables.items()}


def validate(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Validate 1-hop relational integrity, completeness, and lack of quarantine leakage."""
    observations = {r["observation_id"] for r in tables.get("observation", [])}
    quarantine = {r["observation_id"] for r in tables.get("quarantine", [])}

    # Collect accepted observation IDs directly from all revision tables
    accepted: set[str] = set()
    valid_revisions: set[str] = set()
    for table_name, rows in tables.items():
        if table_name.endswith("_revision"):
            for r in rows:
                if r.get("observation_id"):
                    accepted.add(r["observation_id"])
                if r.get("revision_id"):
                    valid_revisions.add(r["revision_id"])

    if accepted & quarantine or accepted | quarantine != observations:
        raise ValueError("Occurrence reconciliation failed")

    # Verify foreign key from all typed sub-tables to revision tables
    for name in CHILD_SCHEMAS:
        if name in tables:
            for row in tables[name]:
                if row.get("revision_id") not in valid_revisions:
                    raise ValueError(f"Sub-table {name} foreign key revision_id failed")

    return {
        "observations": len(observations),
        "accepted": len(accepted),
        "quarantined": len(quarantine),
        "issues": len(tables.get("quality_issue", [])),
        "blocking_issues": sum(r["severity"] == "error" for r in tables.get("quality_issue", [])),
        "counts": {table: len(rows) for table, rows in tables.items()},
        "semantic_hash": digest(tables),
    }


__all__ = ["CHILD_SCHEMAS", "SCHEMAS", "TABLES", "assemble", "validate"]
