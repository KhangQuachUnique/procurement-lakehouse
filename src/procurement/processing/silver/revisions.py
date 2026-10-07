"""Deterministic reconciliation across partitions; no dependence on physical row order."""

from collections import defaultdict
from datetime import datetime

from procurement.processing.silver.selection import digest

TABLES = ("observation", "entity", "entity_revision", "revision_observation", "current_entity",
          "project_revision", "plan_revision", "package_revision", "notice_revision",
          "result_revision", "opening_revision", "child", "relationship", "quality_issue", "quarantine")


def assemble(rows):
    tables = {name: {} for name in TABLES}
    observations = defaultdict(list)
    version_revisions = defaultdict(set)
    references = {}
    identities = defaultdict(set)
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
        entity, revision, typed = row["entity"], row["revision"], row["typed"]
        eid, rid = entity["entity_id"], revision["revision_id"]
        tables["entity"][eid] = entity
        tables["entity_revision"][rid] = revision
        tables["revision_observation"][oid] = {"observation_id": oid, "revision_id": rid}
        tables[entity["entity_type"] + "_revision"][rid] = typed
        for child in row["children"]:
            tables["child"][child["child_id"]] = child
        references.update((ref["relationship_id"], ref) for ref in row["references"])
        observations[eid].append((datetime.fromisoformat(observation["observed_at"]), rid))
        version_revisions[(eid, revision["source_version"])].add(rid)
        identities[(entity["entity_type"], entity["id_scheme"], entity["source_identity"])].add(eid)
        if typed["business_number"]:
            identities[(entity["entity_type"], "number", typed["business_number"])].add(eid)
    for eid, observed in observations.items():
        latest = max(time for time, _ in observed)
        current = {rid for time, rid in observed if time == latest}
        tables["current_entity"][eid] = {"entity_id": eid,
            "revision_id": next(iter(current)) if len(current) == 1 else None,
            "selection_status": "selected" if len(current) == 1 else "ambiguous",
            "observed_at": latest.isoformat()}
    for (eid, version), revisions in version_revisions.items():
        if len(revisions) > 1:
            iid = digest([eid, version, "conflicting_revisions"])
            tables["quality_issue"][iid] = {"issue_id": iid, "observation_id": None,
                "code": "conflicting_revisions", "path": eid, "severity": "warn"}
    for key, ref in references.items():
        targets = identities[(ref["target_type"], ref["id_scheme"], ref["target_identity"])]
        status = ("missing" if ref["target_identity"] is None else
                  "unresolved" if not targets else "resolved" if len(targets) == 1 else "ambiguous")
        tables["relationship"][key] = {**ref, "resolution_status": status,
                                       "target_entity_id": next(iter(targets)) if status == "resolved" else None}
    return {table: [rows[key] for key in sorted(rows)] for table, rows in tables.items()}


def validate(tables):
    observations = {r["observation_id"] for r in tables["observation"]}
    accepted = {r["observation_id"] for r in tables["revision_observation"]}
    quarantine = {r["observation_id"] for r in tables["quarantine"]}
    entities = {r["entity_id"] for r in tables["entity"]}
    revisions = {r["revision_id"] for r in tables["entity_revision"]}
    if accepted & quarantine or accepted | quarantine != observations:
        raise ValueError("Occurrence reconciliation failed")
    for row in tables["entity_revision"]:
        if row["entity_id"] not in entities:
            raise ValueError("Revision has no entity")
    for table in ("revision_observation", "child"):
        if any(r["revision_id"] not in revisions for r in tables[table]):
            raise ValueError("Revision foreign key failed")
    for row in tables["relationship"]:
        if row["from_revision_id"] not in revisions or (
            row["target_entity_id"] is not None and row["target_entity_id"] not in entities
        ):
            raise ValueError("Relationship foreign key failed")
    return {"observations": len(observations), "accepted": len(accepted),
            "quarantined": len(quarantine), "issues": len(tables["quality_issue"]),
            "blocking_issues": sum(r["severity"] == "error" for r in tables["quality_issue"]),
            "counts": {table: len(rows) for table, rows in tables.items()},
            "semantic_hash": digest(tables)}
