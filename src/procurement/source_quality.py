"""Approved null-root quarantine policy; retain historical exact-one v2 receipts."""

import hashlib
import json

from pydantic import BaseModel, ConfigDict

APPROVED = {
    "resource": "khlcnt",
    "source_date": "2024-04-10",
    "source_id": "f705dfb8-e887-4d6e-aba5-bffba046de24",
    "source_version": "00",
    "observation_id": "588310dd67efc4423fba0f7c12d80746603041e2c45a78676689baa7a6d01765",
    "table_name": "khlcnt_plan_detail",
    "file_key": "objects/bronze/muasamcong/khlcnt_plan_detail/source_date=2024-04-10/run_id=5392438a7198428d98991c2b588f5481/compact-000000.parquet",
    "file_sha256": "ff3c24ba32779ead9e40132fbcab2d0efd902893b5f0bb4b2e34b69eefff9dd2",
    "row_ordinal": 536,
    "content_hash": "a6bed0603660c1b5445e7864dfa58c61c8960400d9eebca71691ff5e23263a87",
    "reason": "missing_root",
}
APPROVAL = {
    "date": "2026-11-07",
    "requester": "gateway-owner",
    "decision": "Duyệt ngoại lệ đúng hồ sơ này",
    "exceptions": [APPROVED],
}


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


APPROVAL_DIGEST = digest(APPROVAL)


class SourceException(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    resource: str
    source_date: str
    source_id: str
    source_version: str | None
    observation_id: str
    table_name: str
    file_key: str
    file_sha256: str
    row_ordinal: int
    content_hash: str
    reason: str

    def assert_approved(self):
        if self.model_dump() != APPROVED:
            raise ValueError("Source exception is not the exact approved observation")


def validate_policy(version, quarantined, blocking, exceptions, approval_digest, status):
    if version == 1:
        if quarantined or blocking or exceptions or approval_digest or status != "STRICT":
            raise ValueError("V1 retains strict zero-quarantine contract")
    elif version == 2:
        if (
            quarantined != 1
            or blocking != 1
            or len(exceptions) != 1
            or approval_digest != APPROVAL_DIGEST
            or status != "ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION"
        ):
            raise ValueError("V2 requires the exact approved exception/counts/policy")
        exceptions[0].assert_approved()
    elif version == 3:
        if (
            approval_digest != NULL_POLICY_DIGEST
            or quarantined != len(exceptions)
            or blocking != len(exceptions)
            or len({e.observation_id for e in exceptions}) != len(exceptions)
            or status != ("ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION" if exceptions else "STRICT")
        ):
            raise ValueError("Null policy counts/digest/status differ")
        for exception in exceptions:
            if not isinstance(exception, NullSourceException):
                raise ValueError("Null exception proof missing")  # noqa: TRY004 -- value rejected by Pydantic validator
            exception.assert_null_policy()
    else:
        raise ValueError("Unsupported readiness version")


def verify_exception_report(report):
    """Check actual typed governance+raw lineage, not just an error count."""
    import pyarrow.parquet as pq

    from procurement.ingestion.engine.metadata import calculate_content_hash

    if len(report["days"]) != 1 or report["resource"] != APPROVED["resource"]:
        raise ValueError("Unapproved exception resource/cohort")
    entry = report["days"][0]
    quality = entry["quality"]
    files = entry["files"]
    if (
        entry["date"] != APPROVED["source_date"]
        or entry["status"] != "needs_review"
        or quality["quarantined"] != 1
        or quality["blocking_issues"] != 1
        or quality["accepted"] + 1 != quality["observations"]
    ):
        raise ValueError("Exception cohort/counts differ")
    q = pq.ParquetFile(files["quarantine"]).read().to_pylist()
    errors = [
        r
        for r in pq.ParquetFile(files["quality_issue"]).read().to_pylist()
        if r["severity"] == "error"
    ]
    oid = APPROVED["observation_id"]
    if q != [{"observation_id": oid, "reason": "missing_root"}] or len(errors) != 1:
        raise ValueError("Unapproved quarantine/error present")
    if (
        errors[0]["observation_id"] != oid
        or errors[0]["code"] != "missing_root"
        or errors[0]["path"] != "root"
    ):
        raise ValueError("Error identity/reason differs")
    observed = pq.ParquetFile(files["observation"]).read().to_pylist()
    rows = [r for r in observed if r["observation_id"] == oid]
    if len(rows) != 1 or len(observed) != quality["observations"]:
        raise ValueError("Exception observation missing/duplicate or count mismatch")
    row = rows[0]
    for key, value in APPROVED.items():
        if key == "reason":
            continue
        actual = str(row[key]) if key == "source_date" else row[key]
        if actual != value:
            raise ValueError("Exception raw lineage differs: " + key)
    payload = json.loads(row["payload_json"])
    if (
        calculate_content_hash(payload) != APPROVED["content_hash"]
        or payload.get("bidPoBidpPlanProjectDetailView") is not None
    ):
        raise ValueError("Exception payload is not the approved missing root")
    for name, path in files.items():
        if name.endswith("_revision"):
            values = pq.ParquetFile(path).read(columns=["observation_id"]).to_pylist()
            if any(r["observation_id"] == oid for r in values):
                raise ValueError("Quarantined observation leaked into domain revisions")
    if not any(
        f["key"] == APPROVED["file_key"]
        and f["sha256"] == APPROVED["file_sha256"]
        and f["table"] == APPROVED["table_name"]
        for f in entry["input_files"]
    ):
        raise ValueError("Approved Bronze file not in producer lineage")
    return SourceException(**APPROVED)


NULL_POLICY = {
    "policy": "NULL_ROOT_QUARANTINE_V1",
    "date": "2026-11-07",
    "requester": "gateway-owner",
    "decision": "Thống nhất các hồ sơ bị null cho vào quarantine ngoại lệ hết, không dùng trong tính toán",
    "action": "retain_raw_observation_issue_quarantine_without_domain_or_fact_contribution",
    "allowed_reason": "missing_root",
}
NULL_POLICY_DIGEST = digest(NULL_POLICY)
ROOT_PATHS = {
    "khlcnt_plan_detail": ("khlcnt", ("bidPoBidpPlanProjectDetailView",)),
    "khlcnt_bid_package_detail": ("khlcnt", ("$",)),
    "contractor_result_detail": ("contractor_result", ("bideContractorInputResultDTO",)),
    "notify_contractor_standard_detail": (
        "notify_contractor",
        ("bidoNotifyContractorM", "bidNoContractorResponse.bidNotification"),
    ),
    "notify_contractor_vk_adb_detail": ("notify_contractor", ("bidoNotifyContractorP",)),
    "notify_contractor_reoffer_detail": ("notify_contractor", ("$",)),
}


class NullSourceException(SourceException):
    root_path: str

    def assert_null_policy(self):
        import re
        from datetime import date

        date.fromisoformat(self.source_date)
        if (
            self.reason != "missing_root"
            or self.table_name not in ROOT_PATHS
            or ROOT_PATHS[self.table_name][0] != self.resource
            or self.root_path not in (*ROOT_PATHS[self.table_name][1], "$")
            or not self.source_id
            or not self.observation_id
            or not self.file_key
            or self.row_ordinal < 0
            or any(
                not re.fullmatch("[0-9a-f]{64}", value)
                for value in (self.content_hash, self.file_sha256)
            )
        ):
            raise ValueError("Exception does not satisfy approved null-root policy")


def null_path(payload, table):
    """Require an explicit null authoritative root; missing/malformed JSON is not null."""
    if table not in ROOT_PATHS:
        raise ValueError("Unsupported null source family")
    if payload is None:
        return "$"
    if not isinstance(payload, dict):
        raise ValueError("Malformed payload, not a null root")  # noqa: TRY004 -- semantic payload rejection
    found = []
    for path in ROOT_PATHS[table][1]:
        if path == "$":
            continue
        value = payload
        present = True
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                present = False
                break
            value = value[part]
        if present:
            if value is not None:
                raise ValueError("Authoritative root exists or is malformed, not null")
            found.append(path)
    if not found:
        raise ValueError("No explicit null authoritative root")
    return found[0]


def verify_null_report(report):
    """Approve every proven null, but fail on other errors, count/lineage drift or domain leakage."""
    import pyarrow.parquet as pq

    from procurement.ingestion.engine.metadata import calculate_content_hash

    if len(report["days"]) != 1:
        raise ValueError("Null proof requires one exact producer cohort")
    entry = report["days"][0]
    quality = entry["quality"]
    files = entry["files"]
    if entry["status"] != "needs_review":
        raise ValueError("Original review status must be retained")
    quarantine = pq.ParquetFile(files["quarantine"]).read().to_pylist()
    errors = [
        e
        for e in pq.ParquetFile(files["quality_issue"]).read().to_pylist()
        if e["severity"] == "error"
    ]
    qids = {q["observation_id"] for q in quarantine}
    if (
        not quarantine
        or len(qids) != len(quarantine)
        or quality["quarantined"] != len(quarantine)
        or quality["blocking_issues"] != len(errors)
        or len(errors) != len(qids)
        or any(q["reason"] != "missing_root" for q in quarantine)
        or {e["observation_id"] for e in errors} != qids
        or any(e["code"] != "missing_root" or e["path"] != "root" for e in errors)
    ):
        raise ValueError("Unapproved non-null error/quarantine or counter mismatch")
    observed = pq.ParquetFile(files["observation"]).read().to_pylist()
    by_id = {r["observation_id"]: r for r in observed}
    if (
        len(by_id) != len(observed)
        or len(observed) != quality["observations"]
        or not qids <= by_id.keys()
    ):
        raise ValueError("Observation identity/count mismatch")
    valid_ids = set()
    for name, path in files.items():
        if name.endswith("_revision"):
            valid_ids.update(
                r["observation_id"]
                for r in pq.ParquetFile(path).read(columns=["observation_id"]).to_pylist()
            )
    if (
        valid_ids & qids
        or valid_ids | qids != by_id.keys()
        or len(valid_ids) != quality["accepted"]
    ):
        raise ValueError("Domain leakage or occurrence reconciliation failure")
    proofs = []
    for oid in sorted(qids):
        row = by_id[oid]
        payload = json.loads(row["payload_json"])
        if calculate_content_hash(payload) != row["content_hash"]:
            raise ValueError("Raw payload content hash mismatch")
        proof = NullSourceException(
            **{
                k: (str(row[k]) if k == "source_date" else row[k])
                for k in APPROVED
                if k != "reason"
            },
            reason="missing_root",
            root_path=null_path(payload, row["table_name"]),
        )
        proof.assert_null_policy()
        if proof.resource != report["resource"] or proof.source_date != entry["date"]:
            raise ValueError("Source cohort lineage mismatch")
        if not any(
            f["table"] == proof.table_name
            and f["key"] == proof.file_key
            and f["sha256"] == proof.file_sha256
            for f in entry["input_files"]
        ):
            raise ValueError("Source file lineage missing")
        proofs.append(proof)
    return proofs
