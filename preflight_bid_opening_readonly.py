"""Read-only Silver bid opening preflight from a DuckDB-exported JSONL sample.

Run from the repository root:
    python preflight_bid_opening_readonly.py exports/bid_opening_preflight_samples.jsonl

This script reads a local JSONL file and transforms records IN MEMORY only.
It never writes candidate Parquet, object storage, or Iceberg.
"""

from __future__ import annotations

import argparse
import hashlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from procurement.silver.application.revisions import assemble, validate
from procurement.silver.domain.json_numbers import loads_bronze_payload
from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.transformers.bid_opening import (
    BidOpeningTransformer,
    classify_opening_shape,
)


SHAPE_PARTS: dict[str, tuple[tuple[str, str, str | None], ...]] = {
    "ldt_single_envelope": (("single", "bid_open", "lot_open_detail"),),
    "ldt_two_envelope": (
        ("technical", "bid_open_technical", "lot_open_detail_technical"),
        ("financial", "bid_open_financial", "lot_open_detail_financial"),
    ),
    "khac_single_envelope": (("single", "opening_out", None),),
    "khac_two_envelope": (
        ("technical", "opening_out_technical", None),
        ("financial", "opening_out_financial", None),
    ),
    "adb_wb_single_envelope": (("single", "opening_adb_wb", None),),
}


def expected_counts(payload: dict[str, Any], shape: str | None) -> tuple[int, int, int]:
    """Independently count source list entries (before dedup/invalid row filtering)."""
    if shape not in SHAPE_PARTS:
        return (0, 0, 0)
    phases = len(SHAPE_PARTS[shape])
    submissions_count = 0
    lots_count = 0
    for _, part_name, lot_part in SHAPE_PARTS[shape]:
        part = payload.get(part_name)
        if isinstance(part, dict):
            response = part.get("bidSubmissionByContractorViewResponse")
            if isinstance(response, dict):
                submissions = response.get("bidSubmissionDTOList")
                if isinstance(submissions, list):
                    submissions_count += len(submissions)
                    if lot_part is None:
                        for submission in submissions:
                            if not isinstance(submission, dict):
                                continue
                            parents = submission.get("lotNoValueDTOList")
                            if not isinstance(parents, list):
                                continue
                            for parent in parents:
                                if not isinstance(parent, dict):
                                    continue
                                views = parent.get("bidOpenView")
                                if isinstance(views, list) and views:
                                    lots_count += len(views)
                                elif views is None or views == []:
                                    lots_count += 1
        if lot_part is not None:
            lot_rows = payload.get(lot_part)
            if isinstance(lot_rows, list):
                lots_count += len(lot_rows)
    return phases, submissions_count, lots_count


def run(path: Path) -> int:
    if not path.is_file():
        raise FileNotFoundError(f"JSONL input file not found: {path}")

    transformer = BidOpeningTransformer()
    transformed = []
    errors: list[str] = []
    statuses: Counter[str] = Counter()

    print("source_id | shape | status | phase actual/expected | submissions actual/expected | lots actual/expected | issues | errors")
    print("-" * 150)

    with path.open(encoding="utf-8") as file:
        for ordinal, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                row = loads_bronze_payload(line)
                source_id = str(row["source_id"])
                raw_payload = row["payload_json"]
                if not isinstance(raw_payload, str):
                    raise TypeError("Expected payload_json as VARCHAR from DuckDB export")
                payload = loads_bronze_payload(raw_payload)
                if not isinstance(payload, dict):
                    raise TypeError("Bronze payload must be JSON object")

                shape = classify_opening_shape(payload)
                root = transformer.extract_root(payload)
                version_raw = root.get("notifyVersion") if isinstance(root, dict) else None
                version = str(version_raw) if version_raw is not None else None
                checksum = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

                envelope = RawRecordEnvelope(
                    namespace="muasamcong",
                    table_name="bid_opening_detail",
                    resource="bid_opening",
                    source_date="2026-11-06",  # synthetic metadata, not the historical source date
                    run_id="local-read-only-preflight",
                    row_ordinal=ordinal,
                    file_key=f"local_preflight/{source_id}.json",
                    file_sha256=checksum,
                    source_id=source_id,
                    source_version=version,
                    content_hash=checksum,
                    ingested_at=datetime(2026, 11, 6, tzinfo=timezone.utc),
                    payload=payload,
                )

                result = transformer.transform(envelope)
                transformed.append(result.to_dict())
                status = (result.typed_attributes or {}).get("collection_status")
                statuses[str(status)] += 1
                expected_phase, expected_submissions, expected_lots = expected_counts(payload, shape)
                actual = Counter(c.typed_table for c in result.children)
                phase_count = actual["opening_phase"]
                submission_count = actual["opening_submission"]
                lot_count = actual["opening_lot_submission"]
                blocking = sum(issue.severity == "error" for issue in result.issues)
                if result.quarantine is not None:
                    errors.append(f"{source_id}: QUARANTINE {result.quarantine.reason}")
                for name, got, expected in (
                    ("phase", phase_count, expected_phase),
                    ("submission", submission_count, expected_submissions),
                    ("lot", lot_count, expected_lots),
                ):
                    if got != expected:
                        errors.append(f"{source_id}: {name} count mismatch {got} != {expected}")
                if blocking:
                    errors.append(f"{source_id}: {blocking} blocking quality issues")

                sub_ids = {
                    c.child_id for c in result.children if c.typed_table == "opening_submission"
                }
                for child in result.children:
                    if child.typed_table != "opening_lot_submission":
                        continue
                    attrs = child.typed_attributes or {}
                    parent_id = attrs.get("submission_id")
                    if parent_id is not None and parent_id not in sub_ids:
                        errors.append(f"{source_id}: dangling lot submission_id {parent_id}")

                print(
                    f"{source_id} | {shape} | {status} | "
                    f"{phase_count}/{expected_phase} | "
                    f"{submission_count}/{expected_submissions} | "
                    f"{lot_count}/{expected_lots} | {len(result.issues)} | {blocking}"
                )
                if result.issues:
                    counts = Counter(f"{issue.code}:{issue.severity}" for issue in result.issues)
                    print("  issue_codes:", dict(counts))
            except Exception as exc:
                errors.append(f"record line {ordinal}: {type(exc).__name__}: {exc}")
                print(f"line {ordinal} | TRANSFORMATION FAILED: {type(exc).__name__}: {exc}")

    if transformed:
        try:
            tables = assemble(transformed)
            report = validate(tables)
            print("\nAssembled counts:", {k: len(v) for k, v in tables.items() if v})
            print("Quality summary:", {k: report[k] for k in ("observations", "accepted", "quarantined", "issues", "blocking_issues")})
            if report["blocking_issues"]:
                errors.append(f"assembled result: {report['blocking_issues']} blocking issues")
        except Exception as exc:
            errors.append(f"assemble/validate: {type(exc).__name__}: {exc}")

    print("Collection statuses:", dict(statuses))
    print("\nRESULT:", "PASS" if not errors else f"REVIEW ({len(errors)} finding(s))")
    for error in errors:
        print(" -", error)
    return 0 if not errors else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jsonl", type=Path, help="DuckDB JSONL export with source_id and payload_json")
    args = parser.parse_args()
    return run(args.jsonl)


if __name__ == "__main__":
    raise SystemExit(main())
