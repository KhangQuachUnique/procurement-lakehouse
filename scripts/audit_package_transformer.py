"""Read-only Bronze audit of the registered KHLCNT Package Silver transformer.

Examples (from the repository root):
  python scripts/audit_package_transformer.py --date 2024-01-01 --limit 5000 \
      --output reports/package_audit_2024-01-01.json
  python scripts/audit_package_transformer.py --date 2025-12-31 --limit 0

Runs `transform()` in memory; DOES NOT write Silver Parquet or S3 objects.
The audit-only file_sha256 is intentionally not a real content hash.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from procurement.cli.silver import get_s3_fs
from procurement.processing.silver.protocols import RawRecordEnvelope
from procurement.processing.silver.registry import get_transformer
from procurement.processing.silver.transformers.package import (
    PACKAGE_LOCATION_COLUMNS,
    PACKAGE_LOT_COLUMNS,
    PACKAGE_REVISION_COLUMNS,
    _valid_reference,
)
from procurement.processing.silver.json_numbers import loads_bronze_payload

TABLE = "khlcnt_bid_package_detail"
REQUIRED_BRONZE = frozenset(
    {"payload", "source_id", "source_date", "run_id", "content_hash"}
)
OPTIONAL_BRONZE = frozenset({"source_version"})
REVISION_FIELDS = frozenset(PACKAGE_REVISION_COLUMNS)
LOCATION_FIELDS = frozenset(PACKAGE_LOCATION_COLUMNS)
LOT_FIELDS = frozenset(PACKAGE_LOT_COLUMNS)


def make_report(source_date: str) -> dict[str, Any]:
    return {
        "source_date": source_date,
        "table": TABLE,
        "scanned": 0,
        "accepted": 0,
        "quarantined": 0,
        "transform_exceptions": 0,
        "files_read": 0,
        "by_bid_field": Counter(),
        "by_process_apply": Counter(),
        "quarantine_reasons": Counter(),
        "quality_issue_codes": Counter(),
        "quality_issue_paths": Counter(),
        "null_fields": Counter(),
        "reference_fields": Counter(),
        "reference_schemes": Counter(),
        "source_plan_id_present": 0,
        "source_plan_no_present": 0,
        "source_both_plan_refs_missing": 0,
        "source_id_mismatches": 0,
        "missing_package_number": 0,
        "reference_mismatches": 0,
        "schema_mismatches": 0,
        "plan_id_unexpected_nonnull": 0,
        "source_location_items": 0,
        "source_lot_items": 0,
        "expected_location_children": 0,
        "expected_lot_children": 0,
        "actual_location_children": 0,
        "actual_lot_children": 0,
        "location_count_mismatches": 0,
        "lot_count_mismatches": 0,
        "child_schema_mismatches": 0,
        "child_data_mismatches": 0,
        "unexpected_child_kinds": 0,
        "max_locations_in_package": 0,
        "max_lots_in_package": 0,
        "large_lot_packages_100_plus": 0,
        "examples": [],
    }


def example(
    report: dict[str, Any], kind: str, source_id: Any, detail: str, max_examples: int
) -> None:
    if len(report["examples"]) < max_examples:
        report["examples"].append(
            {
                "kind": kind,
                "source_id": None if source_id is None else str(source_id),
                "detail": str(detail)[:400],
            }
        )


def _source_items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _audit_children(
    report: dict[str, Any],
    result: Any,
    payload: dict[str, Any],
    source_id: Any,
    cap: int,
) -> None:
    raw_locs = _source_items(payload.get("bidLocation"))
    raw_lots = _source_items(payload.get("bidpBidLotList"))
    expected_locs = [
        (i, item) for i, item in enumerate(raw_locs) if isinstance(item, dict)
    ]
    expected_lots = [
        (i, item) for i, item in enumerate(raw_lots) if isinstance(item, dict)
    ]
    locations = [child for child in result.children if child.kind == "location"]
    lots = [child for child in result.children if child.kind == "lot"]

    report["source_location_items"] += len(raw_locs)
    report["source_lot_items"] += len(raw_lots)
    report["expected_location_children"] += len(expected_locs)
    report["expected_lot_children"] += len(expected_lots)
    report["actual_location_children"] += len(locations)
    report["actual_lot_children"] += len(lots)
    report["max_locations_in_package"] = max(
        report["max_locations_in_package"], len(raw_locs)
    )
    report["max_lots_in_package"] = max(report["max_lots_in_package"], len(raw_lots))
    if len(raw_lots) >= 100:
        report["large_lot_packages_100_plus"] += 1

    if len(locations) != len(expected_locs):
        report["location_count_mismatches"] += 1
        example(
            report,
            "location_count_mismatch",
            source_id,
            f"expected_valid={len(expected_locs)} actual={len(locations)}",
            cap,
        )
    if len(lots) != len(expected_lots):
        report["lot_count_mismatches"] += 1
        example(
            report,
            "lot_count_mismatch",
            source_id,
            f"expected_valid={len(expected_lots)} actual={len(lots)}",
            cap,
        )

    for child in result.children:
        if child.kind not in {"location", "lot"}:
            report["unexpected_child_kinds"] += 1
            example(report, "unexpected_child", source_id, child.kind, cap)
            continue
        try:
            typed_row = json.loads(child.payload_json)
            expected_fields = (
                LOCATION_FIELDS if child.kind == "location" else LOT_FIELDS
            )
            if not isinstance(typed_row, dict) or set(typed_row) != expected_fields:
                report["child_schema_mismatches"] += 1
                example(
                    report,
                    "child_schema_mismatch",
                    source_id,
                    f"kind={child.kind}, fields={sorted(typed_row) if isinstance(typed_row,dict) else type(typed_row).__name__}",
                    cap,
                )
                continue
            expected_path = f"{'bidLocation' if child.kind == 'location' else 'bidpBidLotList'}[{child.ordinal}]"
            actual_ordinal = typed_row.get(
                "location_ordinal" if child.kind == "location" else "lot_ordinal"
            )
            if (
                typed_row["revision_id"] != result.revision.revision_id
                or actual_ordinal != child.ordinal
                or child.path != expected_path
                or (child.kind == "lot" and typed_row["lot_id"] != child.child_id)
            ):
                report["child_data_mismatches"] += 1
                example(
                    report,
                    "child_data_mismatch",
                    source_id,
                    f"kind={child.kind}, ordinal={child.ordinal}, path={child.path}",
                    cap,
                )
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            report["child_schema_mismatches"] += 1
            example(report, "child_payload_invalid", source_id, repr(exc), cap)


def audit_one(
    report: dict[str, Any],
    transformer: Any,
    bronze_row: dict[str, Any],
    *,
    file_path: str,
    ordinal: int,
    max_examples: int,
) -> None:
    report["scanned"] += 1
    source_id = bronze_row.get("source_id")
    try:
        raw_payload = bronze_row["payload"]

        payload = (
            loads_bronze_payload(raw_payload)
            if isinstance(raw_payload, (str, bytes, bytearray))
            else raw_payload
        )
        if not isinstance(payload, dict):
            payload = {} if payload is None else {"invalid_payload": payload}
        envelope = RawRecordEnvelope(
            namespace="muasamcong",
            table_name=TABLE,
            resource="khlcnt",
            source_date=str(bronze_row["source_date"])[:10],
            run_id=str(bronze_row["run_id"]),
            row_ordinal=ordinal,
            file_key=file_path,
            file_sha256="READ_ONLY_AUDIT_NOT_REAL_SHA256",
            source_id=str(source_id) if source_id is not None else "",
            source_version=(
                str(bronze_row["source_version"])
                if bronze_row.get("source_version") is not None
                else None
            ),
            content_hash=str(bronze_row["content_hash"]),
            ingested_at=datetime.now(UTC),
            payload=payload,
        )
        result = transformer.transform(envelope)
    except Exception as exc:
        report["transform_exceptions"] += 1
        example(
            report,
            "transform_exception",
            source_id,
            f"{type(exc).__name__}: {exc}",
            max_examples,
        )
        return

    for issue in result.issues:
        report["quality_issue_codes"][issue.code] += 1
        report["quality_issue_paths"][issue.path] += 1
    if result.quarantine is not None:
        report["quarantined"] += 1
        report["quarantine_reasons"][result.quarantine.reason] += 1
        example(report, "quarantine", source_id, result.quarantine.reason, max_examples)
        return

    report["accepted"] += 1
    typed = result.typed_attributes or {}
    report["by_bid_field"][str(typed.get("bid_field") or "<NULL>")] += 1
    report["by_process_apply"][str(typed.get("process_apply") or "<NULL>")] += 1
    for field in REVISION_FIELDS - {"revision_id", "entity_id", "root_json"}:
        if typed.get(field) is None:
            report["null_fields"][field] += 1
    if set(typed) != REVISION_FIELDS:
        report["schema_mismatches"] += 1
        example(
            report,
            "revision_schema_mismatch",
            source_id,
            f"missing={sorted(REVISION_FIELDS-set(typed))}; extra={sorted(set(typed)-REVISION_FIELDS)}",
            max_examples,
        )
    if typed.get("plan_id") is not None:
        report["plan_id_unexpected_nonnull"] += 1

    if str(source_id) != str(payload.get("id")):
        report["source_id_mismatches"] += 1
        example(
            report,
            "source_id_mismatch",
            source_id,
            f"payload.id={payload.get('id')!r}",
            max_examples,
        )
    if not typed.get("package_no"):
        report["missing_package_number"] += 1

    plan_id = _valid_reference(payload.get("planId"))
    plan_no = _valid_reference(payload.get("planNo"))
    if plan_id:
        report["source_plan_id_present"] += 1
    if plan_no:
        report["source_plan_no_present"] += 1
    if not plan_id and not plan_no:
        report["source_both_plan_refs_missing"] += 1
    for ref in result.references:
        report["reference_fields"][ref.field_name] += 1
        report["reference_schemes"][ref.id_scheme] += 1
    expected_ref = (
        ("uuid", plan_id, "planId")
        if plan_id
        else (("number", plan_no, "planNo") if plan_no else None)
    )
    refs = result.references
    reference_is_correct = (
        len(refs) == 0
        if expected_ref is None
        else len(refs) == 1
        and refs[0].target_type == "plan"
        and refs[0].id_scheme == expected_ref[0]
        and refs[0].target_identity == expected_ref[1]
        and refs[0].field_name == expected_ref[2]
        and refs[0].from_revision_id == result.revision.revision_id
    )
    if not reference_is_correct or typed.get("plan_no") != plan_no:
        report["reference_mismatches"] += 1
        example(
            report,
            "reference_mismatch",
            source_id,
            f"source planId={plan_id!r} planNo={plan_no!r}"
            f" ref={[(r.id_scheme,r.target_identity,r.field_name) for r in refs]}"
            f" typed.plan_no={typed.get('plan_no')!r}",
            max_examples,
        )

    _audit_children(report, result, payload, source_id, max_examples)


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    transformer = get_transformer(TABLE)
    if transformer is None:
        raise RuntimeError(
            "Package transformer not registered. Check transformers/__init__.py"
        )
    fs, bucket = get_s3_fs()
    base = f"{bucket}/bronze/muasamcong/{TABLE}/source_date={args.date}"
    if not fs.exists(base):
        raise FileNotFoundError(f"Bronze partition not found: {base}")
    files = sorted(f for f in fs.find(base) if f.endswith(".parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found at: {base}")

    report = make_report(args.date)
    report["files_available"] = len(files)
    report["limit"] = args.limit
    stopped_for_limit = False
    for file_path in files:
        if stopped_for_limit:
            break
        with fs.open(file_path, "rb") as stream:
            parquet = pq.ParquetFile(stream)
            names = set(parquet.schema_arrow.names)
            missing = REQUIRED_BRONZE - names
            if missing:
                raise ValueError(
                    f"{file_path} is missing Bronze columns: {sorted(missing)}"
                )
            selected = sorted((REQUIRED_BRONZE | OPTIONAL_BRONZE) & names)
            report["files_read"] += 1
            ordinal = 0
            for batch in parquet.iter_batches(
                columns=selected, batch_size=args.batch_size
            ):
                for row in batch.to_pylist():
                    if args.limit and report["scanned"] >= args.limit:
                        stopped_for_limit = True
                        break
                    audit_one(
                        report,
                        transformer,
                        row,
                        file_path=file_path,
                        ordinal=ordinal,
                        max_examples=args.max_examples,
                    )
                    ordinal += 1
                    if (
                        args.progress_every
                        and report["scanned"] % args.progress_every == 0
                    ):
                        print(
                            f"  Scanned {report['scanned']:,} | "
                            f"accepted={report['accepted']:,} | "
                            f"quarantine={report['quarantined']:,} | "
                            f"exceptions={report['transform_exceptions']:,}",
                            flush=True,
                        )
                if stopped_for_limit:
                    break
    report["completed_at_utc"] = datetime.now(UTC).isoformat()
    report["read_only"] = True
    report["completed_full_partition"] = not stopped_for_limit and report[
        "files_read"
    ] == len(files)
    return report


def print_report(report: dict[str, Any]) -> None:
    scanned = report["scanned"]
    print("\n" + "=" * 72)
    print(f"PACKAGE BRONZE BULK AUDIT | source_date={report['source_date']}")
    print("=" * 72)
    print(
        f"Scanned: {scanned:,} | Accepted: {report['accepted']:,} | "
        f"Quarantine: {report['quarantined']:,} | Exceptions: {report['transform_exceptions']:,}"
    )
    print(f"Acceptance rate: {100 * report['accepted']/scanned if scanned else 0:.2f}%")
    print(f"Parquet files read: {report['files_read']} / {report['files_available']}")
    print("By bid field:", dict(report["by_bid_field"]))
    print("By process apply:", dict(report["by_process_apply"]))
    print("Quarantine reasons:", dict(report["quarantine_reasons"]))
    print("Quality issue codes:", dict(report["quality_issue_codes"]))
    print("Null fields (accepted only):", dict(sorted(report["null_fields"].items())))
    print("Plan reference fields:", dict(report["reference_fields"]))
    print("Reference mismatches:", report["reference_mismatches"])
    print("Revision schema mismatches:", report["schema_mismatches"])
    print("Unexpected non-NULL Plan FK:", report["plan_id_unexpected_nonnull"])
    print("Source ID mismatches:", report["source_id_mismatches"])
    print(
        "Locations (source / valid / actual):",
        report["source_location_items"],
        report["expected_location_children"],
        report["actual_location_children"],
    )
    print(
        "Lots (source / valid / actual):",
        report["source_lot_items"],
        report["expected_lot_children"],
        report["actual_lot_children"],
    )
    print(
        "Child count mismatches (location / lot):",
        report["location_count_mismatches"],
        report["lot_count_mismatches"],
    )
    print(
        "Child schema/data mismatches:",
        report["child_schema_mismatches"],
        report["child_data_mismatches"],
    )
    print("Maximum lots in one package:", report["max_lots_in_package"])
    print("Packages with 100+ lots:", report["large_lot_packages_100_plus"])
    if report["examples"]:
        print("Examples:")
        for item in report["examples"]:
            print("  ", item)
    print("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only Bronze Package Silver bulk audit"
    )
    parser.add_argument("--date", required=True, help="Partition date: YYYY-MM-DD")
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum records to scan; 0 means scan the entire day",
    )
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--max-examples", type=int, default=15)
    parser.add_argument("--output", type=Path, help="Optional local JSON report path")
    args = parser.parse_args()
    if (
        args.limit < 0
        or args.batch_size < 1
        or args.progress_every < 0
        or args.max_examples < 0
    ):
        parser.error("limit, progress-every, max-examples >=0; batch-size >=1")
    report = run_audit(args)
    print_report(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2)
        print(f"Local report: {args.output}")


if __name__ == "__main__":
    main()
