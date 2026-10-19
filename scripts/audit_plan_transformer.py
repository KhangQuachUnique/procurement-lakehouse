"""Read-only bulk audit of the KHLCNT Plan Silver transformer.

Reads Bronze Parquet from the existing SeaweedFS endpoint using the project's
Silver CLI configuration. Never writes to S3 or invokes `silver run`.

Example:
  python scripts/audit_plan_transformer.py --date 2024-01-01 --limit 10000 \
      --output reports/plan_audit_2024-01-01.json
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
from procurement.processing.silver.transformers.plan import clean_project_reference


EXPECTED_FIELDS = frozenset(
    {
        "revision_id", "entity_id", "plan_no", "title", "investor_name",
        "project_id", "project_no", "plan_type", "total_investment",
        "currency", "decision_date", "decision_no", "decision_agency",
        "bid_pack", "location", "public_date", "status_raw", "root_json",
    }
)
NULLABLE_PROFILE_FIELDS = EXPECTED_FIELDS - {"revision_id", "entity_id", "root_json"}
REQUIRED_BRONZE_COLUMNS = {"payload", "source_id", "source_date", "run_id", "content_hash"}
OPTIONAL_BRONZE_COLUMNS = {"source_version"}


def make_report(date: str) -> dict[str, Any]:
    return {
        "source_date": date,
        "table": "khlcnt_plan_detail",
        "scanned": 0,
        "accepted": 0,
        "quarantined": 0,
        "transform_exceptions": 0,
        "files_read": 0,
        "by_plan_type": Counter(),
        "quarantine_reasons": Counter(),
        "quality_issue_codes": Counter(),
        "quality_issue_paths": Counter(),
        "null_fields": Counter(),
        "reference_fields": Counter(),
        "reference_schemes": Counter(),
        "source_pid_present": 0,
        "source_pno_present": 0,
        "source_pid_without_pno": 0,
        "source_pno_without_pid": 0,
        "source_pid_and_pno_missing": 0,
        "project_embedded_present": 0,
        "project_embedded_id_checked": 0,
        "project_embedded_id_mismatches": 0,
        "project_embedded_no_checked": 0,
        "project_embedded_no_mismatches": 0,
        "project_embedded_without_pid": 0,
        "reference_mismatches": 0,
        "schema_mismatches": 0,
        "project_id_unexpected_nonnull": 0,
        "examples": [],
    }


def add_example(report: dict[str, Any], kind: str, source_id: Any, detail: str, cap: int) -> None:
    if len(report["examples"]) < cap:
        report["examples"].append({
            "kind": kind,
            "source_id": None if source_id is None else str(source_id),
            "detail": str(detail)[:300],
        })


def audit_one(
    report: dict[str, Any], transformer: Any, row: dict[str, Any], *,
    file_path: str, ordinal: int, max_examples: int,
) -> None:
    report["scanned"] += 1
    source_id = row.get("source_id")

    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        if not isinstance(payload, dict):
            # Let BaseResourceTransformer quarantine this as missing root.
            payload = {} if payload is None else {"invalid_payload": payload}

        envelope = RawRecordEnvelope(
            namespace="muasamcong",
            table_name="khlcnt_plan_detail",
            resource="khlcnt",
            source_date=str(row["source_date"])[:10],
            run_id=str(row["run_id"]),
            row_ordinal=ordinal,  # Ordinal is relative to the current physical file.
            file_key=file_path,
            # Audit-only placeholder. Do NOT persist this observation to Silver:
            # it is NOT the actual SHA-256 of the Parquet file.
            file_sha256="READ_ONLY_AUDIT_NOT_REAL_SHA256",
            source_id=str(source_id) if source_id is not None else "",
            source_version=(
                str(row["source_version"])
                if row.get("source_version") is not None else None
            ),
            content_hash=str(row["content_hash"]),
            ingested_at=datetime.now(UTC),
            payload=payload,
        )
        result = transformer.transform(envelope)
    except Exception as exc:
        report["transform_exceptions"] += 1
        add_example(report, "transform_exception", source_id,
                    f"{type(exc).__name__}: {exc}", max_examples)
        return

    for issue in result.issues:
        report["quality_issue_codes"][issue.code] += 1
        report["quality_issue_paths"][issue.path] += 1

    if result.quarantine is not None:
        report["quarantined"] += 1
        report["quarantine_reasons"][result.quarantine.reason] += 1
        add_example(report, "quarantine", source_id,
                    result.quarantine.reason, max_examples)
        return

    report["accepted"] += 1
    typed = result.typed_attributes or {}
    report["by_plan_type"][str(typed.get("plan_type") or "<NULL>")] += 1

    for key in NULLABLE_PROFILE_FIELDS:
        if typed.get(key) is None:
            report["null_fields"][key] += 1

    current_fields = set(typed)
    if current_fields != EXPECTED_FIELDS:
        report["schema_mismatches"] += 1
        add_example(report, "schema_mismatch", source_id,
                    f"missing={sorted(EXPECTED_FIELDS - current_fields)}; "
                    f"extra={sorted(current_fields - EXPECTED_FIELDS)}", max_examples)

    if typed.get("project_id") is not None:
        report["project_id_unexpected_nonnull"] += 1

    root = payload.get("bidPoBidpPlanProjectDetailView")
    root = root if isinstance(root, dict) else {}
    pid = clean_project_reference(root.get("pid"))
    pno = clean_project_reference(root.get("pno"))

    if pid:
        report["source_pid_present"] += 1
    if pno:
        report["source_pno_present"] += 1
    if pid and not pno:
        report["source_pid_without_pno"] += 1
    elif pno and not pid:
        report["source_pno_without_pid"] += 1
    elif not pid and not pno:
        report["source_pid_and_pno_missing"] += 1

    for ref in result.references:
        report["reference_fields"][ref.field_name] += 1
        report["reference_schemes"][ref.id_scheme] += 1

    expected_ref = ("uuid", pid, "pid") if pid else (("number", pno, "pno") if pno else None)
    if expected_ref is None:
        correct_ref = len(result.references) == 0
    else:
        correct_ref = len(result.references) == 1 and (
            result.references[0].target_type == "project"
            and result.references[0].id_scheme == expected_ref[0]
            and result.references[0].target_identity == expected_ref[1]
            and result.references[0].field_name == expected_ref[2]
            and result.references[0].from_revision_id == result.revision.revision_id
        )

    if not correct_ref:
        report["reference_mismatches"] += 1
        add_example(report, "reference_mismatch", source_id,
                    f"pid={pid!r}; pno={pno!r}; "
                    f"references={[(r.id_scheme, r.target_identity, r.field_name) for r in result.references]}",
                    max_examples)

    if typed.get("project_no") != pno:
        report["reference_mismatches"] += 1
        add_example(report, "project_no_mismatch", source_id,
                    f"source pno={pno!r}, typed.project_no={typed.get('project_no')!r}",
                    max_examples)

    embedded = payload.get("project")
    if isinstance(embedded, dict):
        report["project_embedded_present"] += 1
        embedded_id = clean_project_reference(embedded.get("id"))
        embedded_no = clean_project_reference(embedded.get("no"))
        if embedded_id and not pid:
            report["project_embedded_without_pid"] += 1
            add_example(report, "embedded_project_without_pid", source_id,
                        f"embedded.id={embedded_id}", max_examples)
        if embedded_id and pid:
            report["project_embedded_id_checked"] += 1
            if embedded_id != pid:
                report["project_embedded_id_mismatches"] += 1
                add_example(report, "embedded_id_mismatch", source_id,
                            f"pid={pid}; project.id={embedded_id}", max_examples)
        if embedded_no and pno:
            report["project_embedded_no_checked"] += 1
            if embedded_no != pno:
                report["project_embedded_no_mismatches"] += 1
                add_example(report, "embedded_no_mismatch", source_id,
                            f"pno={pno}; project.no={embedded_no}", max_examples)


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    transformer = get_transformer("khlcnt_plan_detail")
    if transformer is None:
        raise RuntimeError("Plan transformer not registered. Check transformers/__init__.py")

    fs, bucket = get_s3_fs()
    path = f"{bucket}/bronze/muasamcong/khlcnt_plan_detail/source_date={args.date}"
    if not fs.exists(path):
        raise FileNotFoundError(f"No Bronze partition at {path}")

    files = sorted(f for f in fs.find(path) if f.endswith(".parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files at {path}")
    report = make_report(args.date)
    report["limit"] = args.limit
    report["files_available"] = len(files)

    for file_path in files:
        if args.limit and report["scanned"] >= args.limit:
            break
        with fs.open(file_path, "rb") as stream:
            parquet = pq.ParquetFile(stream)
            names = set(parquet.schema_arrow.names)
            missing = REQUIRED_BRONZE_COLUMNS - names
            if missing:
                raise ValueError(f"{file_path} missing Bronze columns: {sorted(missing)}")
            columns = sorted((REQUIRED_BRONZE_COLUMNS | OPTIONAL_BRONZE_COLUMNS) & names)
            report["files_read"] += 1
            ordinal = 0
            for batch in parquet.iter_batches(batch_size=args.batch_size, columns=columns):
                for row in batch.to_pylist():
                    if args.limit and report["scanned"] >= args.limit:
                        break
                    audit_one(report, transformer, row, file_path=file_path,
                              ordinal=ordinal, max_examples=args.max_examples)
                    ordinal += 1
                    if args.progress_every and report["scanned"] % args.progress_every == 0:
                        print(f"  Scanned {report['scanned']:,} rows | "
                              f"accepted={report['accepted']:,} | "
                              f"quarantine={report['quarantined']:,} | "
                              f"exceptions={report['transform_exceptions']:,}", flush=True)
                if args.limit and report["scanned"] >= args.limit:
                    break
    report["completed_at_utc"] = datetime.now(UTC).isoformat()
    report["read_only"] = True
    report["completed_full_partition"] = report["files_read"] == len(files) and (
        not args.limit or report["scanned"] < args.limit
    )
    return report


def print_report(report: dict[str, Any]) -> None:
    scanned = report["scanned"]
    accepted = report["accepted"]
    print("\n" + "=" * 68)
    print(f"PLAN BRONZE BULK AUDIT | source_date={report['source_date']}")
    print("=" * 68)
    print(f"Scanned: {scanned:,} | Accepted: {accepted:,} | "
          f"Quarantined: {report['quarantined']:,} | Exceptions: {report['transform_exceptions']:,}")
    print(f"Acceptance rate: {(accepted / scanned * 100) if scanned else 0:.2f}%")
    print(f"Parquet files read: {report['files_read']:,} / {report['files_available']:,}")
    print("By plan type:", dict(report["by_plan_type"]))
    print("Quarantine:", dict(report["quarantine_reasons"]))
    print("Quality issue codes:", dict(report["quality_issue_codes"]))
    print("Quality issue paths:", dict(report["quality_issue_paths"]))
    print("Source pid/pno:", {k: report[k] for k in (
        "source_pid_present", "source_pno_present", "source_pid_without_pno",
        "source_pno_without_pid", "source_pid_and_pno_missing")})
    print("References:", dict(report["reference_fields"]))
    print("Embedded Project check:", {k: report[k] for k in (
        "project_embedded_present", "project_embedded_id_checked",
        "project_embedded_id_mismatches", "project_embedded_no_checked",
        "project_embedded_no_mismatches", "project_embedded_without_pid")})
    print("Null counts (accepted only):", dict(sorted(report["null_fields"].items())))
    print("Schema mismatches:", report["schema_mismatches"])
    print("Reference mismatches:", report["reference_mismatches"])
    print("Unexpected non-NULL project_id:", report["project_id_unexpected_nonnull"])
    if report["examples"]:
        print("Examples (first few):")
        for ex in report["examples"]:
            print("  ", ex)
    print("=" * 68)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Bronze Plan transformer bulk audit")
    parser.add_argument("--date", required=True, help="Bronze source_date, e.g. 2024-01-01")
    parser.add_argument("--limit", type=int, default=10000,
                        help="Maximum records to scan (0 means entire day; default 10000)")
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--max-examples", type=int, default=15)
    parser.add_argument("--output", type=Path, help="Optional local JSON report file (not S3)")
    args = parser.parse_args()
    if args.limit < 0 or args.batch_size < 1 or args.max_examples < 0 or args.progress_every < 0:
        parser.error("limit, max-examples, progress-every >= 0; batch-size >= 1")
    report = run_audit(args)
    print_report(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2)
        print(f"Local report: {args.output}")


if __name__ == "__main__":
    main()
