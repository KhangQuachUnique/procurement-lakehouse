"""Streaming, read-only Bronze -> Silver bid-opening preflight.

Use from the repository root with a DuckDB export containing columns:
    source_id VARCHAR, payload_json VARCHAR

Preferred input is compressed Parquet. JSONL from the earlier sample script is
also accepted. No Silver Candidate, object storage or Iceberg writes occur.
The optional --report creates only a LOCAL summary JSON file.

Example:
    python preflight_bid_opening_full_readonly.py \
        exports/bid_opening_full_preflight.parquet \
        --expected-records 577660 \
        --report exports/bid_opening_full_preflight_report.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from procurement.silver.application.revisions import assemble, validate
from procurement.silver.domain.json_numbers import loads_bronze_payload
from procurement.silver.domain.protocols import RawRecordEnvelope, TransformedRecord
from procurement.silver.domain.schema import metadata
from procurement.silver.domain.transformers.bid_opening import (
    BidOpeningTransformer,
    classify_opening_shape,
)
from preflight_bid_opening_readonly import expected_counts

FIXED_OBSERVED_AT = datetime(2026, 11, 6, tzinfo=timezone.utc)
TABLES = (
    "opening_revision",
    "opening_phase",
    "opening_submission",
    "opening_lot_submission",
)


def read_source_rows(path: Path, batch_size: int) -> Iterator[tuple[str, str]]:
    """Read without materializing the exported dataset in memory."""
    if path.suffix.lower() == ".parquet":
        # DuckDB is a declared project dependency; PyArrow need not be installed.
        import duckdb

        with duckdb.connect(database=":memory:") as con:
            cursor = con.execute(
                "SELECT source_id, payload_json FROM read_parquet(?)",
                [str(path)],
            )
            columns = [entry[0] for entry in cursor.description]
            if columns != ["source_id", "payload_json"]:
                raise ValueError("Parquet must provide source_id and payload_json")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for source_id, payload_json in rows:
                    yield source_id, payload_json
        return

    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    yield row["source_id"], row["payload_json"]
                except (ValueError, TypeError, KeyError) as exc:
                    # A malformed outer row must remain visible as a finding.
                    raise ValueError(f"Bad JSONL outer row on line {line_number}") from exc
        return

    raise ValueError("Input must be .parquet, .jsonl or .ndjson")


class Summary:
    def __init__(self, preview_limit: int) -> None:
        self.processed = 0
        self.accepted = 0
        self.quarantined = 0
        self.blocking_issues = 0
        self.quality_issues = 0
        self.unlinked_lots = 0
        self.shapes: Counter[str] = Counter()
        self.statuses: Counter[str] = Counter()
        self.issue_codes: Counter[str] = Counter()
        self.actual_counts: Counter[str] = Counter()
        self.expected_counts: Counter[str] = Counter()
        self.finding_types: Counter[str] = Counter()
        self.finding_preview: list[dict[str, str]] = []
        self.preview_limit = preview_limit
        self.seen_source_ids: set[str] = set()
        self.failed_source_ids: set[str] = set()
        self.batch_checks = 0

    @property
    def finding_count(self) -> int:
        return sum(self.finding_types.values())

    def finding(self, source_id: str, kind: str, detail: str) -> None:
        self.finding_types[kind] += 1
        self.failed_source_ids.add(source_id)
        if len(self.finding_preview) < self.preview_limit:
            self.finding_preview.append(
                {"source_id": source_id, "type": kind, "detail": detail}
            )

    def as_dict(self, input_path: Path, expected_records: int | None) -> dict[str, Any]:
        return {
            "result": "PASS" if not self.finding_count else "REVIEW",
            "input": str(input_path),
            "expected_records": expected_records,
            "processed": self.processed,
            "accepted": self.accepted,
            "quarantined": self.quarantined,
            "blocking_issues": self.blocking_issues,
            "quality_issues": self.quality_issues,
            "unlinked_lots": self.unlinked_lots,
            "distinct_source_ids": len(self.seen_source_ids),
            "shapes": dict(self.shapes.most_common()),
            "collection_statuses": dict(self.statuses.most_common()),
            "issue_codes": dict(self.issue_codes.most_common()),
            "silver_counts": dict(self.actual_counts),
            "source_expected_child_counts": dict(self.expected_counts),
            "batch_assemble_validate_checks": self.batch_checks,
            "finding_count": self.finding_count,
            "affected_source_ids": len(self.failed_source_ids),
            "finding_types": dict(self.finding_types.most_common()),
            "finding_preview": self.finding_preview,
            "notes": [
                "Physical metadata is synthetic, not authentic Bronze provenance.",
                "Batch assembly does not certify globally current revisions.",
                "PASS does not certify Bronze provenance or authorize Silver/Iceberg publication.",
            ],
        }


def process_one(
    source_id_raw: Any,
    raw_payload: Any,
    ordinal: int,
    transformer: BidOpeningTransformer,
    summary: Summary,
) -> TransformedRecord | None:
    source_id = str(source_id_raw) if source_id_raw is not None else f"<line:{ordinal}>"
    summary.processed += 1

    if source_id_raw is None or not isinstance(raw_payload, str):
        summary.finding(source_id, "invalid_input_row", "Missing source_id or string payload_json")
        return None

    if source_id in summary.seen_source_ids:
        summary.finding(
            source_id, "duplicate_source_id", "Source ID appears more than once in export"
        )
    summary.seen_source_ids.add(source_id)

    try:
        payload = loads_bronze_payload(raw_payload)
        if not isinstance(payload, dict):
            raise TypeError("Bronze payload is not a JSON object")
        shape = classify_opening_shape(payload)
        root = transformer.extract_root(payload)
        version_raw = root.get("notifyVersion") if isinstance(root, dict) else None
        version = str(version_raw) if version_raw is not None else None
        checksum = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
        env = RawRecordEnvelope(
            namespace="muasamcong",
            table_name="bid_opening_detail",
            resource="bid_opening",
            source_date="2026-11-06",  # deliberately synthetic
            run_id="full-readonly-preflight",
            row_ordinal=ordinal,
            file_key=f"readonly_preflight/{ordinal}/{source_id}",
            file_sha256=checksum,
            source_id=source_id,
            source_version=version,
            content_hash=checksum,
            ingested_at=FIXED_OBSERVED_AT,
            payload=payload,
        )
        transformed = transformer.transform(env)
    except Exception as exc:
        summary.finding(source_id, "transform_exception", f"{type(exc).__name__}: {exc}")
        return None

    summary.shapes[str(shape)] += 1
    if shape is None:
        summary.finding(
            source_id, "unsupported_opening_shape",
            f"Unexpected Bronze payload components: {sorted(payload)}",
        )
    summary.statuses[str((transformed.typed_attributes or {}).get("collection_status"))] += 1
    summary.quality_issues += len(transformed.issues)
    for issue in transformed.issues:
        summary.issue_codes[f"{issue.code}:{issue.severity}"] += 1
        if issue.severity == "error":
            summary.blocking_issues += 1
            summary.finding(source_id, "blocking_quality_issue", f"{issue.code} @ {issue.path}")

    if transformed.quarantine is not None:
        summary.quarantined += 1
        summary.finding(source_id, "quarantine", transformed.quarantine.reason)
    else:
        summary.accepted += 1
        if transformed.revision is None or transformed.typed_attributes is None:
            summary.finding(
                source_id, "invalid_accepted",
                "Accepted record missing revision or typed attributes"
            )
        if transformed.entity is not None and transformed.entity.source_identity != source_id:
            summary.finding(
                source_id, "source_identity_mismatch",
                f"bronze={source_id}, extracted={transformed.entity.source_identity}",
            )

    phase_expected, submission_expected, lot_expected = expected_counts(payload, shape)
    expected = {
        "opening_phase": phase_expected,
        "opening_submission": submission_expected,
        "opening_lot_submission": lot_expected,
    }
    actual = Counter(child.typed_table for child in transformed.children)
    for table in TABLES[1:]:
        count = actual[table]
        summary.actual_counts[table] += count
        summary.expected_counts[table] += expected[table]
        if count != expected[table]:
            summary.finding(
                source_id,
                "child_count_mismatch",
                f"{table} count {count} != source expected {expected[table]}",
            )
    if transformed.quarantine is None:
        summary.actual_counts["opening_revision"] += 1

    submission_ids = {
        child.child_id
        for child in transformed.children
        if child.typed_table == "opening_submission"
    }
    child_ids: set[str] = set()
    for child in transformed.children:
        if child.child_id in child_ids:
            summary.finding(source_id, "duplicate_child_id", f"Child ID: {child.child_id}")
        child_ids.add(child.child_id)
        if child.revision_id != (
            transformed.revision.revision_id if transformed.revision else None
        ):
            summary.finding(source_id, "wrong_child_revision", f"{child.kind} @ {child.path}")
        if child.typed_table != "opening_lot_submission":
            continue
        attrs = child.typed_attributes or {}
        parent_id = attrs.get("submission_id")
        if parent_id is None:
            summary.unlinked_lots += 1
        elif parent_id not in submission_ids:
            summary.finding(source_id, "dangling_submission_id", f"{parent_id} @ {child.path}")

    return transformed


def check_batch(rows: list[dict[str, Any]], summary: Summary, number: int) -> None:
    if not rows:
        return
    try:
        tables = assemble(rows)
        outcome = validate(tables)
        summary.batch_checks += 1
        # Validate the actual 4-table SQLAlchemy column contracts, including
        # required columns. The current revision gains observation_id and
        # is_current when assemble() runs.
        source_child_counts = Counter(
            child.get("typed_table")
            for row in rows
            for child in row["children"]
        )
        accepted_in_batch = sum(row["quarantine"] is None for row in rows)
        for table_name in TABLES:
            table_def = metadata.tables.get(f"silver.{table_name}")
            if table_def is None:
                table_def = metadata.tables.get(table_name)
            if table_def is None:
                summary.finding(
                    f"<batch:{number}>", "missing_schema_contract", table_name
                )
                continue
            schema_columns = {column.name for column in table_def.columns}
            required_columns = {
                column.name for column in table_def.columns if not column.nullable
            }
            for row_index, item in enumerate(tables.get(table_name, [])):
                actual_columns = set(item)
                missing_columns = schema_columns - actual_columns
                extra_columns = actual_columns - schema_columns
                null_columns = {
                    column for column in required_columns if item.get(column) is None
                }
                if missing_columns or extra_columns or null_columns:
                    summary.finding(
                        f"<batch:{number}>", "schema_mismatch",
                        f"{table_name}[{row_index}] missing={sorted(missing_columns)} "
                        f"extra={sorted(extra_columns)} null_required={sorted(null_columns)}",
                    )
            if len(tables.get(table_name, [])) != (
                accepted_in_batch
                if table_name == "opening_revision"
                else source_child_counts[table_name]
            ):
                summary.finding(
                    f"<batch:{number}>", "assembly_row_count_mismatch", table_name
                )
        if outcome["observations"] != len(rows):
            summary.finding(
                f"<batch:{number}>", "assembly_observation_mismatch",
                f"{outcome['observations']} != {len(rows)}",
            )
        if outcome["accepted"] + outcome["quarantined"] != len(rows):
            summary.finding(
                f"<batch:{number}>", "assembly_reconciliation_mismatch",
                f"accepted={outcome['accepted']}, quarantined={outcome['quarantined']}, "
                f"input={len(rows)}",
            )
        if outcome["blocking_issues"] != sum(
            1
            for row in rows
            for issue in row["issues"]
            if issue["severity"] == "error"
        ):
            summary.finding(
                f"<batch:{number}>", "assembly_issue_mismatch",
                f"Assembler reported {outcome['blocking_issues']} blocking issues",
            )
    except Exception as exc:
        summary.finding(
            f"<batch:{number}>", "assemble_validate_exception", f"{type(exc).__name__}: {exc}"
        )


def run(
    path: Path,
    *,
    batch_size: int,
    progress_every: int,
    expected_records: int | None,
    report_path: Path | None,
    preview_limit: int,
) -> int:
    if not path.is_file():
        print(f"Input not found: {path}", file=sys.stderr)
        return 2

    transformer = BidOpeningTransformer()
    summary = Summary(preview_limit)
    batch: list[dict[str, Any]] = []
    batch_number = 0

    print(f"Read-only preflight: {path}", flush=True)
    print("Transforms in bounded batches. No Silver Candidate or Iceberg writes.", flush=True)
    for ordinal, (source_id, payload_json) in enumerate(read_source_rows(path, batch_size), 1):
        result = process_one(source_id, payload_json, ordinal, transformer, summary)
        if result is not None:
            batch.append(result.to_dict())
        if len(batch) >= batch_size:
            batch_number += 1
            check_batch(batch, summary, batch_number)
            batch.clear()
        if summary.processed % progress_every == 0:
            print(
                f"processed={summary.processed:,} accepted={summary.accepted:,} "
                f"quarantined={summary.quarantined:,} blocking={summary.blocking_issues:,} "
                f"findings={summary.finding_count:,}",
                flush=True,
            )

    if batch:
        batch_number += 1
        check_batch(batch, summary, batch_number)
    if expected_records is not None and summary.processed != expected_records:
        summary.finding(
            "<input>", "record_count_mismatch",
            f"Export has {summary.processed} records, expected {expected_records}",
        )

    report = summary.as_dict(path, expected_records)
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n--- FINAL SUMMARY ---")
    print(
        f"processed={summary.processed:,} distinct_source_ids={len(summary.seen_source_ids):,} "
        f"accepted={summary.accepted:,} quarantine={summary.quarantined:,}"
    )
    print(
        f"quality_issues={summary.quality_issues:,} blocking_issues={summary.blocking_issues:,} "
        f"unlinked_lots={summary.unlinked_lots:,}"
    )
    print("silver_counts:", dict(summary.actual_counts))
    print("source_expected_children:", dict(summary.expected_counts))
    print("opening_shapes:", dict(summary.shapes.most_common()))
    print("collection_statuses:", dict(summary.statuses.most_common()))
    print("top_issue_codes:", dict(summary.issue_codes.most_common(20)))
    print("finding_types:", dict(summary.finding_types.most_common()))
    for example in summary.finding_preview[:20]:
        print("  SAMPLE:", example["source_id"], example["type"], example["detail"])
    print("RESULT:", report["result"])
    if report_path is not None:
        print(f"Local report: {report_path}")
    return 0 if report["result"] == "PASS" else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="DuckDB export (.parquet or .jsonl)")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--expected-records", type=int)
    parser.add_argument("--report", type=Path, help="Optional local JSON summary path")
    parser.add_argument("--preview-limit", type=int, default=100)
    args = parser.parse_args()
    if args.batch_size <= 0 or args.progress_every <= 0 or args.preview_limit < 0:
        parser.error("batch-size/progress-every must be positive, preview-limit nonnegative")
    return run(
        args.input,
        batch_size=args.batch_size,
        progress_every=args.progress_every,
        expected_records=args.expected_records,
        report_path=args.report,
        preview_limit=args.preview_limit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
