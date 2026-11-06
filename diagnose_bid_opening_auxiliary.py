"""Read-only root-cause report for the five KHAC dual-envelope auxiliary lot anomalies.

Run from the procurement-lakehouse repo root:
    python diagnose_bid_opening_auxiliary.py exports/bid_opening_preflight_exceptions.jsonl

Only reads JSONL, runs local transformer IN MEMORY, prints diagnosis. No Silver writes.
"""

from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from procurement.silver.domain.json_numbers import loads_bronze_payload
from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.transformers.bid_opening import BidOpeningTransformer
from datetime import datetime, timezone
import hashlib

TARGETS = {
    "IB2500278343", "IB2500398744", "IB2500411770",
    "IB2500539743", "IB2600332128",
}

PHASE = {
    "opening_out": "single",
    "opening_out_technical": "technical",
    "opening_out_financial": "financial",
    "opening_adb_wb": "single",
}

AUX_PATH = re.compile(
    r"^(opening_out(?:_technical|_financial)?|opening_adb_wb)"
    r"\.bidSubmissionByContractorViewResponse\.bidoLotOpenDetailDTOS\[(\d+)\]"
    r"(?:\.([a-zA-Z0-9_]+))?$"
)

FIELD_TO_ATTR = {
    "lotPrice": "lot_price",
    "lotFinalPrice": "lot_final_price",
    "discountPercent": "discount_rate",
    "lotPriceUnit": "currency",
    "passedYn": "passed_yn",
    "techScore": "tech_score",
    "evalPrice": "evaluation_price",
    "ranking": "evaluation_rank",
}


def value(d: Any, key: str) -> Any:
    return d.get(key) if isinstance(d, dict) else None


def describe_match(item: dict[str, Any]) -> str:
    return (
        f"phase={item['phase']}, bidOpenId={item['bid_open_id']}, "
        f"path={item['path']}, lot_price={item['lot_price']}, "
        f"lot_final_price={item['lot_final_price']}"
    )


def diagnosis(payload: dict[str, Any], result: Any, *, limit: int) -> None:
    sub_by_id = {
        child.child_id: child.source_id
        for child in result.children
        if child.typed_table == "opening_submission"
    }
    primary = []
    for child in result.children:
        if child.typed_table != "opening_lot_submission":
            continue
        attrs = child.typed_attributes or {}
        primary.append({
            "phase": child.role,
            "lot_no": attrs.get("lot_no"),
            "contractor": attrs.get("contractor_code"),
            "bid_open_id": sub_by_id.get(attrs.get("submission_id")),
            "path": child.path,
            "lot_price": attrs.get("lot_price"),
            "lot_final_price": attrs.get("lot_final_price"),
            "attrs": attrs,
        })

    issues = [i for i in result.issues if i.code in {
        "unmatched_auxiliary_lot", "conflicting_auxiliary_lot", "ambiguous_auxiliary_lot",
        "invalid_auxiliary_field", "duplicate_auxiliary_lot",
    }]
    summaries = Counter(i.code for i in issues)
    conflict_fields = Counter()
    unmatched_reasons = Counter()
    examples: dict[str, list[str]] = defaultdict(list)

    for issue in issues:
        match = AUX_PATH.match(issue.path)
        if not match:
            examples[issue.code].append(f"{issue.path}: unrecognized path shape")
            continue
        part, index_str, field = match.groups()
        phase = PHASE[part]
        resp = value(value(payload.get(part), "bidSubmissionByContractorViewResponse"), "bidoLotOpenDetailDTOS")
        index = int(index_str)
        if not isinstance(resp, list) or index >= len(resp):
            examples[issue.code].append(f"{issue.path}: auxiliary item missing")
            continue
        aux = resp[index]
        lot_no = value(aux, "lotNo")
        code = value(aux, "contractorCode")
        bid_open_id = value(aux, "bidOpenId")
        pair = [p for p in primary if p["lot_no"] == lot_no and p["contractor"] == code]
        same_phase = [p for p in pair if p["phase"] == phase]
        exact = [p for p in same_phase if bid_open_id is None or p["bid_open_id"] == bid_open_id]
        other = [p for p in pair if p["phase"] != phase]
        if issue.code == "unmatched_auxiliary_lot":
            if not pair:
                reason = "lot+contractor absent from all nested lots"
            elif not same_phase:
                reason = "lot+contractor exists only in OTHER phase"
            elif bid_open_id is not None and not exact:
                reason = "lot+contractor matches phase, but bidOpenId differs"
            else:
                reason = "match exists, but another identity/ambiguity guard failed"
            unmatched_reasons[reason] += 1
            example_category = f"unmatched_auxiliary_lot [{reason}]"
            detail = (
                f"{issue.path}\n"
                f"    auxiliary: phase={phase}, lot={lot_no}, contractor={code}, bidOpenId={bid_open_id}; reason={reason}\n"
                f"    same phase nested: " + (" | ".join(describe_match(p) for p in same_phase[:2]) or "NONE") + "\n"
                f"    other phase nested: " + (" | ".join(describe_match(p) for p in other[:2]) or "NONE")
            )
        elif issue.code == "conflicting_auxiliary_lot":
            conflict_fields[field or "<unknown>"] += 1
            example_category = f"conflicting_auxiliary_lot [{field or '<unknown>'}]"
            attr = FIELD_TO_ATTR.get(field or "")
            primary_values = [(p["attrs"].get(attr), p["path"], p["bid_open_id"]) for p in exact] if attr else []
            detail = (
                f"{issue.path}\n"
                f"    auxiliary: phase={phase}, lot={lot_no}, contractor={code}, bidOpenId={bid_open_id}, {field}={value(aux, field or '')!r}\n"
                f"    primary: {primary_values or 'NO EXACT MATCH'}"
            )
        else:
            example_category = issue.code
            detail = (
                f"{issue.path} | phase={phase}, lot={lot_no}, contractor={code}, "
                f"bidOpenId={bid_open_id}; same-phase={len(same_phase)}, exact={len(exact)}"
            )
        examples[example_category].append(detail)

    print("  issues:", dict(summaries))
    print("  unmatched reasons:", dict(unmatched_reasons))
    print("  conflict fields:", dict(conflict_fields))
    for code in sorted(examples):
        print(f"  EXAMPLES {code} ({min(limit, len(examples[code]))}/{len(examples[code])}):")
        for row in examples[code][:limit]:
            print("    " + row.replace("\n", "\n    "))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("jsonl", type=Path)
    p.add_argument("--samples", type=int, default=3, help="Example anomalies per code and opening")
    args = p.parse_args()
    if not args.jsonl.is_file():
        p.error(f"File not found: {args.jsonl}")
    seen = set()
    transformer = BidOpeningTransformer()
    for ordinal, line in enumerate(args.jsonl.open(encoding="utf-8"), start=1):
        if not line.strip():
            continue
        row = loads_bronze_payload(line)
        sid = str(row.get("source_id", ""))
        if sid not in TARGETS:
            continue
        seen.add(sid)
        raw = row["payload_json"]
        payload = loads_bronze_payload(raw)
        checksum = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        root = transformer.extract_root(payload)
        version = value(root, "notifyVersion")
        envelope = RawRecordEnvelope(
            namespace="muasamcong", table_name="bid_opening_detail", resource="bid_opening",
            source_date="2026-11-06", run_id="read-only-diagnostic", row_ordinal=ordinal,
            file_key=f"diagnose/{sid}.json", file_sha256=checksum, source_id=sid,
            source_version=str(version) if version is not None else None,
            content_hash=checksum, ingested_at=datetime(2026, 11, 6, tzinfo=timezone.utc),
            payload=payload,
        )
        result = transformer.transform(envelope)
        print(f"\n=== {sid} ===")
        diagnosis(payload, result, limit=max(1, args.samples))
    missing = TARGETS - seen
    if missing:
        print("Missing target source IDs:", ", ".join(sorted(missing)))
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
