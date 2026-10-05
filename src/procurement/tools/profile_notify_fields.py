"""Profile every committed notice payload in a year; write local evidence only."""

import argparse
import csv
import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from procurement.common.catalog import get_resource
from procurement.quality.contracts import lookup
from procurement.storage.committed import iter_committed_records, select_committed_days
from procurement.storage.object_store import create_s3_filesystem

OPENING_ROOTS = {
    "bid_opening_detail": ("notify.bidNoContractorResponse.bidNotification",
                           "roundmng.bidoBidroundMngViewDTO",
                           "bid_open.bidSubmissionByContractorViewResponse", "lot_open_detail",
                           "bid_open_technical.bidSubmissionByContractorViewResponse",
                           "bid_open_financial.bidSubmissionByContractorViewResponse",
                           "lot_open_detail_technical", "lot_open_detail_financial"),
}
ROOTS = {
    "notify_contractor_standard_detail": (
        "bidoNotifyContractorM", "bidNoContractorResponse.bidNotification",
    ),
    "notify_contractor_reoffer_detail": ("",),
    "notify_contractor_vk_adb_detail": ("bidoNotifyContractorP",),
}
RECORD_COLUMNS = (
    "table", "source_date", "run_id", "source_id", "source_version",
    "payload_paths", "populated_paths", "field_occurrences", "business_root",
    "business_fields", "populated_business_fields",
)


def is_populated(value):
    """Whitespace, null and empty containers are empty; zero and false are populated."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (dict, list)):
        return bool(value)
    return True


def walk(value, path="$"):
    """Keep exact object keys; normalize array indexes to [] without decoding strings."""
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = path + "[" + json.dumps(key, ensure_ascii=False) + "]"
            yield child_path, child
            yield from walk(child, child_path)
    elif isinstance(value, list):
        for child in value:
            child_path = path + "[]"
            yield child_path, child
            yield from walk(child, child_path)


@dataclass
class FieldCounts:
    present_records: int = 0
    non_null_records: int = 0
    populated_records: int = 0
    null_records: int = 0
    empty_records: int = 0
    occurrences: int = 0
    populated_occurrences: int = 0
    types: Counter = field(default_factory=Counter)


@dataclass
class TableProfile:
    records: int = 0
    missing_business_root: int = 0
    paths: dict = field(default_factory=dict)
    business_fields: dict = field(default_factory=dict)


def observe(counts, items):
    """Each path counts once per record; array occurrences are counted separately."""
    seen = defaultdict(set)
    occurrences = 0
    for path, value in items:
        node = counts.setdefault(path, FieldCounts())
        populated = is_populated(value)
        node.occurrences += 1
        node.populated_occurrences += populated
        node.types[type(value).__name__] += 1
        occurrences += 1
        flags = seen[path]
        flags.add("present_records")
        flags.add("null_records" if value is None else "non_null_records")
        flags.add("populated_records" if populated else "empty_records")
    for path, flags in seen.items():
        node = counts[path]
        for flag in flags:
            setattr(node, flag, getattr(node, flag) + 1)
    return len(seen), sum("populated_records" in flags for flags in seen.values()), occurrences


def profile_record(profile, table, record):
    payload = record["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    profile.records += 1
    paths, populated, occurrences = observe(profile.paths, walk(payload))
    root_path, root = None, None
    for candidate in (ROOTS | OPENING_ROOTS)[table]:
        value = lookup(payload, candidate)
        if isinstance(value, dict) and value:
            root_path, root = candidate or "$", value
            break
    profile.missing_business_root += root is None
    business_count, business_populated, _ = observe(
        profile.business_fields,
        walk(payload) if table == "bid_opening_detail" else root.items() if root else (),
    )
    return {
        "table": table, "source_date": str(record["source_date"])[:10],
        "run_id": record["run_id"], "source_id": record["source_id"],
        "source_version": record.get("source_version"), "payload_paths": paths,
        "populated_paths": populated, "field_occurrences": occurrences,
        "business_root": root_path, "business_fields": business_count,
        "populated_business_fields": business_populated,
    }


def profile_day(fs, day):
    profiles = {table: TableProfile() for table in (ROOTS | OPENING_ROOTS)}
    records = []
    # Exhaust the iterator so count, lineage and payload hashes are all verified.
    for table, record in iter_committed_records(fs, (day,), verify_hash=True):
        records.append(profile_record(profiles[table], table, record))
    return profiles, records


def merge_profiles(target, source):
    for table, incoming in source.items():
        profile = target[table]
        profile.records += incoming.records
        profile.missing_business_root += incoming.missing_business_root
        for attr in ("paths", "business_fields"):
            destination = getattr(profile, attr)
            for path, counts in getattr(incoming, attr).items():
                node = destination.setdefault(path, FieldCounts())
                for name in FieldCounts.__dataclass_fields__:
                    if name == "types":
                        node.types.update(counts.types)
                    else:
                        setattr(node, name, getattr(node, name) + getattr(counts, name))


def field_rows(profiles, attr):
    for table, profile in profiles.items():
        for path, counts in sorted(getattr(profile, attr).items()):
            row = {"table": table, "field": path, "total_records": profile.records}
            row.update({name: getattr(counts, name) for name in FieldCounts.__dataclass_fields__
                        if name != "types"})
            row["missing_records"] = profile.records - counts.present_records
            row["present_pct"] = round(100 * counts.present_records / profile.records, 6)
            row["populated_pct"] = round(100 * counts.populated_records / profile.records, 6)
            row["always_present"] = counts.present_records == profile.records
            row["always_populated"] = counts.populated_records == profile.records
            row["types"] = json.dumps(dict(counts.types), sort_keys=True)
            yield row


def write_field_csv(path, rows):
    rows = list(rows)
    columns = ["table", "field", "total_records", *(
        name for name in FieldCounts.__dataclass_fields__ if name != "types"
    ), "missing_records", "present_pct", "populated_pct", "always_present",
        "always_populated", "types"]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def run_profile(fs, selection, output, year, workers):
    profiles = {table: TableProfile() for table in (ROOTS | OPENING_ROOTS)}
    (output / "selection.json").write_text(
        json.dumps([asdict(day) for day in selection], default=str, indent=2), encoding="utf-8",
    )
    with (output / "records.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=RECORD_COLUMNS)
        writer.writeheader()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Bound queued results to one batch of days; never retain yearly raw payloads.
            for offset in range(0, len(selection), workers):
                batch = selection[offset:offset + workers]
                results = pool.map(lambda day: profile_day(fs, day), batch)
                for day, (daily_profiles, records) in zip(batch, results, strict=True):
                    merge_profiles(profiles, daily_profiles)
                    writer.writerows(records)
                    print(f"Verified {day.source_date}: {len(records):,} records", flush=True)
    write_field_csv(output / "fields.csv", field_rows(profiles, "paths"))
    write_field_csv(output / "business-fields.csv", field_rows(profiles, "business_fields"))
    summary = {
        "year": year, "completed_at": datetime.now(UTC).isoformat(),
        "success_days": len(selection), "files": sum(len(day.files) for day in selection),
        "records": sum(profile.records for profile in profiles.values()),
        "integrity": "count, lineage and payload hash verified for all selected records",
        "tables": {},
    }
    lines = [f"# Notice field profile {year}", "",
             f"Read all {summary['records']:,} records across {len(selection)} committed days.",
             "Count, lineage and payload hashes verified. Exact selection: selection.json.", "",
             "Percentages use ALL records in each table, including missing business roots.",
             "Present means the key exists, including null. Populated excludes null, whitespace",
             "and empty lists/objects; zero and false count as populated. Nonempty containers",
             "count as populated even when their children are null.",
             "Array [] paths count each record once; occurrences count every array element.",
             "A mixed array can count as both null and populated in the same record.",
             "JSON encoded strings are not decoded. Paths use quoted keys to avoid collisions.",
             "Business fields use the first nonempty supported root, with no per-field fallback",
             "or alias merging. This profile does not validate identity or root agreement.",
             "Observed 100% presence is evidence, not a universal business requirement.", ""]
    for table, profile in profiles.items():
        always = sorted(key for key, counts in profile.business_fields.items()
                        if counts.populated_records == profile.records)
        summary["tables"][table] = {
            "records": profile.records, "missing_business_root": profile.missing_business_root,
            "payload_paths": len(profile.paths), "business_fields": len(profile.business_fields),
            "always_populated_business_fields": always,
        }
        lines.extend([f"## {table}", "",
                      (f"Records: {profile.records:,}; missing business root: "
                       f"{profile.missing_business_root:,}; payload paths: {len(profile.paths)}."), "",
                      "Business fields populated in every observed record:", "",
                      ", ".join(f"`{key}`" for key in always) or "None / no records.", ""])
    (output / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource", choices=("notify_contractor", "bid_opening"), default="notify_contractor")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    try:
        start, end = date(args.year, 1, 1), date(args.year, 12, 31)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= args.workers <= 16:
        parser.error("--workers must be between 1 and 16")
    output = args.output or Path(
        f"exports/{args.resource}-field-profile-{args.year}-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    )
    output.mkdir(parents=True, exist_ok=False)
    print(f"Selecting committed days for {args.year} (requires every day SUCCESS)...", flush=True)
    fs = create_s3_filesystem()
    selection = select_committed_days(fs, get_resource(args.resource), start, end)
    summary = run_profile(fs, selection, output, args.year, args.workers)
    print(f"DONE: {summary['records']:,} records; reports: {output.resolve()}", flush=True)


if __name__ == "__main__":
    main()
