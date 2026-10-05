"""Count current Bronze rows using cached, parallel Parquet footer reads."""

import argparse
import hashlib
import json
import sqlite3
import struct
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from procurement.common.catalog import RESOURCE_CATALOG, SUPPORTED_RESOURCES
from procurement.common.settings import settings
from procurement.storage.object_store import create_s3_filesystem
from procurement.tools.bronze_explorer import BronzeTable, _select_current_files

LABELS = {"project_detail": "project", "khlcnt_plan_detail": "khlcnt",
          "khlcnt_bid_package_detail": "packagebid", "contractor_result_detail": "result", "bid_opening_detail": "opening"}


def footer_count(fs, uri, size):
    """Fetch only the footer, not Arrow's usual trailing 64 KB or S3 block cache."""
    key = uri.removeprefix("s3://")
    if size < 12:
        raise ValueError(f"Invalid Parquet size: {uri}")
    tail = fs.cat_file(key, start=size - 8, end=size)
    if len(tail) != 8 or tail[4:] != b"PAR1":
        raise ValueError(f"Invalid Parquet footer: {uri}")
    length = struct.unpack("<I", tail[:4])[0]
    if not 0 < length <= size - 12:
        raise ValueError(f"Invalid Parquet footer length: {uri}")
    metadata = fs.cat_file(key, start=size - 8 - length, end=size - 8)
    if len(metadata) != length:
        raise ValueError(f"Truncated Parquet footer: {uri}")
    return pq.read_metadata(pa.BufferReader(b"PAR1" + metadata + tail)).num_rows


def signature(info):
    marker = info.get("ETag") or info.get("etag") or info.get("LastModified") or info.get("mtime")
    return json.dumps([str(marker), info["size"]]) if marker is not None else None


def count_files(fs, files, metadata, database, *, namespace, workers=16):
    database = Path(database)
    database.parent.mkdir(parents=True, exist_ok=True)
    counts, pending = {}, []
    with sqlite3.connect(database, timeout=60) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS file_counts (
            namespace TEXT, uri TEXT, signature TEXT, records INTEGER,
            PRIMARY KEY (namespace, uri))""")
        cached = {uri: (sig, count) for uri, sig, count in con.execute(
            "SELECT uri, signature, records FROM file_counts WHERE namespace=?", (namespace,))}
        for uri in files:
            sig = signature(metadata[uri])
            if sig is not None and uri in cached and cached[uri][0] == sig:
                counts[uri] = cached[uri][1]
            else:
                pending.append((uri, sig))
        print(f"Files: {len(files)}; cached: {len(counts)}; footers to read: {len(pending)}", flush=True)

        def read(item):
            uri, sig = item
            return uri, sig, footer_count(fs, uri, metadata[uri]["size"])

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for index, (uri, sig, count) in enumerate(pool.map(read, pending), 1):
                counts[uri] = count
                if sig is not None:
                    con.execute("INSERT OR REPLACE INTO file_counts VALUES (?, ?, ?, ?)",
                                (namespace, uri, sig, count))
                if index % 500 == 0:
                    con.commit()
                    print(f"Footers: {index}/{len(pending)}", flush=True)
        con.commit()
    return counts


def build_report(selected, counts, committed, issues, start, end):
    rows = Counter({(year, LABELS.get(table.table, "noti")): 0
                    for table in selected for year in range(start.year, end.year + 1)})
    day_counts = Counter()
    table_resources = {t: d.identity.resource for d in RESOURCE_CATALOG for t in d.tables}
    for table, files in selected.items():
        for uri in files:
            parts = uri.rsplit("/", 3)
            day = parts[-3].removeprefix("source_date=")
            rows[(int(day[:4]), LABELS.get(table.table, "noti"))] += counts[uri]
            day_counts[(table_resources[table.table], day)] += counts[uri]
    issues = list(issues)
    for resource, days in committed.items():
        for day, attempt in days.items():
            actual = day_counts[(resource, day)]
            if actual != attempt.bronze_records:
                issues.append({"resource": resource, "source_date": day, "issue": "count_mismatch",
                               "expected_records": attempt.bronze_records, "actual_records": actual})
    missing_days = {resource: (end-start).days + 1 - len(days) for resource, days in committed.items()}
    totals = Counter()
    for (_, label), count in rows.items():
        totals[label] += count
    return {"counted_at": datetime.now(UTC).isoformat(), "start": str(start), "end": str(end),
            "rows": [{"year": year, "resource": label, "records": count}
                     for (year, label), count in sorted(rows.items())],
            "totals": dict(totals), "missing_days": missing_days, "issues": issues,
            "complete": not issues and not any(missing_days.values())}


def show(report, path, *, cached=False):
    print(f"{'CACHED SNAPSHOT' if cached else 'COUNTED'} at {report['counted_at']}")
    for label in ("project", "khlcnt", "packagebid", "noti", "result"):
        if label in report["totals"]:
            print(f"{label:<14} {report['totals'][label]:>15,}")
    if not report["complete"]:
        print(f"INCOMPLETE: issues={len(report['issues'])}; missing committed days={report['missing_days']}")
    print(f"Report: {path}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int)
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--resource", choices=("all", *SUPPORTED_RESOURCES), default="all")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--cache-dir", type=Path, default=Path("exports/bronze-counts"))
    parser.add_argument("--cached", action="store_true", help="Show last saved snapshot without accessing storage")
    args = parser.parse_args(argv)
    if args.year is not None:
        if args.start_year is not None or args.end_year is not None:
            parser.error("Use --year or --start-year/--end-year, not both")
        args.start_year = args.end_year = args.year
    if args.start_year is None or args.end_year is None:
        parser.error("Specify --year or both --start-year and --end-year")
    try:
        start, end = date(args.start_year, 1, 1), date(args.end_year, 12, 31)
    except ValueError as exc:
        parser.error(str(exc))
    if start > end or not 1 <= args.workers <= 32:
        parser.error("Invalid year range or workers (1..32)")
    namespace = hashlib.sha256(json.dumps([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])
                               .encode()).hexdigest()
    path = args.cache_dir / f"{args.start_year}-{args.end_year}-{args.resource}-{namespace[:12]}.json"
    if args.cached:
        if not path.exists():
            parser.error("No cached report for this scope. Run once without --cached.")
        report = json.loads(path.read_text(encoding="utf-8"))
        show(report, path, cached=True)
        return 0 if report["complete"] else 2
    started = time.perf_counter()
    fs = create_s3_filesystem()
    tables = [BronzeTable(d.identity.source, t) for d in RESOURCE_CATALOG
              if args.resource in ("all", d.identity.resource) for t in d.tables]
    metadata, committed, issues = {}, {}, []
    selected = _select_current_files(fs, bucket=settings.OBJECT_STORAGE_BUCKET, tables=tables,
                                     start=start, end=end, workers=args.workers, issues=issues,
                                     file_metadata=metadata, committed_days=committed)
    counts = count_files(fs, [uri for paths in selected.values() for uri in paths], metadata,
                         args.cache_dir / "footers.sqlite", namespace=namespace, workers=args.workers)
    report = build_report(selected, counts, committed, issues, start, end)
    report["elapsed_seconds"] = round(time.perf_counter()-started, 2)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
    temporary.replace(path)
    show(report, path)
    print(f"Elapsed: {report['elapsed_seconds']}s")
    return 0 if report["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
