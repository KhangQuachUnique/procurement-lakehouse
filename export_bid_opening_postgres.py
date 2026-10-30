#!/usr/bin/env python3
"""Export active PostgreSQL-committed Bid Opening Bronze Parquet into per-year ZIPs.

Run from the procurement-lakehouse repository root with the project's Python env:
    uv run python export_bid_opening_postgres.py --years 2022 2023 2024

This is a RAW PARQUET SHARING archive, NOT a bronze_transfer import archive.
It reads PostgreSQL and S3 only; it does not modify either system.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

from sqlalchemy import text

from procurement.common.settings import settings
from procurement.infrastructure.database import create_application_engine
from procurement.storage.object_store import create_s3_filesystem

SQL = text("""
SELECT
    p.id::text AS partition_id,
    p.source,
    p.resource,
    p.source_date,
    c.id::text AS commit_id,
    c.attempt_id::text AS attempt_id,
    c.record_count,
    c.file_count,
    cf.file_number,
    cf.table_name,
    cf.bucket,
    cf.object_key,
    cf.row_count AS file_rows,
    cf.size_bytes,
    cf.sha256
FROM bronze_meta.partitions AS p
JOIN bronze_meta.commits AS c ON c.id = p.current_commit_id
LEFT JOIN bronze_meta.commit_files AS cf ON cf.commit_id = c.id
WHERE p.source = 'muasamcong'
  AND p.resource = 'bid_opening'
  AND p.source_date >= :first_day
  AND p.source_date < :last_day
ORDER BY p.source_date, cf.file_number
""")

README = """Bid Opening Bronze — CURRENT PostgreSQL commits, raw Parquet sharing archive.

This archive is NOT compatible with:
  python -m procurement.tools.bronze_transfer import

The legacy bronze_transfer format requires S3 _control run/day/page manifests.
This archive instead uses bronze_meta PostgreSQL commit records. Do not feed it
into the old importer or blindly copy the entire ZIP into an S3 bucket.

To inspect locally, extract the ZIP and use DuckDB:
  SELECT COUNT(*) FROM read_parquet(
    'objects/bronze/muasamcong/bid_opening_detail/**/*.parquet',
    union_by_name=true
  );

manifest.json includes all days (including 0-record days), current PostgreSQL
commit and attempt IDs, row counts, and checksums for every included Parquet.
Restoring this archive to a new live procurement-lakehouse installation needs
a PostgreSQL-aware import tool that restores referential integrity and lineage.
"""


def _year_info(rows: list[dict], year: int, allow_partial: bool) -> dict:
    days: dict[str, dict] = {}
    for r in rows:
        day = r["source_date"].isoformat()
        if day not in days:
            days[day] = {
                "source_date": day,
                "partition_id": r["partition_id"],
                "commit_id": r["commit_id"],
                "attempt_id": r["attempt_id"],
                "record_count": int(r["record_count"]),
                "file_count": int(r["file_count"]),
                "files": [],
            }
        if r["object_key"] is not None:
            days[day]["files"].append({
                "table_name": r["table_name"],
                "bucket": r["bucket"],
                "object_key": r["object_key"],
                "file_number": int(r["file_number"]),
                "row_count": int(r["file_rows"]),
                "size_bytes": int(r["size_bytes"]),
                "sha256": r["sha256"],
            })
    expected_days = (date(year + 1, 1, 1) - date(year, 1, 1)).days
    if len(days) != expected_days and not allow_partial:
        raise RuntimeError(
            f"{year}: only {len(days)}/{expected_days} committed days. "
            "Pass --allow-partial to explicitly export an incomplete year."
        )
    for d in days.values():
        if d["file_count"] != len(d["files"]):
            raise RuntimeError(
                f"{d['source_date']}: PostgreSQL says {d['file_count']} files, "
                f"but found {len(d['files'])} in commit_files"
            )
        if sum(f["row_count"] for f in d["files"]) != d["record_count"]:
            raise RuntimeError(
                f"{d['source_date']}: PostgreSQL record_count does not match file row counts"
            )
        for f in d["files"]:
            if f["bucket"] != settings.OBJECT_STORAGE_BUCKET:
                raise RuntimeError(
                    f"{d['source_date']}: file belongs to bucket {f['bucket']!r}; "
                    f"expected {settings.OBJECT_STORAGE_BUCKET!r}"
                )
            if not f["object_key"].startswith("bronze/") or ".." in Path(f["object_key"]).parts:
                raise RuntimeError(f"Unsafe Parquet key: {f['object_key']!r}")
    all_files = [f for d in days.values() for f in d["files"]]
    return {
        "format": "procurement-bronze-pg-parquet-share-v1",
        "source": "muasamcong",
        "resource": "bid_opening",
        "year": year,
        "committed_days": len(days),
        "expected_days": expected_days,
        "coverage_complete": len(days) == expected_days,
        "record_count": sum(d["record_count"] for d in days.values()),
        "file_count": len(all_files),
        "bytes": sum(f["size_bytes"] for f in all_files),
        "days": list(days.values()),
    }


def _zip_year(fs, manifest: dict, out: Path) -> None:
    if out.exists():
        raise FileExistsError(f"Will not overwrite existing file: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".part")
    if tmp.exists():
        raise FileExistsError(f"Leftover partial export exists: {tmp}")
    try:
        with ZipFile(tmp, "w", compression=ZIP_STORED, allowZip64=True) as zipf:
            zipf.writestr("README.txt", README)
            for day in manifest["days"]:
                for file in day["files"]:
                    src = f"{file['bucket']}/{file['object_key']}"
                    dest = f"objects/{file['object_key']}"
                    h = hashlib.sha256()
                    size = 0
                    with fs.open(src, "rb") as inp, zipf.open(dest, "w", force_zip64=True) as outfp:
                        while chunk := inp.read(1024 * 1024):
                            h.update(chunk)
                            outfp.write(chunk)
                            size += len(chunk)
                    if size != file["size_bytes"] or h.hexdigest() != file["sha256"]:
                        raise RuntimeError(f"Parquet checksum/size mismatch: {src}")
            zipf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        with ZipFile(tmp, "r") as z:
            if z.testzip() is not None:
                raise RuntimeError(f"ZIP CRC verification failed: {tmp}")
        tmp.rename(out)
    finally:
        tmp.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, nargs="+", default=[2022, 2023, 2024])
    parser.add_argument("--out-dir", type=Path, default=Path("exports"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    engine = create_application_engine()
    fs = None if args.dry_run else create_s3_filesystem()
    try:
        with engine.connect() as conn:
            for year in args.years:
                if year < 2000 or year >= date.today().year:
                    raise ValueError(f"Not a closed historical calendar year: {year}")
                rows = [dict(r) for r in conn.execute(SQL, {
                    "first_day": date(year, 1, 1), "last_day": date(year + 1, 1, 1)
                }).mappings()]
                manifest = _year_info(rows, year, args.allow_partial)
                path = args.out_dir / f"bid-opening-{year}-postgres.zip"
                print(json.dumps({
                    "year": year,
                    "committed_days": manifest["committed_days"],
                    "expected_days": manifest["expected_days"],
                    "records": manifest["record_count"],
                    "parquet_files": manifest["file_count"],
                    "parquet_bytes": manifest["bytes"],
                    "destination": str(path),
                    "dry_run": args.dry_run,
                }, indent=2), flush=True)
                if not args.dry_run:
                    _zip_year(fs, manifest, path)
                    print(f"VERIFIED ZIP: {path}", flush=True)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Export failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
