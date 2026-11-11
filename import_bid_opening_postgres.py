#!/usr/bin/env python3
"""Safely restore export_bid_opening_postgres.py ZIPs to Bronze S3 + PostgreSQL.

Compatible with procurement-bronze-pg-parquet-share-v1 only, NOT bronze_transfer.

Run from the destination procurement-lakehouse repository root:
    uv run python import_bid_opening_postgres.py --archive imports/bid-opening-2022-postgres.zip --dry-run
    uv run python import_bid_opening_postgres.py --archive imports/bid-opening-2022-postgres.zip

Safety design:
- Validates ZIP manifest, file SHA-256, Parquet row counts and Bronze row lineage.
- Never overwrites existing S3 objects with different content.
- Does not refresh or replace already committed PostgreSQL partitions.
- Publishes metadata atomically per day, only after its Parquet is on S3 and verified.
- Preserves source attempt UUIDs; maps historical payload run_id to dagster_run_id.
- After interruption, rerun the command to reuse verified uploads and skip commits.
- No deletion, schema migration, external API calls, or Docker manipulation.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4
from zipfile import ZIP_STORED, ZipFile

FORMAT = "procurement-bronze-pg-parquet-share-v1"
SOURCE = "muasamcong"
RESOURCE = "bid_opening"
TABLE = "bid_opening_detail"
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
KEY_RE = re.compile(
    r"^bronze/muasamcong/bid_opening_detail/"
    r"source_date=(\d{4}-\d{2}-\d{2})/"
    r"run_id=[A-Za-z0-9_-]+/[^/]+\.parquet$"
)
CHUNK = 1024 * 1024


@dataclass(frozen=True)
class FilePlan:
    key: str
    bucket: str
    number: int
    rows: int
    size: int
    sha: str

    @property
    def member(self) -> str:
        return "objects/" + self.key


@dataclass(frozen=True)
class DayPlan:
    day: date
    partition_id: UUID
    attempt_id: UUID
    commit_id: UUID
    records: int
    files: tuple[FilePlan, ...]


@dataclass(frozen=True)
class ArchivePlan:
    year: int
    days: tuple[DayPlan, ...]
    file_count: int
    record_count: int


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _uuid(value: Any, label: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid {label}") from exc


def _parse_manifest(archive: ZipFile) -> ArchivePlan:
    members = archive.infolist()
    names = [m.filename for m in members]
    if len(names) != len(set(names)):
        raise ValueError("ZIP contains duplicate member names")
    if "manifest.json" not in names:
        raise ValueError("ZIP lacks manifest.json (wrong transfer format?)")
    if archive.getinfo("manifest.json").file_size > 32 * 1024 * 1024:
        raise ValueError("Manifest too large")
    for entry in members:
        if entry.is_dir() or entry.flag_bits & 1 or "\\" in entry.filename:
            raise ValueError(f"Unsupported ZIP entry: {entry.filename!r}")
        if any(part in ("", ".", "..") for part in entry.filename.split("/")):
            raise ValueError(f"Unsafe ZIP path: {entry.filename!r}")
    manifest = json.loads(archive.read("manifest.json"))
    if not isinstance(manifest, dict):
        raise ValueError("Invalid manifest")
    if (manifest.get("format"), manifest.get("source"), manifest.get("resource")) != (
        FORMAT, SOURCE, RESOURCE
    ):
        raise ValueError("Incorrect archive format/source/resource")
    year = _positive_int(manifest.get("year"), "year")
    if not 2000 <= year <= 2100:
        raise ValueError("Archive year out of range")
    year_length = 366 if calendar.isleap(year) else 365
    if _positive_int(manifest.get("expected_days"), "expected_days") != year_length:
        raise ValueError("Incorrect expected_days")
    days_raw = manifest.get("days")
    if not isinstance(days_raw, list):
        raise ValueError("Manifest days must be a list")
    if _positive_int(manifest.get("committed_days"), "committed_days") != len(days_raw):
        raise ValueError("Committed day count mismatch")
    if bool(manifest.get("coverage_complete")) != (len(days_raw) == year_length):
        raise ValueError("Coverage flag mismatch")
    days: list[DayPlan] = []
    expected_members = {"manifest.json"}
    if "README.txt" in names:
        expected_members.add("README.txt")
    all_keys: set[str] = set()
    all_ids: set[UUID] = set()
    total_files = total_rows = total_bytes = 0
    for raw in days_raw:
        if not isinstance(raw, dict):
            raise ValueError("Invalid day")
        day = date.fromisoformat(raw["source_date"])
        if day.year != year or day in {entry.day for entry in days}:
            raise ValueError(f"Duplicate or out-of-year date: {day}")
        partition_id = _uuid(raw.get("partition_id"), "partition_id")
        attempt_id = _uuid(raw.get("attempt_id"), "attempt_id")
        commit_id = _uuid(raw.get("commit_id"), "commit_id")
        if len({partition_id, attempt_id, commit_id}) != 3:
            raise ValueError(f"Overlapping UUIDs for {day}")
        for uid in (partition_id, attempt_id, commit_id):
            if uid in all_ids:
                raise ValueError(f"Repeated source UUID: {uid}")
            all_ids.add(uid)
        records = _positive_int(raw.get("record_count"), "record_count")
        files_raw = raw.get("files")
        if not isinstance(files_raw, list):
            raise ValueError(f"Missing files array for {day}")
        if _positive_int(raw.get("file_count"), "file_count") != len(files_raw):
            raise ValueError(f"File count mismatch for {day}")
        files: list[FilePlan] = []
        for raw_file in files_raw:
            if not isinstance(raw_file, dict):
                raise ValueError("Invalid file descriptor")
            key = raw_file.get("object_key")
            if not isinstance(key, str) or not (match := KEY_RE.fullmatch(key)):
                raise ValueError(f"Unexpected Bronze object key: {key!r}")
            if match.group(1) != day.isoformat() or ".." in key.split("/"):
                raise ValueError(f"Incorrect file date/path: {key!r}")
            if key in all_keys:
                raise ValueError(f"Repeated Parquet object: {key}")
            all_keys.add(key)
            if raw_file.get("table_name") != TABLE:
                raise ValueError(f"Wrong table for {key}")
            bucket = raw_file.get("bucket")
            if not isinstance(bucket, str) or not bucket or "/" in bucket or "\\" in bucket:
                raise ValueError(f"Invalid source bucket: {bucket!r}")
            sha = raw_file.get("sha256")
            if not isinstance(sha, str) or not SHA_RE.fullmatch(sha):
                raise ValueError(f"Invalid SHA for {key}")
            f = FilePlan(
                key=key,
                bucket=bucket,
                number=_positive_int(raw_file.get("file_number"), "file_number"),
                rows=_positive_int(raw_file.get("row_count"), "row_count"),
                size=_positive_int(raw_file.get("size_bytes"), "size_bytes"),
                sha=sha,
            )
            if f.size == 0:
                raise ValueError(f"Committed Parquet must have positive byte size: {key}")
            if f.member not in names or archive.getinfo(f.member).file_size != f.size:
                raise ValueError(f"Missing or wrong-size ZIP member: {f.member}")
            if archive.getinfo(f.member).compress_type != ZIP_STORED:
                raise ValueError(f"Unexpected compression in {f.member}")
            expected_members.add(f.member)
            files.append(f)
            total_bytes += f.size
        if sorted(f.number for f in files) != list(range(len(files))):
            raise ValueError(f"Nonconsecutive file numbers for {day}")
        if sum(f.rows for f in files) != records:
            raise ValueError(f"Row count mismatch for {day}")
        total_files += len(files)
        total_rows += records
        days.append(DayPlan(day, partition_id, attempt_id, commit_id, records, tuple(files)))
    if set(names) != expected_members:
        raise ValueError("ZIP contains extra/missing files")
    if (_positive_int(manifest.get("file_count"), "file_count") != total_files
        or _positive_int(manifest.get("record_count"), "record_count") != total_rows
        or _positive_int(manifest.get("bytes"), "bytes") != total_bytes):
        raise ValueError("Archive totals do not match file/day data")
    return ArchivePlan(year, tuple(sorted(days, key=lambda d: d.day)), total_files, total_rows)


def _checksum_stream(stream) -> tuple[int, str]:
    hasher = hashlib.sha256()
    size = 0
    while chunk := stream.read(CHUNK):
        hasher.update(chunk)
        size += len(chunk)
    return size, hasher.hexdigest()


def _verify_archive_files(archive: ZipFile, plan: ArchivePlan) -> dict[date, str | None]:
    """Verify every Parquet's bytes, footer and its complete day/run-id lineage."""
    import pyarrow.parquet as pq

    lineage: dict[date, str | None] = {}
    processed = 0
    for day in plan.days:
        source_run_ids: set[str] = set()
        for f in day.files:
            with tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024) as spool:
                hasher = hashlib.sha256()
                size = 0
                with archive.open(f.member) as inp:
                    while chunk := inp.read(CHUNK):
                        hasher.update(chunk)
                        spool.write(chunk)
                        size += len(chunk)
                if size != f.size or hasher.hexdigest() != f.sha:
                    raise ValueError(f"ZIP file SHA/size differs from manifest: {f.key}")
                spool.seek(0)
                pf = pq.ParquetFile(spool)
                if pf.metadata.num_rows != f.rows:
                    raise ValueError(f"Parquet row count mismatch: {f.key}")
                if not {'run_id', 'source_date'}.issubset(pf.schema_arrow.names):
                    raise ValueError(f"Parquet lacks Bronze lineage columns: {f.key}")
                nrows = 0
                for batch in pf.iter_batches(batch_size=8192, columns=["run_id", "source_date"]):
                    ids = batch.column(0).to_pylist()
                    dates = batch.column(1).to_pylist()
                    nrows += len(ids)
                    for run_id, value in zip(ids, dates, strict=True):
                        if not run_id or str(value)[:10] != day.day.isoformat():
                            raise ValueError(f"Parquet lineage date/run_id invalid: {f.key}")
                        source_run_ids.add(str(run_id))
                        if len(source_run_ids) > 1:
                            raise ValueError(f"Multiple original run_ids for {day.day}")
                if nrows != f.rows:
                    raise ValueError(f"Parquet data rows mismatch: {f.key}")
            processed += 1
            if processed % 50 == 0:
                print(f"  Verified {processed}/{plan.file_count} Parquet objects", flush=True)
        lineage[day.day] = next(iter(source_run_ids)) if source_run_ids else None
    return lineage


def _database_preflight(engine, plan: ArchivePlan) -> tuple[set[date], list[DayPlan]]:
    from sqlalchemy import select
    from procurement.metadata.postgres.schema import attempts, commits, partition_leases, partitions

    with engine.connect() as conn:
        existing = conn.execute(
            select(partitions.c.source_date, partitions.c.current_commit_id,
                   partition_leases.c.attempt_id.label("lease_attempt_id"))
            .select_from(partitions.outerjoin(partition_leases,
                                            partition_leases.c.partition_id == partitions.c.id))
            .where(partitions.c.source == SOURCE, partitions.c.resource == RESOURCE,
                   partitions.c.source_date.between(date(plan.year, 1, 1), date(plan.year, 12, 31)))
        ).mappings().all()
        current = {row["source_date"]: row for row in existing}
        skipped = {day.day for day in plan.days if day.day in current and current[day.day]["current_commit_id"] is not None}
        pending = [day for day in plan.days if day.day not in skipped]
        blockers = [str(day.day) for day in pending if day.day in current and current[day.day]["lease_attempt_id"] is not None]
        if blockers:
            raise RuntimeError(f"Active/stale partition lease(s) must be resolved before import: {', '.join(blockers[:10])}")
        if pending:
            a_ids = [day.attempt_id for day in pending]
            c_ids = [day.commit_id for day in pending]
            p_ids = [day.partition_id for day in pending if day.day not in current]
            if conn.execute(select(attempts.c.id).where(attempts.c.id.in_(a_ids))).first() is not None:
                raise RuntimeError("A source attempt UUID is already used by a different destination state")
            if conn.execute(select(commits.c.id).where(commits.c.id.in_(c_ids))).first() is not None:
                raise RuntimeError("A source commit UUID is already used by a different destination state")
            if p_ids and conn.execute(select(partitions.c.id).where(partitions.c.id.in_(p_ids))).first() is not None:
                raise RuntimeError("A source partition UUID is already used by another destination date")
    return skipped, pending


def _upload_verified(fs, archive: ZipFile, f: FilePlan, target_bucket: str) -> bool:
    """Returns True if uploaded, False if identical object was already present."""
    target = f"{target_bucket}/{f.key}"
    if fs.exists(target):
        with fs.open(target, "rb") as reader:
            actual = _checksum_stream(reader)
        if actual != (f.size, f.sha):
            raise RuntimeError(f"Destination S3 object has different bytes; refusing overwrite: {target}")
        return False
    writer = fs.open(target, "wb", autocommit=False)
    try:
        with archive.open(f.member) as source:
            copied_size = 0
            hasher = hashlib.sha256()
            while chunk := source.read(CHUNK):
                copied_size += len(chunk)
                hasher.update(chunk)
                writer.write(chunk)
        if (copied_size, hasher.hexdigest()) != (f.size, f.sha):
            raise RuntimeError(f"ZIP changed during upload: {f.member}")
        writer.close()
        writer.commit()
    except BaseException:
        try:
            writer.discard()
        except Exception:
            pass
        raise
    with fs.open(target, "rb") as reader:
        actual = _checksum_stream(reader)
    if actual != (f.size, f.sha):
        raise RuntimeError(f"Uploaded S3 object failed readback SHA: {target}")
    return True


def _publish_day(engine, day: DayPlan, run_id: str | None, target_bucket: str) -> bool:
    """One atomic DB transaction, no changes to any existing current commit."""
    from sqlalchemy import select
    from procurement.metadata.postgres.schema import attempts, commit_files, commits, partition_leases, partitions

    now = datetime.now(UTC)
    with engine.begin() as conn:
        row = conn.execute(
            select(partitions.c.id, partitions.c.current_commit_id)
            .where(partitions.c.source == SOURCE, partitions.c.resource == RESOURCE,
                   partitions.c.source_date == day.day)
            .with_for_update()
        ).mappings().one_or_none()
        if row is not None and row["current_commit_id"] is not None:
            return False
        if row is None:
            part_id = day.partition_id
            conn.execute(partitions.insert().values(
                id=part_id, source=SOURCE, resource=RESOURCE, source_date=day.day,
                current_commit_id=None, created_at=now,
            ))
            conn.execute(partition_leases.insert().values(
                partition_id=part_id, generation=0,
                attempt_id=None, owner_id=None, heartbeat_at=None, expires_at=None,
            ))
        else:
            part_id = row["id"]
            lease = conn.execute(select(partition_leases.c.attempt_id)
                                 .where(partition_leases.c.partition_id == part_id)
                                 .with_for_update()).one_or_none()
            if lease is None or lease[0] is not None:
                raise RuntimeError(f"Missing/held partition lease for {day.day}; refusing import")
        conn.execute(attempts.insert().values(
            id=day.attempt_id, partition_id=part_id, request_id=day.attempt_id,
            dagster_run_id=run_id if run_id and run_id != str(day.attempt_id) else None,
            kind="import", status="success", owner_id=day.attempt_id,
            lease_generation=1,
            config={"format": FORMAT, "source_partition_id": str(day.partition_id),
                    "source_commit_id": str(day.commit_id)},
            started_at=now, completed_at=now,
        ))
        conn.execute(commits.insert().values(
            id=day.commit_id, partition_id=part_id, attempt_id=day.attempt_id,
            parent_commit_id=None, data_version=uuid4(),
            record_count=day.records, file_count=len(day.files),
            verification={"imported_from": FORMAT, "files_verified": len(day.files),
                          "source_run_id": run_id, "source_commit_id": str(day.commit_id)},
            committed_at=now,
        ))
        if day.files:
            conn.execute(commit_files.insert(), [dict(
                commit_id=day.commit_id, file_number=f.number, table_name=TABLE,
                bucket=target_bucket, object_key=f.key,
                row_count=f.rows, size_bytes=f.size, sha256=f.sha,
                schema_version=1,
            ) for f in day.files])
        conn.execute(partitions.update()
                     .where(partitions.c.id == part_id)
                     .values(current_commit_id=day.commit_id))
    return True


def _import_archive(path: Path, engine, fs, target_bucket: str, dry_run: bool) -> dict[str, Any]:
    with ZipFile(path) as zipf:
        plan = _parse_manifest(zipf)
        print(f"{path.name}: {len(plan.days)} committed days, "
              f"{plan.record_count:,} records, {plan.file_count} files", flush=True)
        print("Verifying ZIP Parquet SHA, row counts and lineage...", flush=True)
        source_runs = _verify_archive_files(zipf, plan)
        skipped, pending = _database_preflight(engine, plan)
        print(f"Destination: {len(skipped)} already committed; {len(pending)} days to import", flush=True)
        summary = {"archive": str(path), "year": plan.year, "dry_run": dry_run,
                   "already_committed": len(skipped), "planned": len(pending),
                   "imported": 0, "reused_parquet": 0, "uploaded_parquet": 0,
                   "records_in_archive": plan.record_count,
                   "files_in_archive": plan.file_count}
        if dry_run:
            return summary
        for i, day in enumerate(pending, 1):
            for f in day.files:
                uploaded = _upload_verified(fs, zipf, f, target_bucket)
                summary["uploaded_parquet" if uploaded else "reused_parquet"] += 1
            did_publish = _publish_day(engine, day, source_runs[day.day], target_bucket)
            if did_publish:
                summary["imported"] += 1
            else:
                summary["already_committed"] += 1
            if i % 20 == 0 or i == len(pending):
                print(f"  Progress {i}/{len(pending)} days; "
                      f"new commits={summary['imported']}, "
                      f"Parquet uploaded={summary['uploaded_parquet']}", flush=True)
        return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, nargs="+", required=True,
                        help="One or more bid-opening-YYYY-postgres.zip archives")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, never write S3/DB")
    args = parser.parse_args()
    try:
        from procurement.common.settings import settings
        from procurement.infrastructure.database import create_application_engine
        from procurement.storage.object_store import create_s3_filesystem

        paths = args.archive
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate archive paths")
        engine = create_application_engine()
        fs = None if args.dry_run else create_s3_filesystem()
        reports = []
        try:
            for path in paths:
                reports.append(_import_archive(path, engine, fs, settings.OBJECT_STORAGE_BUCKET, args.dry_run))
        finally:
            engine.dispose()
        print(json.dumps(reports, ensure_ascii=False, indent=2), flush=True)
        return 0
    except Exception as exc:
        print(f"IMPORT FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
