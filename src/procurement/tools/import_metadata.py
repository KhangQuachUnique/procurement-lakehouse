"""Migrates historical S3 Bronze manifests to PostgreSQL metadata (Part H)."""

import argparse
import hashlib
import json
import logging
import sys
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from procurement.common.attempts import effective_attempt
from procurement.common.catalog import DEFAULT_SOURCE, RESOURCE_CATALOG, SUPPORTED_RESOURCES
from procurement.common.settings import settings
from procurement.infrastructure.database import create_application_engine
from procurement.metadata.postgres.schema import (
    attempts,
    commit_files,
    commits,
    partition_leases,
    partitions,
)
from procurement.models.control import DayManifest, DayStatus
from procurement.storage.control import list_day_manifests
from procurement.storage.object_store import create_s3_filesystem

logger = logging.getLogger(__name__)


def compute_sha256(fs: s3fs.S3FileSystem, key: str, chunk_size: int = 1024 * 1024) -> str:
    """Stream and compute SHA-256 for an S3 object."""
    h = hashlib.sha256()
    with fs.open(key, "rb") as f:
        while chunk := f.read(chunk_size):
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            h.update(chunk)
    return h.hexdigest()


def read_parquet_rows(fs: s3fs.S3FileSystem, key: str, size: int) -> int:
    """Fetch only Parquet footer to get exact row count."""
    if size < 12:
        return 0
    tail = fs.cat_file(key, start=size - 8, end=size)
    if isinstance(tail, str):
        tail = tail.encode("utf-8")
    if len(tail) != 8 or tail[4:] != b"PAR1":
        return 0
    import struct

    length = struct.unpack("<I", tail[:4])[0]
    if not (0 < length <= size - 12):
        return 0
    meta_bytes = fs.cat_file(key, start=size - 8 - length, end=size - 8)
    if isinstance(meta_bytes, str):
        meta_bytes = meta_bytes.encode("utf-8")
    return pq.read_metadata(pa.BufferReader(b"PAR1" + meta_bytes + tail)).num_rows


def discover_parquet_files(
    fs: s3fs.S3FileSystem,
    bucket: str,
    source: str,
    tables: tuple[str, ...],
    source_date_str: str,
    run_id: str,
) -> list[dict[str, Any]]:
    """Discover all Parquet files written under run_id for a given source_date."""
    discovered: list[dict[str, Any]] = []
    file_number = 0

    for table in tables:
        patterns = [
            f"{bucket}/bronze/{source}/{table}/source_date={source_date_str}/run_id={run_id}/*.parquet",
            f"{bucket}/bronze/{table}/source_date={source_date_str}/run_id={run_id}/*.parquet",
        ]
        keys: list[str] = []
        for p in patterns:
            keys.extend([str(k) for k in fs.glob(p)])
            if keys:
                break

        for key in sorted(set(keys)):
            try:
                info = fs.info(key)
                size = info["size"]
            except Exception:  # noqa: BLE001
                size = 0

            obj_key = key.removeprefix(f"{bucket}/")
            rows = read_parquet_rows(fs, key, size)
            sha = compute_sha256(fs, key)

            discovered.append(
                {
                    "file_number": file_number,
                    "table_name": table,
                    "bucket": bucket,
                    "object_key": obj_key,
                    "row_count": rows,
                    "size_bytes": max(size, 1),
                    "sha256": sha,
                    "schema_version": 1,
                }
            )
            file_number += 1

    return discovered


def import_resource_manifests(
    engine: Engine,
    fs: s3fs.S3FileSystem,
    resource: str,
    *,
    source: str = DEFAULT_SOURCE,
    bucket: str = settings.OBJECT_STORAGE_BUCKET,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Find effective DayManifests on S3 and import into PostgreSQL."""
    definition = next((d for d in RESOURCE_CATALOG if d.identity.resource == resource), None)
    if not definition:
        raise ValueError(f"Unknown resource: {resource}")

    manifests = list_day_manifests(fs, definition.identity)
    by_date: dict[str, list[DayManifest]] = defaultdict(list)
    for m in manifests:
        if m.status is DayStatus.SUCCESS:
            by_date[str(m.source_date)].append(m)

    effective_days: dict[str, DayManifest] = {}
    for day_str, attempts_list in by_date.items():
        eff = effective_attempt(attempts_list)
        if eff is not None:
            effective_days[day_str] = eff

    stats = {
        "resource": resource,
        "total_manifest_days": len(effective_days),
        "imported": 0,
        "skipped": 0,
        "mismatches": 0,
        "errors": [],
    }

    if dry_run:
        stats["dry_run"] = True
        return stats

    with engine.begin() as conn:
        for day_str, manifest in sorted(effective_days.items()):
            source_date = manifest.source_date

            # Check if partition already exists in DB
            part_stmt = (
                sa.select(partitions.c.id, partitions.c.current_commit_id)
                .where(
                    partitions.c.source == source,
                    partitions.c.resource == resource,
                    partitions.c.source_date == source_date,
                )
                .with_for_update()
            )
            part_row = conn.execute(part_stmt).mappings().one_or_none()

            if part_row and part_row["current_commit_id"] is not None:
                # Already committed
                stats["skipped"] += 1
                continue

            # Deterministic IDs for reproducible migration
            part_id = (
                part_row["id"]
                if part_row
                else uuid5(NAMESPACE_URL, f"part/{source}/{resource}/{day_str}")
            )
            attempt_id = uuid5(
                NAMESPACE_URL, f"attempt/{source}/{resource}/{day_str}/{manifest.run_id}"
            )
            commit_id = uuid5(
                NAMESPACE_URL, f"commit/{source}/{resource}/{day_str}/{manifest.run_id}"
            )

            now = datetime.now(UTC)
            started_at = manifest.started_at or now
            completed_at = manifest.completed_at or started_at

            # Discover files from S3
            files_data = discover_parquet_files(
                fs,
                bucket,
                source,
                definition.tables,
                day_str,
                manifest.run_id,
            )

            # Reconcile counts
            total_parquet_rows = sum(f["row_count"] for f in files_data)
            record_count = manifest.bronze_records
            if files_data and record_count != total_parquet_rows:
                stats["mismatches"] += 1
                record_count = max(record_count, total_parquet_rows)

            # Insert partition if not present
            if not part_row:
                conn.execute(
                    partitions.insert().values(
                        id=part_id,
                        source=source,
                        resource=resource,
                        source_date=source_date,
                        current_commit_id=None,
                        created_at=started_at,
                    )
                )
                conn.execute(
                    partition_leases.insert().values(
                        partition_id=part_id,
                        generation=0,
                        attempt_id=None,
                        owner_id=None,
                        heartbeat_at=None,
                        expires_at=None,
                    )
                )

            # Insert attempt
            conn.execute(
                attempts.insert().values(
                    id=attempt_id,
                    partition_id=part_id,
                    kind="import",
                    owner_id=attempt_id,
                    lease_generation=1,
                    request_id=attempt_id,
                    dagster_run_id=manifest.run_id,
                    status="success",
                    started_at=started_at,
                    completed_at=completed_at,
                )
            )

            # Insert commit
            conn.execute(
                commits.insert().values(
                    id=commit_id,
                    partition_id=part_id,
                    attempt_id=attempt_id,
                    parent_commit_id=None,
                    data_version=uuid4(),
                    record_count=record_count,
                    file_count=len(files_data),
                    verification={"migrated_from": "s3_manifest", "run_id": manifest.run_id},
                    committed_at=completed_at,
                )
            )

            # Insert commit files
            if files_data:
                conn.execute(
                    commit_files.insert(),
                    [
                        {
                            "commit_id": commit_id,
                            "file_number": f["file_number"],
                            "table_name": f["table_name"],
                            "bucket": f["bucket"],
                            "object_key": f["object_key"],
                            "row_count": f["row_count"],
                            "size_bytes": f["size_bytes"],
                            "sha256": f["sha256"],
                            "schema_version": 1,
                        }
                        for f in files_data
                    ],
                )

            # Update partition current commit
            conn.execute(
                partitions.update()
                .where(partitions.c.id == part_id)
                .values(current_commit_id=commit_id)
            )

            stats["imported"] += 1

    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate historical S3 Bronze manifests to PostgreSQL metadata."
    )
    parser.add_argument(
        "--resource",
        choices=["all", *SUPPORTED_RESOURCES],
        default="all",
        help="Target resource to import (default: all).",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Data source name (default: {DEFAULT_SOURCE}).",
    )
    parser.add_argument(
        "--bucket",
        default=settings.OBJECT_STORAGE_BUCKET,
        help=f"S3 bucket name (default: {settings.OBJECT_STORAGE_BUCKET}).",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="Target PostgreSQL connection URL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Inspect S3 manifests without writing to PostgreSQL.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    fs = create_s3_filesystem()
    engine = create_application_engine(args.db_url)

    target_resources = SUPPORTED_RESOURCES if args.resource == "all" else [args.resource]

    reports = []
    overall_ok = True

    for res in target_resources:
        try:
            report = import_resource_manifests(
                engine,
                fs,
                res,
                source=args.source,
                bucket=args.bucket,
                dry_run=args.dry_run,
            )
            reports.append(report)
        except Exception as exc:  # noqa: BLE001
            reports.append({"resource": res, "error": str(exc)})
            overall_ok = False

    print(json.dumps(reports, indent=2))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
