import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any, BinaryIO, cast

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings

CONTROL_SCHEMA_VERSION = 1
LOCK_TTL = timedelta(hours=12)


class ActiveLockError(RuntimeError):
    """Raised when another run currently owns a daily crawl lock."""


class IncompatibleCheckpointError(RuntimeError):
    """Raised when a checkpoint belongs to a different query definition."""


def calculate_query_fingerprint(query_definition: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        query_definition, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _prefix(identity: ResourceIdentity, source_date: date) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_control/"
        f"{identity.source}/{identity.resource}/"
        f"source_date={source_date.isoformat()}"
    )


def _write_json(fs: s3fs.S3FileSystem, key: str, value: Mapping[str, Any]) -> str:
    content = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with fs.open(key, "wb") as raw_file:
        file = cast(BinaryIO, raw_file)
        file.write(content)
    return f"s3://{key}"


def _read_json(fs: s3fs.S3FileSystem, key: str) -> dict[str, Any] | None:
    if not fs.exists(key):
        return None
    with fs.open(key, "rb") as raw_file:
        return cast(dict[str, Any], json.load(raw_file))


def read_daily_success(
    fs: s3fs.S3FileSystem, identity: ResourceIdentity, source_date: date
) -> dict[str, Any] | None:
    return _read_json(fs, f"{_prefix(identity, source_date)}/_SUCCESS.json")


def write_daily_success(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    metadata: Mapping[str, Any],
) -> str:
    return _write_json(fs, f"{_prefix(identity, source_date)}/_SUCCESS.json", metadata)


def read_page_checkpoint(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    page_number: int,
) -> dict[str, Any] | None:
    return _read_json(
        fs, f"{_prefix(identity, source_date)}/pages/page-{page_number:06d}.json"
    )


def write_page_checkpoint(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    page_number: int,
    metadata: Mapping[str, Any],
) -> str:
    return _write_json(
        fs,
        f"{_prefix(identity, source_date)}/pages/page-{page_number:06d}.json",
        metadata,
    )


def write_run_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
    metadata: Mapping[str, Any],
) -> str:
    return _write_json(
        fs,
        f"{_prefix(identity, source_date)}/runs/run_id={run_id}.json",
        metadata,
    )


def acquire_daily_lock(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
    *,
    now: datetime | None = None,
) -> str:
    current_time = now or datetime.now(UTC)
    key = f"{_prefix(identity, source_date)}/lock.json"
    lock = _read_json(fs, key)
    if lock is not None:
        expires_at = datetime.fromisoformat(lock["expires_at"])
        if expires_at > current_time and lock.get("run_id") != run_id:
            raise ActiveLockError(
                f"source_date={source_date} is locked by run_id={lock.get('run_id')}"
            )
    return _write_json(fs, key, {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "source": identity.source,
        "resource": identity.resource,
        "source_date": source_date.isoformat(),
        "run_id": run_id,
        "status": "running",
        "started_at": current_time.isoformat(),
        "expires_at": (current_time + LOCK_TTL).isoformat(),
    })


def refresh_daily_lock(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
) -> str:
    return acquire_daily_lock(fs, identity, source_date, run_id)


def release_daily_lock(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
) -> None:
    key = f"{_prefix(identity, source_date)}/lock.json"
    lock = _read_json(fs, key)
    if lock is not None and lock.get("run_id") == run_id:
        fs.rm(key)


def ensure_compatible(
    checkpoint: Mapping[str, Any], query_fingerprint: str
) -> None:
    existing = checkpoint.get("query_fingerprint")
    if existing != query_fingerprint:
        raise IncompatibleCheckpointError(
            f"Checkpoint fingerprint {existing!r} does not match current query"
        )
