from datetime import UTC, date, datetime, timedelta

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.storage.io import read_json, write_json

LOCK_TTL = timedelta(hours=12)


class ActiveLockError(RuntimeError):
    """Raised when another run currently owns a daily crawl lock."""


def _key(identity: ResourceIdentity, source_date: date) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_control/{identity.source}/{identity.resource}/"
        f"source_date={source_date.isoformat()}/lock.json"
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
    key = _key(identity, source_date)
    lock = read_json(fs, key)
    if lock is not None:
        expires_at = datetime.fromisoformat(str(lock["expires_at"]))
        if expires_at > current_time and lock.get("run_id") != run_id:
            raise ActiveLockError(
                f"source_date={source_date} is locked by run_id={lock.get('run_id')}"
            )
    return write_json(
        fs,
        key,
        {
            "schema_version": 1,
            "source": identity.source,
            "resource": identity.resource,
            "source_date": source_date.isoformat(),
            "run_id": run_id,
            "status": "running",
            "started_at": current_time.isoformat(),
            "expires_at": (current_time + LOCK_TTL).isoformat(),
        },
    )


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
    key = _key(identity, source_date)
    lock = read_json(fs, key)
    if lock is not None and lock.get("run_id") == run_id:
        fs.rm(key)
