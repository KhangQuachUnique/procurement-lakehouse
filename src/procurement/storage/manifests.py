from collections.abc import Mapping
from datetime import date
from typing import Any

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.storage.io import read_json, write_json


def _prefix(identity: ResourceIdentity, source_date: date) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_control/"
        f"{identity.source}/{identity.resource}/source_date={source_date.isoformat()}"
    )


def read_daily_success(
    fs: s3fs.S3FileSystem, identity: ResourceIdentity, source_date: date
) -> dict[str, Any] | None:
    return read_json(fs, f"{_prefix(identity, source_date)}/_SUCCESS.json")


def write_daily_success(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    metadata: Mapping[str, Any],
) -> str:
    return write_json(fs, f"{_prefix(identity, source_date)}/_SUCCESS.json", metadata)


def read_run_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
) -> dict[str, Any] | None:
    return read_json(fs, f"{_prefix(identity, source_date)}/runs/run_id={run_id}.json")


def write_run_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    run_id: str,
    metadata: Mapping[str, Any],
) -> str:
    return write_json(
        fs,
        f"{_prefix(identity, source_date)}/runs/run_id={run_id}.json",
        metadata,
    )


def list_run_manifests(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    pattern = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_control/{identity.source}/{identity.resource}/"
        "source_date=*/runs/run_id=*.json"
    )
    manifests: list[dict[str, Any]] = []
    for key in fs.glob(pattern):
        manifest = read_json(fs, key)
        if manifest is None:
            continue
        source_date = date.fromisoformat(str(manifest["source_date"]))
        if start_date is not None and source_date < start_date:
            continue
        if end_date is not None and source_date > end_date:
            continue
        manifests.append(manifest)
    return sorted(
        manifests,
        key=lambda item: (str(item.get("source_date", "")), str(item.get("started_at", ""))),
        reverse=True,
    )
