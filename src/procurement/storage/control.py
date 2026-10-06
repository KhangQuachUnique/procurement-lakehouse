import json
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.models.control import DayManifest, DayStatus, PageManifest, RunManifest
from procurement.storage.io import read_json, write_json


def _check_legacy_write_allowed() -> None:
    if os.getenv("PREVENT_LEGACY_MANIFEST_WRITES") == "1":
        raise RuntimeError(
            "Legacy S3 manifest writing is disabled. Use IngestionService and PostgreSQL metadata."
        )
    warnings.warn(
        "Writing S3 JSON manifests is deprecated; migrate to PostgreSQL MetadataService.",
        DeprecationWarning,
        stacklevel=3,
    )


class DayCommitUncertainError(RuntimeError):
    """The caller must not rewrite the attempt after an unacknowledged commit."""


def commit_day_manifest(
    fs: s3fs.S3FileSystem, identity: ResourceIdentity, manifest: DayManifest
) -> str:
    if manifest.status is not DayStatus.SUCCESS:
        raise ValueError("Only a successful day can be committed")
    try:
        return write_day_manifest(fs, identity, manifest)
    except Exception as write_error:
        try:
            persisted = read_day_manifest(fs, identity, manifest.run_id, manifest.source_date)
        except Exception as read_error:
            raise DayCommitUncertainError(
                f"Cannot verify day commit for {manifest.run_id}/{manifest.source_date}"
            ) from read_error
        if persisted == manifest:
            return f"s3://{_day_prefix(identity, manifest.run_id, manifest.source_date)}/day.json"
        # Even an absent object can reflect a request still completing remotely.
        raise DayCommitUncertainError(
            f"Day commit was not acknowledged for {manifest.run_id}/{manifest.source_date}"
        ) from write_error


def _resource_prefix(identity: ResourceIdentity) -> str:
    return f"{settings.OBJECT_STORAGE_BUCKET}/_control/{identity.source}/{identity.resource}"


def _run_prefix(identity: ResourceIdentity, run_id: str) -> str:
    return f"{_resource_prefix(identity)}/run_id={run_id}"


def _day_prefix(identity: ResourceIdentity, run_id: str, source_date: date) -> str:
    return f"{_run_prefix(identity, run_id)}/source_date={source_date.isoformat()}"


def write_run_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    manifest: RunManifest,
) -> str:
    _check_legacy_write_allowed()
    return write_json(
        fs,
        f"{_run_prefix(identity, manifest.run_id)}/run.json",
        manifest.model_dump(mode="json"),
    )


def read_run_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    run_id: str,
) -> RunManifest | None:
    data = read_json(fs, f"{_run_prefix(identity, run_id)}/run.json")
    return None if data is None else RunManifest.model_validate(data)


def list_run_manifests(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    workers: int = 1,
) -> list[RunManifest]:
    manifests: list[RunManifest] = []
    # A wildcard glob descends into every run directory on S3, issuing a LIST
    # per historical run. List only the resource's immediate children instead.
    prefix = _resource_prefix(identity)
    try:
        entries = fs.ls(prefix, detail=False)
    except FileNotFoundError:
        entries = []
    keys = sorted(
        f"{str(entry).rstrip('/')}/run.json"
        for entry in entries
        if str(entry).rstrip("/").rsplit("/", 1)[-1].startswith("run_id=")
    )

    def read(key):
        try:
            # One GET, without the exists + open metadata requests per header.
            return json.loads(fs.cat_file(key))
        except FileNotFoundError:
            return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        data_rows = list(pool.map(read, keys))
    for data in data_rows:
        if data is None:
            continue
        manifest = RunManifest.model_validate(data)
        if start_date is not None and manifest.end_date < start_date:
            continue
        if end_date is not None and manifest.start_date > end_date:
            continue
        manifests.append(manifest)
    return sorted(manifests, key=lambda item: item.started_at, reverse=True)


def write_day_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    manifest: DayManifest,
) -> str:
    _check_legacy_write_allowed()
    return write_json(
        fs,
        f"{_day_prefix(identity, manifest.run_id, manifest.source_date)}/day.json",
        manifest.model_dump(mode="json"),
    )


def read_day_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    run_id: str,
    source_date: date,
) -> DayManifest | None:
    data = read_json(fs, f"{_day_prefix(identity, run_id, source_date)}/day.json")
    return None if data is None else DayManifest.model_validate(data)


def list_day_manifests(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    run_id: str | None = None,
    source_date: date | None = None,
) -> list[DayManifest]:
    run_part = f"run_id={run_id}" if run_id else "run_id=*"
    date_part = f"source_date={source_date.isoformat()}" if source_date else "source_date=*"
    pattern = f"{_resource_prefix(identity)}/{run_part}/{date_part}/day.json"
    manifests: list[DayManifest] = []
    for key in fs.glob(pattern):
        data = read_json(fs, key)
        if data is not None:
            manifests.append(DayManifest.model_validate(data))
    return sorted(manifests, key=lambda item: item.started_at, reverse=True)


def write_page_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    manifest: PageManifest,
) -> str:
    _check_legacy_write_allowed()
    return write_json(
        fs,
        (
            f"{_day_prefix(identity, manifest.run_id, manifest.source_date)}/pages/"
            f"page-{manifest.page_number:06d}.json"
        ),
        manifest.model_dump(mode="json"),
    )


def read_page_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    run_id: str,
    source_date: date,
    page_number: int,
) -> PageManifest | None:
    data = read_json(
        fs,
        (f"{_day_prefix(identity, run_id, source_date)}/pages/page-{page_number:06d}.json"),
    )
    return None if data is None else PageManifest.model_validate(data)


def list_page_manifests(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    run_id: str,
    source_date: date,
) -> list[PageManifest]:
    pattern = f"{_day_prefix(identity, run_id, source_date)}/pages/page-*.json"
    manifests: list[PageManifest] = []
    for key in fs.glob(pattern):
        data = read_json(fs, key)
        if data is not None:
            manifests.append(PageManifest.model_validate(data))
    return sorted(manifests, key=lambda item: item.page_number)
