from datetime import date

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.models.control import DayManifest, PageManifest, RunManifest
from procurement.storage.io import read_json, write_json


def _resource_prefix(identity: ResourceIdentity) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_control/"
        f"{identity.source}/{identity.resource}"
    )


def _run_prefix(identity: ResourceIdentity, run_id: str) -> str:
    return f"{_resource_prefix(identity)}/run_id={run_id}"


def _day_prefix(identity: ResourceIdentity, run_id: str, source_date: date) -> str:
    return f"{_run_prefix(identity, run_id)}/source_date={source_date.isoformat()}"


def write_run_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    manifest: RunManifest,
) -> str:
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
) -> list[RunManifest]:
    pattern = f"{_resource_prefix(identity)}/run_id=*/run.json"
    manifests: list[RunManifest] = []
    for key in fs.glob(pattern):
        data = read_json(fs, key)
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


def write_page_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    manifest: PageManifest,
) -> str:
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
        (
            f"{_day_prefix(identity, run_id, source_date)}/pages/"
            f"page-{page_number:06d}.json"
        ),
    )
    return None if data is None else PageManifest.model_validate(data)
