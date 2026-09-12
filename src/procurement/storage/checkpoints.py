from collections.abc import Mapping
from datetime import date
from typing import Any

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.storage.io import read_json, write_json

CONTROL_SCHEMA_VERSION = 2


def _prefix(identity: ResourceIdentity, source_date: date) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_control/"
        f"{identity.source}/{identity.resource}/source_date={source_date.isoformat()}"
    )


def read_page_checkpoint(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    page_number: int,
) -> dict[str, Any] | None:
    return read_json(fs, f"{_prefix(identity, source_date)}/pages/page-{page_number:06d}.json")


def write_page_checkpoint(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    page_number: int,
    metadata: Mapping[str, Any],
) -> str:
    return write_json(
        fs,
        f"{_prefix(identity, source_date)}/pages/page-{page_number:06d}.json",
        metadata,
    )
