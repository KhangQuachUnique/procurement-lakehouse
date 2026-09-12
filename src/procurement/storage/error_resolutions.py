from collections.abc import Mapping
from datetime import date
from enum import StrEnum
from typing import Any

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.storage.io import read_json, write_json


class ResolutionStatus(StrEnum):
    PENDING = "pending"
    RETRYING = "retrying"
    RECOVERED = "recovered"
    DEAD_LETTER = "dead_letter"


def _key(identity: ResourceIdentity, source_date: date, error_id: str) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_error_state/{identity.source}/{identity.resource}/"
        f"source_date={source_date.isoformat()}/error_id={error_id}.json"
    )


def read_error_resolution(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    error_id: str,
) -> dict[str, Any] | None:
    return read_json(fs, _key(identity, source_date, error_id))


def write_error_resolution(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    error_id: str,
    state: Mapping[str, Any],
) -> str:
    return write_json(fs, _key(identity, source_date, error_id), state)


def list_error_resolutions(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    *,
    source_date: date | None = None,
) -> dict[str, dict[str, Any]]:
    date_part = f"source_date={source_date.isoformat()}" if source_date else "source_date=*"
    pattern = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_error_state/{identity.source}/{identity.resource}/"
        f"{date_part}/error_id=*.json"
    )
    result: dict[str, dict[str, Any]] = {}
    for key in fs.glob(pattern):
        state = read_json(fs, key)
        if state is not None and state.get("error_id"):
            result[str(state["error_id"])] = state
    return result
