from collections.abc import Mapping
from datetime import date
from typing import Any

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.storage.io import write_json


def write_retry_manifest(
    fs: s3fs.S3FileSystem,
    identity: ResourceIdentity,
    source_date: date,
    retry_run_id: str,
    metadata: Mapping[str, Any],
) -> str:
    key = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_retry_runs/{identity.source}/{identity.resource}/"
        f"source_date={source_date.isoformat()}/retry_run_id={retry_run_id}.json"
    )
    return write_json(fs, key, metadata)
