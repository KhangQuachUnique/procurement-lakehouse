from datetime import date
from typing import Any

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.storage.control import list_run_manifests, read_run_manifest
from procurement.storage.errors import list_error_records


class RunRepository:
    def __init__(self, fs: s3fs.S3FileSystem) -> None:
        self._fs = fs

    def list(
        self,
        identity: ResourceIdentity,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        return [
            manifest.model_dump(mode="json")
            for manifest in list_run_manifests(
                self._fs,
                identity,
                start_date=start_date,
                end_date=end_date,
            )
        ]

    def get(self, identity: ResourceIdentity, run_id: str) -> dict[str, Any] | None:
        manifest = read_run_manifest(self._fs, identity, run_id)
        return None if manifest is None else manifest.model_dump(mode="json")


class ErrorRepository:
    def __init__(self, fs: s3fs.S3FileSystem) -> None:
        self._fs = fs

    def list(
        self,
        identity: ResourceIdentity,
        *,
        source_date: date | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return [
            record.model_dump(mode="json")
            for record in list_error_records(
                self._fs,
                identity,
                source_date=source_date,
                run_id=run_id,
            )
        ]
