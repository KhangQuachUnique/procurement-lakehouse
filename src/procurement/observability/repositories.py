from datetime import date
from typing import Any

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.storage.error_resolutions import list_error_resolutions
from procurement.storage.errors import list_error_records
from procurement.storage.manifests import list_run_manifests, read_run_manifest


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
        return list_run_manifests(
            self._fs, identity, start_date=start_date, end_date=end_date
        )

    def get(
        self,
        identity: ResourceIdentity,
        source_date: date,
        run_id: str,
    ) -> dict[str, Any] | None:
        return read_run_manifest(self._fs, identity, source_date, run_id)


class ErrorRepository:
    def __init__(self, fs: s3fs.S3FileSystem) -> None:
        self._fs = fs

    def list(
        self,
        identity: ResourceIdentity,
        *,
        source_date: date | None = None,
        run_id: str | None = None,
        retryable: bool | None = None,
    ) -> list[dict[str, Any]]:
        errors = list_error_records(
            self._fs,
            identity,
            source_date=source_date,
            run_id=run_id,
            retryable=retryable,
        )
        resolutions = list_error_resolutions(self._fs, identity, source_date=source_date)
        views: list[dict[str, Any]] = []
        for error in errors:
            view = dict(error)
            resolution = resolutions.get(str(error["error_id"]))
            if resolution is not None:
                view["resolution"] = resolution
                view["resolution_status"] = resolution.get("status")
            else:
                view["resolution"] = None
                view["resolution_status"] = (
                    "pending" if bool(error.get("retryable")) else "not_retryable"
                )
            views.append(view)
        return views
