from datetime import date

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.models.control import DayManifest, PageManifest, RunManifest, RunStatus
from procurement.storage.control import (
    list_day_manifests,
    list_page_manifests,
    list_run_manifests,
    read_day_manifest,
    read_run_manifest,
)
from procurement.storage.execution import read_execution


class ControlRepository:
    def __init__(self, fs: s3fs.S3FileSystem) -> None:
        self._fs = fs

    def list_runs(
        self,
        identity: ResourceIdentity,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = 100,
        status: RunStatus | None = None,
    ) -> list[RunManifest]:
        manifests = list_run_manifests(
            self._fs,
            identity,
            start_date=start_date,
            end_date=end_date,
        )
        if status is not None:
            manifests = [item for item in manifests if item.status is status]
        manifests.sort(key=lambda item: (item.started_at, item.run_id), reverse=True)
        return manifests[:limit]

    def get_execution(self, identity, run_id):
        return read_execution(self._fs, identity, run_id)

    def get_run(self, identity: ResourceIdentity, run_id: str) -> RunManifest | None:
        return read_run_manifest(self._fs, identity, run_id)

    def list_attempts(
        self,
        identity: ResourceIdentity,
        *,
        run_id: str | None = None,
        source_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = 1000,
    ) -> list[DayManifest]:
        attempts = list_day_manifests(
            self._fs,
            identity,
            run_id=run_id,
            source_date=source_date,
        )
        if start_date is not None:
            attempts = [item for item in attempts if item.source_date >= start_date]
        if end_date is not None:
            attempts = [item for item in attempts if item.source_date <= end_date]
        return attempts[:limit]

    def get_attempt(
        self,
        identity: ResourceIdentity,
        *,
        run_id: str,
        source_date: date,
    ) -> DayManifest | None:
        return read_day_manifest(self._fs, identity, run_id, source_date)

    def list_pages(
        self,
        identity: ResourceIdentity,
        *,
        run_id: str,
        source_date: date,
    ) -> list[PageManifest]:
        return list_page_manifests(
            self._fs,
            identity,
            run_id=run_id,
            source_date=source_date,
        )
