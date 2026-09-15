from datetime import date

import s3fs

from procurement.common.resources import ResourceIdentity
from procurement.models.errors import ErrorRecord
from procurement.storage.errors import list_error_records


class ErrorRepository:
    def __init__(self, fs: s3fs.S3FileSystem) -> None:
        self._fs = fs

    def list(
        self,
        identity: ResourceIdentity,
        *,
        source_date: date | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        error_type: str | None = None,
        limit: int = 200,
    ) -> list[ErrorRecord]:
        records = list_error_records(
            self._fs,
            identity,
            source_date=source_date,
            run_id=run_id,
        )
        if stage is not None:
            records = [item for item in records if item.stage == stage]
        if error_type is not None:
            records = [item for item in records if item.error_type == error_type]
        return records[:limit]
