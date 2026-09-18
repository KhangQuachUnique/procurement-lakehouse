from collections.abc import Callable, Iterable, Mapping
from datetime import date
from typing import Any, Protocol

import dlt
from dlt.destinations import filesystem

from procurement.common.cancellation import INTERRUPTIONS
from procurement.common.settings import settings
from procurement.models.bronze import BronzeRecord


class BronzeWriter(Protocol):
    def write_page(self, tables: Mapping[str, list[BronzeRecord]]) -> int: ...


class BronzeWriteError(RuntimeError):
    def __init__(self, cause: Exception, persisted_records: int) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.persisted_records = persisted_records


class DltBronzeWriter:
    """One attempt owns one pipeline. Counts include only confirmed table loads."""

    def __init__(self, pipeline_factory: Callable[[], Any]) -> None:
        self._pipeline_factory = pipeline_factory
        self._pipeline: Any = None

    def write_page(self, tables: Mapping[str, list[BronzeRecord]]) -> int:
        persisted = 0
        try:
            for table, records in tables.items():
                if not records:
                    continue
                if self._pipeline is None:
                    self._pipeline = self._pipeline_factory()
                info = self._pipeline.run(create_bronze_resource(records, name=table))
                if info is None:
                    raise RuntimeError("DLT returned no load receipt for a non-empty resource")
                info.raise_on_failed_jobs()
                persisted += len(records)
        except INTERRUPTIONS:
            raise
        except Exception as exc:
            raise BronzeWriteError(exc, persisted) from exc
        return persisted


def create_bronze_destination(*, source_partition_date: date, run_id: str):
    return filesystem(
        bucket_url=f"s3://{settings.OBJECT_STORAGE_BUCKET}/bronze",
        credentials={
            "aws_access_key_id": settings.OBJECT_STORAGE_ACCESS_KEY,
            "aws_secret_access_key": settings.OBJECT_STORAGE_SECRET_KEY,
            "endpoint_url": settings.OBJECT_STORAGE_ENDPOINT,
            "region_name": "us-east-1",
        },
        layout=("{table_name}/source_date={source_date}/run_id={run_id}/{load_id}.{file_id}.{ext}"),
        extra_placeholders={
            "source_date": source_partition_date.isoformat(),
            "run_id": run_id,
        },
    )


def create_bronze_resource(records: Iterable[BronzeRecord], *, name: str):
    """Create one append-only Bronze table resource for a page/chunk."""

    serialized = (record.model_dump(mode="json") for record in records)
    return dlt.resource(
        serialized,
        name=name,
        table_name=name,
        write_disposition="append",
        file_format="parquet",
        max_table_nesting=0,
        columns={
            "source_version": {
                "data_type": "text",
                "nullable": True,
            },
        },
    )
