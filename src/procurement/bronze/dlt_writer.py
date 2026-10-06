"""DLT pipeline adapter for Bronze ingestion."""

from collections.abc import Callable, Iterable, Mapping
from datetime import date
from typing import Any

import dlt
from dlt.destinations import filesystem

from procurement.bronze.models import BronzeRecord
from procurement.bronze.paths import bronze_layout_template
from procurement.bronze.writer import BronzeWriteError, serialize_record
from procurement.common.cancellation import INTERRUPTIONS


def create_bronze_destination(
    *,
    source_partition_date: date,
    run_id: str,
    bucket: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    endpoint_url: str | None = None,
    dataset: str = "muasamcong",
    filesystem_factory: Callable[..., Any] = filesystem,
):
    from procurement.common.settings import settings

    b = bucket or settings.OBJECT_STORAGE_BUCKET
    ak = access_key or settings.OBJECT_STORAGE_ACCESS_KEY
    sk = secret_key or settings.OBJECT_STORAGE_SECRET_KEY
    ep = endpoint_url if endpoint_url is not None else settings.OBJECT_STORAGE_ENDPOINT

    credentials = {
        "aws_access_key_id": ak,
        "aws_secret_access_key": sk,
        "region_name": "us-east-1",
    }
    if ep:
        credentials["endpoint_url"] = ep

    return filesystem_factory(
        bucket_url=f"s3://{b}/bronze/{dataset}",
        credentials=credentials,
        layout=bronze_layout_template(),
        extra_placeholders={
            "source_date": source_partition_date.isoformat(),
            "run_id": run_id,
        },
    )


def create_bronze_resource(records: Iterable[BronzeRecord], *, name: str):
    """Create one append-only Bronze table resource for a page/chunk."""

    def serialized():
        for record in records:
            yield serialize_record(record)

    return dlt.resource(
        serialized(),
        name=name,
        table_name=name,
        write_disposition="append",
        file_format="parquet",
        max_table_nesting=0,
        columns={
            "payload": {"data_type": "text", "nullable": False},
            "source_version": {
                "data_type": "text",
                "nullable": True,
            },
        },
    )


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
