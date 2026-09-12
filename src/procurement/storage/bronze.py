from collections.abc import Iterable
from datetime import date

import dlt
from dlt.destinations import filesystem

from procurement.common.settings import settings
from procurement.models.bronze import BronzeRecord


def create_bronze_destination(*, source_partition_date: date, run_id: str):
    return filesystem(
        bucket_url=f"s3://{settings.OBJECT_STORAGE_BUCKET}/bronze",
        credentials={
            "aws_access_key_id": settings.OBJECT_STORAGE_ACCESS_KEY,
            "aws_secret_access_key": settings.OBJECT_STORAGE_SECRET_KEY,
            "endpoint_url": settings.OBJECT_STORAGE_ENDPOINT,
            "region_name": "us-east-1",
        },
        layout=(
            "{table_name}/source_date={source_date}/run_id={run_id}/"
            "{load_id}.{file_id}.{ext}"
        ),
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
    )
