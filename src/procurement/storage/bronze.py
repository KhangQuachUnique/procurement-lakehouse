from collections.abc import Iterator
from datetime import date
from typing import Any

import dlt
from dlt.destinations import filesystem

from procurement.common.settings import settings


def create_bronze_destination(*, source_partition_date: date):
    source_year = f"{source_partition_date.year:04d}"
    source_month = f"{source_partition_date.month:02d}"
    source_day = f"{source_partition_date.day:02d}"
    return filesystem(
        bucket_url=f"s3://{settings.OBJECT_STORAGE_BUCKET}/bronze",
        credentials={
            "aws_access_key_id": settings.OBJECT_STORAGE_ACCESS_KEY,
            "aws_secret_access_key": settings.OBJECT_STORAGE_SECRET_KEY,
            "endpoint_url": settings.OBJECT_STORAGE_ENDPOINT,
            "region_name": "us-east-1",
        },
        layout=(
            "{table_name}/source_year={source_year}/source_month={source_month}/"
            "source_day={source_day}/{load_id}.{file_id}.{ext}"
        ),
        extra_placeholders={
            "source_year": source_year,
            "source_month": source_month,
            "source_day": source_day,
        },
    )


def create_bronze_resource(records: Iterator[dict[str, Any]], *, name: str):
    """Bronze is append-only/at-least-once; Silver owns canonical deduplication."""
    return dlt.resource(
        records,
        name=name,
        table_name=lambda record: record["_resource"],
        write_disposition="append",
        file_format="parquet",
        max_table_nesting=0,
    )
