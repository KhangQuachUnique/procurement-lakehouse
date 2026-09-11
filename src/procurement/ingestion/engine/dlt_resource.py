from collections.abc import Iterator
from typing import Any

import dlt


def create_bronze_resource(records: Iterator[dict[str, Any]], *, name: str):
    """Wrap resource-specific records with shared Bronze DLT settings."""

    return dlt.resource(
        records,
        name=name,
        table_name=lambda record: record["_resource"],
        write_disposition="append",
        file_format="parquet",
        max_table_nesting=0,
    )
