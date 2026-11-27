"""Physical I/O adapters for Bronze S3A Parquet reading and Iceberg WAP writing."""

from procurement_spark.io.bronze_reader import read_bronze_parquet, read_bronze_tables_by_name
from procurement_spark.io.iceberg_writer import (
    abort_wap_branch,
    ensure_namespace_and_tables,
    publish_wap_branch,
    stage_to_wap_branch,
)

__all__ = [
    "abort_wap_branch",
    "ensure_namespace_and_tables",
    "publish_wap_branch",
    "read_bronze_parquet",
    "read_bronze_tables_by_name",
    "stage_to_wap_branch",
]
