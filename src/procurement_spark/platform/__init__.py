"""Shared Spark 4.0 + Apache Iceberg platform infrastructure for Silver and Gold."""

from procurement_spark.platform.config import SparkSilverSettings, detect_lan_ip
from procurement_spark.platform.iceberg_writer import (
    abort_wap_branch,
    ensure_namespace_and_tables,
    merge_into_table,
    publish_wap_branch,
    stage_to_wap_branch,
)
from procurement_spark.platform.spark_session import build_spark_session

__all__ = [
    "SparkSilverSettings",
    "abort_wap_branch",
    "build_spark_session",
    "detect_lan_ip",
    "ensure_namespace_and_tables",
    "publish_wap_branch",
    "stage_to_wap_branch",
]
