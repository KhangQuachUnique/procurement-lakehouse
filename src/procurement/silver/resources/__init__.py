"""Spark Silver resource registry."""

from procurement.silver.resources.registry import (
    SPARK_RESOURCES,
    SparkResourceSpec,
    get_spark_resource,
)

__all__ = ["SPARK_RESOURCES", "SparkResourceSpec", "get_spark_resource"]
