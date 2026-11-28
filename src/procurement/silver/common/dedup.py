"""Deterministic Spark Window deduplication helpers."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def dedup_latest(
    df: DataFrame,
    partition_cols: list[str],
    order_cols: list[Column],
) -> DataFrame:
    """Keep exactly one deterministic winning row per partition key."""
    w = Window.partitionBy(*partition_cols).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
