"""Native Spark Parquet reader for pinned Bronze S3A file lists."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BRONZE_CORE_COLUMNS = (
    "payload",
    "source_id",
    "source_version",
    "source_date",
    "run_id",
    "ingested_at",
    "content_hash",
)

EMPTY_BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("payload", T.StringType(), True),
        T.StructField("source_id", T.StringType(), True),
        T.StructField("source_version", T.StringType(), True),
        T.StructField("source_date", T.DateType(), True),
        T.StructField("run_id", T.StringType(), True),
        T.StructField("ingested_at", T.TimestampType(), True),
        T.StructField("content_hash", T.StringType(), True),
    ]
)


def read_bronze_parquet(spark: SparkSession, s3a_uris: list[str]) -> DataFrame:
    """Read exact pinned Bronze Parquet files directly via S3A with column pruning."""
    if not s3a_uris:
        return spark.createDataFrame([], schema=EMPTY_BRONZE_SCHEMA)

    raw_df = spark.read.parquet(*s3a_uris)
    available = set(raw_df.columns)
    select_exprs = [
        F.col(c) if c in available else F.lit(None).cast("string").alias(c)
        for c in BRONZE_CORE_COLUMNS
    ]
    return raw_df.select(*select_exprs).withColumn(
        "source_date", F.to_date(F.col("source_date").cast("string"))
    )


def read_bronze_tables_by_name(
    spark: SparkSession,
    uris_by_table: dict[str, list[str]],
    expected_tables: tuple[str, ...],
) -> dict[str, DataFrame]:
    """Read each Bronze table's pinned S3A Parquet files into a keyed dict of DataFrames."""
    return {
        tbl: read_bronze_parquet(spark, uris_by_table.get(tbl, []))
        for tbl in expected_tables
    }
