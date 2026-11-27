"""Shared Silver Data Quality & Quarantine tables and pure PySpark issue extraction helpers."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from procurement_spark.common.hashing import sha256_key

SHARED_DQ_TABLE = "silver_dq_issues"
SHARED_QUARANTINE_TABLE = "silver_quarantine"

SHARED_GOVERNANCE_DDLS: dict[str, str] = {
    SHARED_DQ_TABLE: """
        issue_id STRING NOT NULL,
        resource STRING NOT NULL,
        table_name STRING NOT NULL,
        revision_id STRING NOT NULL,
        source_date DATE NOT NULL,
        field_name STRING NOT NULL,
        issue_code STRING NOT NULL,
        severity STRING NOT NULL,
        raw_value STRING
    """,
    SHARED_QUARANTINE_TABLE: """
        quarantine_id STRING NOT NULL,
        resource STRING NOT NULL,
        bronze_table STRING NOT NULL,
        source_date DATE NOT NULL,
        source_id STRING,
        run_id STRING,
        content_hash STRING,
        reason STRING NOT NULL,
        raw_payload STRING
    """,
}


@dataclass(frozen=True)
class FieldIssueSpec:
    """One row-level field quality check producing an entry in silver_dq_issues when condition is True."""

    condition: Column
    field_name: str
    issue_code: str
    raw_value: Column
    severity: str = "warn"


def extract_dq_issues(
    df: DataFrame,
    *,
    resource: str,
    table_name: str,
    specs: list[FieldIssueSpec],
) -> tuple[DataFrame, DataFrame]:
    """Attach lightweight `dq_status` ('VALID'|'WARN') to df and emit normalized `silver_dq_issues` DF.

    Only rows with at least one triggered check appear in `silver_dq_issues`.
    """
    if not specs:
        empty_issues = df.sparkSession.createDataFrame(
            [],
            schema=(
                "issue_id STRING, resource STRING, table_name STRING, revision_id STRING, "
                "source_date DATE, field_name STRING, issue_code STRING, severity STRING, raw_value STRING"
            ),
        )
        return df.withColumn("dq_status", F.lit("VALID")), empty_issues

    issue_structs = [
        F.when(
            spec.condition,
            F.struct(
                F.lit(spec.field_name).alias("field_name"),
                F.lit(spec.issue_code).alias("issue_code"),
                F.lit(spec.severity).alias("severity"),
                spec.raw_value.cast("string").alias("raw_value"),
            ),
        )
        for spec in specs
    ]

    with_arr = df.withColumn("_dq_arr", F.array_compact(F.array(*issue_structs)))
    annotated_df = with_arr.withColumn(
        "dq_status",
        F.when(F.size(F.col("_dq_arr")) > 0, F.lit("WARN")).otherwise(F.lit("VALID")),
    ).drop("_dq_arr")

    issues_df = (
        with_arr.filter(F.size(F.col("_dq_arr")) > 0)
        .select(
            "revision_id",
            "source_date",
            F.explode(F.col("_dq_arr")).alias("_iss"),
        )
        .select(
            sha256_key(
                F.lit(resource),
                F.lit(table_name),
                F.col("revision_id"),
                F.col("_iss.field_name"),
                F.col("_iss.issue_code"),
            ).alias("issue_id"),
            F.lit(resource).alias("resource"),
            F.lit(table_name).alias("table_name"),
            F.col("revision_id"),
            F.col("source_date"),
            F.col("_iss.field_name").alias("field_name"),
            F.col("_iss.issue_code").alias("issue_code"),
            F.col("_iss.severity").alias("severity"),
            F.col("_iss.raw_value").alias("raw_value"),
        )
    )
    return annotated_df, issues_df
