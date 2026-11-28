"""Pure Spark SQL expression builders for Gold monetary normalization and award recovery.

Inlining as SQL expressions (rather than session-scoped ``CREATE TEMPORARY FUNCTION``)
avoids Iceberg 1.10 catalog ``UnsupportedOperationException`` while giving Spark Catalyst
the exact same zero-overhead JVM whole-stage codegen.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def norm_bid_price_sql(raw_expr: str, ref_expr: str) -> str:
    """Return a Spark SQL expression normalizing a raw bid price against a baseline."""
    return f"""(
        CASE
            WHEN ({raw_expr}) IS NULL OR ({raw_expr}) <= 0 THEN CAST(NULL AS DECIMAL(38,6))
            WHEN ({ref_expr}) IS NOT NULL AND ({raw_expr}) <= ({ref_expr}) * 2 THEN CAST(({raw_expr}) AS DECIMAL(38,6))
            WHEN ({ref_expr}) IS NOT NULL AND ({raw_expr}) > ({ref_expr}) * 2 THEN (
                CASE
                    WHEN FLOOR(({raw_expr}) / 1000000.0) BETWEEN ({ref_expr}) * 0.2 AND ({ref_expr}) * 1.5
                    THEN CAST(FLOOR(({raw_expr}) / 1000000.0) AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 1000.0) BETWEEN ({ref_expr}) * 0.2 AND ({ref_expr}) * 1.5
                    THEN CAST(FLOOR(({raw_expr}) / 1000.0) AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 100.0) BETWEEN ({ref_expr}) * 0.2 AND ({ref_expr}) * 1.5
                    THEN CAST(FLOOR(({raw_expr}) / 100.0) AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 10.0) BETWEEN ({ref_expr}) * 0.2 AND ({ref_expr}) * 1.5
                    THEN CAST(FLOOR(({raw_expr}) / 10.0) AS DECIMAL(38,6))
                    ELSE CAST(NULL AS DECIMAL(38,6))
                END
            )
            WHEN ({ref_expr}) IS NULL
                 AND ({raw_expr}) > 10000000000000
                 AND FLOOR(({raw_expr}) / 1000.0) <= 500000000000
            THEN CAST(FLOOR(({raw_expr}) / 1000.0) AS DECIMAL(38,6))
            WHEN ({raw_expr}) <= 50000000000000 THEN CAST(({raw_expr}) AS DECIMAL(38,6))
            ELSE CAST(NULL AS DECIMAL(38,6))
        END
    )"""


def recover_award_amount_sql(raw_expr: str, ref_expr: str) -> str:
    """Return a Spark SQL expression recovering scaled/concatenated award amount typos."""
    return f"""(
        CASE
            WHEN ({raw_expr}) IS NULL OR ({raw_expr}) <= 0 THEN CAST(({raw_expr}) AS DECIMAL(38,6))
            WHEN ({ref_expr}) IS NOT NULL AND ({raw_expr}) <= ({ref_expr}) * 2 THEN CAST(({raw_expr}) AS DECIMAL(38,6))
            WHEN ({ref_expr}) IS NOT NULL AND ({raw_expr}) > ({ref_expr}) * 2 THEN (
                CASE
                    WHEN FLOOR(({raw_expr}) / 1000000.0) = ({ref_expr})
                         AND ((CAST(({raw_expr}) AS BIGINT) % 1000000) * 1000) BETWEEN ({ref_expr}) * 0.3 AND ({ref_expr}) * 1.0
                    THEN CAST((CAST(({raw_expr}) AS BIGINT) % 1000000) * 1000 AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 1000000.0) BETWEEN ({ref_expr}) * 0.3 AND ({ref_expr}) * 1.2
                    THEN CAST(FLOOR(({raw_expr}) / 1000000.0) AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 1000.0) BETWEEN ({ref_expr}) * 0.3 AND ({ref_expr}) * 1.2
                    THEN CAST(FLOOR(({raw_expr}) / 1000.0) AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 100.0) BETWEEN ({ref_expr}) * 0.3 AND ({ref_expr}) * 1.2
                    THEN CAST(FLOOR(({raw_expr}) / 100.0) AS DECIMAL(38,6))
                    WHEN FLOOR(({raw_expr}) / 10.0) BETWEEN ({ref_expr}) * 0.3 AND ({ref_expr}) * 1.2
                    THEN CAST(FLOOR(({raw_expr}) / 10.0) AS DECIMAL(38,6))
                    WHEN CAST(({ref_expr}) AS BIGINT) >= 1000000
                         AND STARTSWITH(
                             CAST(CAST(({raw_expr}) AS BIGINT) AS STRING),
                             CAST(CAST(({ref_expr}) AS BIGINT) AS STRING)
                         )
                    THEN CAST(({ref_expr}) AS DECIMAL(38,6))
                    ELSE CAST(({raw_expr}) AS DECIMAL(38,6))
                END
            )
            WHEN ({ref_expr}) IS NULL
                 AND ({raw_expr}) > 10000000000000
                 AND (CAST(FLOOR(({raw_expr}) / 1000.0) AS BIGINT) % 1000) = 0
                 AND FLOOR(({raw_expr}) / 1000.0) <= 500000000000
            THEN CAST(FLOOR(({raw_expr}) / 1000.0) AS DECIMAL(38,6))
            ELSE CAST(({raw_expr}) AS DECIMAL(38,6))
        END
    )"""


def register_gold_functions(spark: SparkSession) -> None:
    """No-op hook kept for API symmetry; SQL macros are inlined directly into queries."""
    _ = spark
