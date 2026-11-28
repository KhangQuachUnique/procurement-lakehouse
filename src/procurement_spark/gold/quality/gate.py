"""Pre-publish WAP quality gate for the 17 public Gold tables."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import SparkSession

from procurement_spark.gold.schema import MART_PRIMARY_KEYS


@dataclass
class GoldGateResult:
    passed: bool
    table_counts: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)
    award_amount_sum: Decimal = Decimal("0")
    supplier_amount_sum: Decimal = Decimal("0")


def run_gold_quality_gate(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    branch_name: str | None = None,
) -> GoldGateResult:
    """Audit all 17 Gold tables on a WAP branch (or main) before publishing.

    Checks:
      1. 0 NULL primary keys and 0 duplicate primary keys on every one of the 17 tables.
      2. Cross-fact VND reconciliation: fact_award_daily.award_amount_sum == fact_supplier_daily.award_amount_sum.
      3. dim_date guard: 0 dates < 2000-01-01 or > 2035-12-31.
    """
    result = GoldGateResult(passed=True)
    prev_branch = spark.conf.get("spark.wap.branch", None)
    try:
        if branch_name:
            spark.conf.set("spark.wap.branch", branch_name)

        for tbl, pk in MART_PRIMARY_KEYS.items():
            full_name = f"{catalog}.{namespace}.{tbl}"
            row = spark.sql(
                f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT {pk}) AS distinct_pks,
                    SUM(CASE WHEN {pk} IS NULL THEN 1 ELSE 0 END) AS null_pks
                FROM {full_name}
                """
            ).first()
            total = int(row["total_rows"])
            distinct_pks = int(row["distinct_pks"])
            null_pks = int(row["null_pks"] or 0)
            result.table_counts[tbl] = total

            if null_pks > 0:
                result.passed = False
                result.failures.append(f"{tbl}: {null_pks} NULL primary keys ({pk})")
            if distinct_pks != total:
                result.passed = False
                result.failures.append(
                    f"{tbl}: {total - distinct_pks} duplicate primary keys ({pk})"
                )

        # Cross-fact award amount reconciliation
        awd_sum = spark.sql(
            f"SELECT COALESCE(SUM(award_amount_sum), CAST(0 AS DECIMAL(38,6))) FROM {catalog}.{namespace}.fact_award_daily"
        ).first()[0]
        sup_sum = spark.sql(
            f"SELECT COALESCE(SUM(award_amount_sum), CAST(0 AS DECIMAL(38,6))) FROM {catalog}.{namespace}.fact_supplier_daily"
        ).first()[0]
        result.award_amount_sum = Decimal(str(awd_sum))
        result.supplier_amount_sum = Decimal(str(sup_sum))
        if result.award_amount_sum != result.supplier_amount_sum:
            result.passed = False
            result.failures.append(
                f"Cross-fact award sum mismatch: fact_award_daily={result.award_amount_sum} "
                f"!= fact_supplier_daily={result.supplier_amount_sum}"
            )

        # dim_date bounds guard
        bad_dates = int(
            spark.sql(
                f"SELECT COUNT(*) FROM {catalog}.{namespace}.dim_date "
                f"WHERE date_key < DATE '2000-01-01' OR date_key > DATE '2035-12-31'"
            ).first()[0]
        )
        if bad_dates > 0:
            result.passed = False
            result.failures.append(f"dim_date: {bad_dates} out-of-range dates (<2000 or >2035)")
    finally:
        if prev_branch is not None:
            spark.conf.set("spark.wap.branch", prev_branch)
        elif branch_name:
            spark.conf.unset("spark.wap.branch")

    return result
