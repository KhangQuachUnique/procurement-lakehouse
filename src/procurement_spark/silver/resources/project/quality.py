"""Declarative Data Quality rules for resource: project."""

from procurement_spark.silver.quality.rules import QualityRule

PROJECT_QUALITY_RULES: tuple[QualityRule, ...] = (
    QualityRule(
        name="quarantine_rate_below_5pct",
        metric="quarantined_rows",
        severity="block",
        max_rate=0.05,
    ),
    QualityRule(
        name="invalid_money_warn",
        metric="invalid_money_rows",
        severity="warn",
        max_rate=0.01,
    ),
    QualityRule(
        name="unknown_currency_warn",
        metric="unknown_currency_rows",
        severity="warn",
        max_rate=0.05,
    ),
    QualityRule(
        name="invalid_date_order_warn",
        metric="invalid_date_order_rows",
        severity="warn",
        max_rate=0.05,
    ),
)
