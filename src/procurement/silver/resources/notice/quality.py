"""Declarative Data Quality rules for resource: notice."""

from procurement.silver.quality.rules import QualityRule

NOTICE_QUALITY_RULES: tuple[QualityRule, ...] = (
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
        max_rate=0.02,
    ),
    QualityRule(
        name="unknown_currency_warn",
        metric="unknown_currency_rows",
        severity="warn",
        max_rate=0.15,
    ),
)
