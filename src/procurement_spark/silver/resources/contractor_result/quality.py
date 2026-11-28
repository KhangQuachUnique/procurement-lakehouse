"""Declarative Data Quality rules for resource: contractor_result."""

from __future__ import annotations

from procurement_spark.silver.quality.rules import QualityRule

CONTRACTOR_RESULT_QUALITY_RULES: tuple[QualityRule, ...] = (
    QualityRule(
        name="quarantine_rate_below_5pct",
        metric="quarantined_rows",
        severity="block",
        max_rate=0.05,
    ),
    QualityRule(
        name="consortium_ambiguity_warn",
        metric="consortium_ambiguity_rows",
        severity="warn",
        max_rate=0.20,
    ),
    QualityRule(
        name="suspect_decision_date_warn",
        metric="suspect_decision_date_rows",
        severity="warn",
        max_rate=0.05,
    ),
)
