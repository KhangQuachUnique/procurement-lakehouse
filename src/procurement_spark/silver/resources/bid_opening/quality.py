"""Declarative Data Quality rules for resource: bid_opening."""

from __future__ import annotations

from procurement_spark.silver.quality.rules import QualityRule

BID_OPENING_QUALITY_RULES: tuple[QualityRule, ...] = (
    QualityRule(
        name="quarantine_rate_below_5pct",
        metric="quarantined_rows",
        severity="block",
        max_rate=0.05,
    ),
    QualityRule(
        name="warn_rows_monitor",
        metric="warn_rows",
        severity="warn",
        max_rate=0.25,
    ),
)
