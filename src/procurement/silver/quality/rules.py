"""Declarative Data Quality rules evaluated inline and at post-load gate."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class QualityRule:
    """Simple, declarative threshold rule for inline or post-load metrics."""

    name: str
    metric: str
    severity: Literal["warn", "block"] = "warn"
    max_count: int | None = None
    max_rate: float | None = None


@dataclass(frozen=True)
class RuleEvaluation:
    rule: QualityRule
    observed_count: int
    observed_rate: float
    passed: bool


def evaluate_inline_rules(
    rules: tuple[QualityRule, ...],
    metrics: dict[str, Any],
) -> tuple[bool, list[RuleEvaluation]]:
    """Evaluate declarative rules against batch-level inline metrics."""
    total = int(metrics.get("raw_input_rows", 0))
    evals: list[RuleEvaluation] = []
    all_blocking_passed = True

    for rule in rules:
        count = int(metrics.get(rule.metric, 0))
        rate = (count / total) if total > 0 else 0.0
        passed = True
        if rule.max_count is not None and count > rule.max_count:
            passed = False
        if rule.max_rate is not None and rate > rule.max_rate:
            passed = False
        if not passed and rule.severity == "block":
            all_blocking_passed = False
        evals.append(
            RuleEvaluation(
                rule=rule,
                observed_count=count,
                observed_rate=round(rate, 6),
                passed=passed,
            )
        )
    return all_blocking_passed, evals
