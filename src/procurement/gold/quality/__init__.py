"""Pre-publish WAP quality gate for Spark Gold."""

from procurement.gold.quality.gate import GoldGateResult, run_gold_quality_gate

__all__ = ["GoldGateResult", "run_gold_quality_gate"]
