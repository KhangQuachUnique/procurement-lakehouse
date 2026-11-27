"""3-tier Data Quality engine: Inline metrics, Post-load WAP gate, and Audit."""

from procurement_spark.quality.gate import GateCheckResult, run_post_load_gate
from procurement_spark.quality.issues import (
    SHARED_DQ_TABLE,
    SHARED_GOVERNANCE_DDLS,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement_spark.quality.rules import QualityRule, evaluate_inline_rules

__all__ = [
    "FieldIssueSpec",
    "GateCheckResult",
    "QualityRule",
    "SHARED_DQ_TABLE",
    "SHARED_GOVERNANCE_DDLS",
    "SHARED_QUARANTINE_TABLE",
    "evaluate_inline_rules",
    "extract_dq_issues",
    "run_post_load_gate",
]
