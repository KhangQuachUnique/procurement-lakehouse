"""3-tier Data Quality engine: Inline metrics, Post-load WAP gate, and Audit."""

from procurement.silver.quality.gate import GateCheckResult, run_post_load_gate
from procurement.silver.quality.issues import (
    SHARED_DQ_TABLE,
    SHARED_GOVERNANCE_DDLS,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement.silver.quality.rules import QualityRule, evaluate_inline_rules

__all__ = [
    "SHARED_DQ_TABLE",
    "SHARED_GOVERNANCE_DDLS",
    "SHARED_QUARANTINE_TABLE",
    "FieldIssueSpec",
    "GateCheckResult",
    "QualityRule",
    "evaluate_inline_rules",
    "extract_dq_issues",
    "run_post_load_gate",
]
