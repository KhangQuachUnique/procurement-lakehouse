"""Spark Silver resource: contractor_result."""

from procurement.silver.resources.contractor_result.quality import CONTRACTOR_RESULT_QUALITY_RULES
from procurement.silver.resources.contractor_result.schema import CONTRACTOR_RESULT_ICEBERG_DDLS
from procurement.silver.resources.contractor_result.transform import (
    CONTRACTOR_RESULT_TRANSFORM_VERSION,
    ContractorResultTransformOutput,
    transform_contractor_result,
)

__all__ = [
    "CONTRACTOR_RESULT_ICEBERG_DDLS",
    "CONTRACTOR_RESULT_QUALITY_RULES",
    "CONTRACTOR_RESULT_TRANSFORM_VERSION",
    "ContractorResultTransformOutput",
    "transform_contractor_result",
]
