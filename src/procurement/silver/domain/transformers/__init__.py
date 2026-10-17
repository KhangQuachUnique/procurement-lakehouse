"""Silver transformers package. Automatically imports all registered modules."""

from procurement.silver.domain.transformers.base import BaseResourceTransformer
from procurement.silver.domain.transformers.notice import (
    ReofferNoticeTransformer,
    StandardNoticeTransformer,
    VkAdbNoticeTransformer,
)
from procurement.silver.domain.transformers.project import ProjectDetailTransformer
from procurement.silver.domain.transformers.result import ContractorResultTransformer

__all__ = [
    "BaseResourceTransformer",
    "ContractorResultTransformer",
    "ProjectDetailTransformer",
    "ReofferNoticeTransformer",
    "StandardNoticeTransformer",
    "VkAdbNoticeTransformer",
]
