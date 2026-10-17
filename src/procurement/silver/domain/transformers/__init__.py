"""Silver transformers package. Automatically imports all registered modules."""

from procurement.silver.domain.transformers.base import BaseResourceTransformer
from procurement.silver.domain.transformers.notice import (
    ReofferNoticeTransformer,
    StandardNoticeTransformer,
    VkAdbNoticeTransformer,
)
from procurement.silver.domain.transformers.project import ProjectDetailTransformer

__all__ = [
    "BaseResourceTransformer",
    "ProjectDetailTransformer",
    "ReofferNoticeTransformer",
    "StandardNoticeTransformer",
    "VkAdbNoticeTransformer",
]
