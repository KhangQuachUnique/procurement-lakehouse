"""Silver transformers package. Automatically imports all registered modules."""

from procurement.silver.domain.transformers.base import BaseResourceTransformer
from procurement.silver.domain.transformers.notice import (
    ReofferNoticeTransformer,
    StandardNoticeTransformer,
    VkAdbNoticeTransformer,
)
from procurement.silver.domain.transformers.package import PackageDetailTransformer
from procurement.silver.domain.transformers.plan import PlanDetailTransformer
from procurement.silver.domain.transformers.project import ProjectDetailTransformer
from procurement.silver.domain.transformers.result import ContractorResultTransformer

from procurement.silver.domain.transformers.bid_opening import (
    BidOpeningTransformer,
)

__all__ = [
    "BaseResourceTransformer",
    "ContractorResultTransformer",
    "PackageDetailTransformer",
    "PlanDetailTransformer",
    "ProjectDetailTransformer",
    "ReofferNoticeTransformer",
    "StandardNoticeTransformer",
    "VkAdbNoticeTransformer",
]
