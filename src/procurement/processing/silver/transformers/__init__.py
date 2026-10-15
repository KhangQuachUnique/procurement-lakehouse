"""Silver transformers package. Automatically imports all registered modules."""

from procurement.processing.silver.transformers.base import BaseResourceTransformer
from procurement.processing.silver.transformers.project import ProjectDetailTransformer
from procurement.processing.silver.transformers.result import ResultDetailTransformer

__all__ = ["BaseResourceTransformer", "ProjectDetailTransformer", "ResultDetailTransformer"]
