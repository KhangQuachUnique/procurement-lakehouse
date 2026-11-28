"""Project resource package."""

from procurement.silver.resources.project.quality import PROJECT_QUALITY_RULES
from procurement.silver.resources.project.schema import PROJECT_ICEBERG_DDLS
from procurement.silver.resources.project.transform import (
    PROJECT_TRANSFORM_VERSION,
    transform_project,
)

__all__ = [
    "PROJECT_ICEBERG_DDLS",
    "PROJECT_QUALITY_RULES",
    "PROJECT_TRANSFORM_VERSION",
    "transform_project",
]
