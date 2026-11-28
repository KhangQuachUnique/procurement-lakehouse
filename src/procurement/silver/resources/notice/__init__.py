"""Notice resource package (standard, vk_adb, reoffer)."""

from procurement.silver.resources.notice.quality import NOTICE_QUALITY_RULES
from procurement.silver.resources.notice.schema import NOTICE_ICEBERG_DDLS
from procurement.silver.resources.notice.transform import NOTICE_TRANSFORM_VERSION, transform_notice

__all__ = [
    "NOTICE_ICEBERG_DDLS",
    "NOTICE_QUALITY_RULES",
    "NOTICE_TRANSFORM_VERSION",
    "transform_notice",
]
