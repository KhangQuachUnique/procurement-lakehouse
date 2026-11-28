"""PostgreSQL control plane for Spark Gold runs and Silver snapshot watermarks."""

from procurement.gold.control.watermarks import GoldWatermarkStore

__all__ = ["GoldWatermarkStore"]
