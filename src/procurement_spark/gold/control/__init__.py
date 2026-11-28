"""PostgreSQL control plane for Spark Gold runs and Silver snapshot watermarks."""

from procurement_spark.gold.control.watermarks import GoldWatermarkStore

__all__ = ["GoldWatermarkStore"]
