"""Outbound Port for publishing Gold dimensional tables to Iceberg Lakehouse."""

from typing import Any, Protocol, runtime_checkable

from procurement.gold.domain.models import RunPlan


@runtime_checkable
class GoldStoragePort(Protocol):
    """Port interface for writing and managing Iceberg Format v2 Gold tables."""

    def publish_verified(self, db: Any, plan: RunPlan, schema: str) -> dict[str, Any]:
        """Publish verified candidate tables to lake.gold_v2."""
        ...

    def activate(self, plan: RunPlan) -> dict[str, Any]:
        """Atomically activate the published release."""
        ...

    def is_active(self, plan: RunPlan) -> bool:
        """Check whether the given plan's release is currently active."""
        ...
