"""Inbound Port for driving Gold pipeline execution.

Defines the use-case interface called by CLI, Dagster, or external orchestrators.
"""

from typing import Any, Protocol

from procurement.gold.domain.models import RunPlan


class ProcessGoldPort(Protocol):
    """Primary port for executing or resuming a Gold dimensional pipeline run."""

    def create(self, plan: RunPlan) -> str:
        """Register a new run plan and return its run_id."""
        ...

    def resume(self, run_id: str, on_phase: Any = None) -> dict[str, Any]:
        """Execute or resume state machine phases until COMMITTED or terminal state."""
        ...
