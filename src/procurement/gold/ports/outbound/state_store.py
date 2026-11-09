"""Outbound Port for persisting Gold pipeline run state and checkpoints."""

from typing import Any, Protocol

from procurement.gold.domain.models import Phase, RunPlan


class StateStorePort(Protocol):
    """Port interface for run state and execution checkpoint persistence."""

    def create(self, plan: RunPlan) -> None:
        """Record a planned run."""
        ...

    def phase(self, run_id: str) -> Phase:
        """Get the current execution phase of a run."""
        ...

    def plan(self, run_id: str) -> RunPlan:
        """Retrieve the immutable RunPlan for a run."""
        ...

    def transition(self, run_id: str, phase: Phase, evidence: Any = None) -> None:
        """Transition a run to a new phase with optional audit evidence."""
        ...

    def status(self, run_id: str) -> dict[str, Any]:
        """Get the full status dictionary for a run."""
        ...
