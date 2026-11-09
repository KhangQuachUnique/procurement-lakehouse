"""Gold Application Service orchestrating the pipeline execution lifecycle.

Hexagonal Application Service implementing the ProcessGoldPort.
Manages deterministic transition across states:
PLANNED -> INPUTS_FROZEN -> CALCULATED -> PREPARED -> PUBLISHED_VERIFIED -> ACTIVE -> COMMITTED.
"""

from collections.abc import Callable
from typing import Any, Protocol

from procurement.gold.domain.models import Phase, RunPlan, compute_gold_logic_version
from procurement.gold.ports.inbound.process_gold import ProcessGoldPort


class Backend(Protocol):
    """Protocol for injected computational and IO backends."""

    def freeze(self, db: Any, plan: RunPlan) -> dict[str, Any]: ...
    def calculate(self, db: Any, plan: RunPlan) -> dict[str, Any]: ...
    def prepare(self, db: Any, plan: RunPlan) -> dict[str, Any]: ...
    def publish_verified(self, db: Any, plan: RunPlan) -> dict[str, Any]: ...
    def activate(self, db: Any, plan: RunPlan) -> dict[str, Any]: ...
    def accept(self, db: Any, plan: RunPlan) -> None: ...


class GoldService(ProcessGoldPort):
    """Hexagonal Application Service for Gold dimensional processing."""

    def __init__(
        self,
        root: Any,
        backend: Backend,
        version: Callable[[], str] = compute_gold_logic_version,
    ):
        self.root = root
        self.backend = backend
        self.version = version

    def create(self, plan: RunPlan) -> str:
        """Register a new run plan in state store and return run_id."""
        from procurement.gold.application.state import StateStore

        if plan.logic_version != self.version():
            raise ValueError("Plan logic differs from installed code")
        with StateStore(self.root) as state:
            state.create(plan)
        return plan.run_id

    def resume(self, run_id: str, on_phase: Callable[[Phase], None] | None = None) -> dict[str, Any]:
        """Execute or resume pending phases until COMMITTED."""
        from procurement.gold.application.state import StateStore

        with StateStore(self.root) as state:
            plan = state.plan(run_id)
            if plan.logic_version != self.version():
                raise ValueError("Cannot resume under a different logic version")

            state.db.execute("SET memory_limit=?", [plan.budget.memory_limit])
            state.db.execute("SET threads=?", [plan.budget.threads])

            for start, end, method in (
                (Phase.PLANNED, Phase.INPUTS_FROZEN, "freeze"),
                (Phase.INPUTS_FROZEN, Phase.CALCULATED, "calculate"),
                (Phase.CALCULATED, Phase.PREPARED, "prepare"),
                (Phase.PREPARED, Phase.PUBLISHED_VERIFIED, "publish_verified"),
                (Phase.PUBLISHED_VERIFIED, Phase.ACTIVE, "activate"),
                (Phase.ACTIVE, Phase.COMMITTED, "accept"),
            ):
                if state.phase(run_id) != start:
                    continue
                try:
                    if start == Phase.INPUTS_FROZEN and hasattr(self.backend, "equivalent"):
                        with state.transaction() as db:
                            equivalent = self.backend.equivalent(db, plan)
                            if equivalent is not None:
                                state.accept_equivalent(plan, equivalent)
                        if equivalent is not None:
                            return state.status(run_id)

                    evidence = None
                    if method in {"freeze", "calculate", "publish_verified"}:
                        evidence = getattr(self.backend, method)(state.db, plan)

                    with state.transaction() as db:
                        if method == "accept":
                            receipt = state.receipt(run_id, "activate")
                            self.backend.accept(db, plan)
                            state.accept(plan, receipt)
                        else:
                            receipt = (
                                evidence
                                if evidence is not None
                                else getattr(self.backend, method)(db, plan)
                            )
                            if not isinstance(receipt, dict):
                                raise ValueError("Phase must return durable evidence")
                            state.checkpoint(run_id, method, receipt)
                        state.advance(run_id, start, end)
                except BaseException as exc:
                    state.error(run_id, exc)
                    raise
                if on_phase:
                    on_phase(end)
            return state.status(run_id)
