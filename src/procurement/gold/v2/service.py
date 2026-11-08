"""One lifecycle for CLI/Dagster. Backends own IO, never choose dates or inputs."""

from typing import Protocol

from .contracts import Phase, RunPlan, logic_version
from .state import StateStore


class Backend(Protocol):
    # Remote methods must verify uncertain side effects using run_id as their
    # idempotency identity. Local transactions cannot roll back remote commits.
    def freeze(self, db, plan: RunPlan) -> dict: ...
    def calculate(self, db, plan: RunPlan) -> dict: ...
    def prepare(self, db, plan: RunPlan) -> dict: ...
    def publish_verified(self, db, plan: RunPlan) -> dict: ...
    def activate(self, db, plan: RunPlan) -> dict: ...
    def accept(self, db, plan: RunPlan) -> None: ...


class GoldService:
    def __init__(self, root, backend: Backend, version=logic_version):
        self.root, self.backend, self.version = root, backend, version

    def create(self, plan):
        if plan.logic_version != self.version():
            raise ValueError("Plan logic differs from installed code")
        with StateStore(self.root) as state:
            state.create(plan)
        return plan.run_id

    def resume(self, run_id, on_phase=None):
        with StateStore(self.root) as state:
            plan = state.plan(run_id)
            if plan.logic_version != self.version():
                raise ValueError("Cannot resume under a different logic version")
            # The frozen budget applies to capture/equivalence as well as compute.
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
