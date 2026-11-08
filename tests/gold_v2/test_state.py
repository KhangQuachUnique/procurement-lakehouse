import pytest

from procurement.gold.v2.contracts import Phase, RunPlan
from procurement.gold.v2.service import GoldService
from procurement.gold.v2.state import StateStore


class Backend:
    def __init__(self):
        self.calls = []
        self.remote = {}
        self.fail = None

    def effect(self, name, db, plan):
        self.calls.append(name)
        # Remote side effect survives local rollback/lost acknowledgement.
        receipt = self.remote.setdefault(
            name, {"release_id": plan.run_id, "plan_identity": plan.identity}
        )
        if self.fail == name:
            self.fail = None
            raise RuntimeError("lost acknowledgement")
        return receipt

    def freeze(self, db, p):
        return self.effect("freeze", db, p)

    def calculate(self, db, p):
        return self.effect("calculate", db, p)

    def prepare(self, db, p):
        return self.effect("prepare", db, p)

    def publish_verified(self, db, p):
        return self.effect("publish_verified", db, p)

    def activate(self, db, p):
        return self.effect("activate", db, p)

    def accept(self, db, p):
        self.effect("accept", db, p)


def test_immutable_plan_and_unfinished_target(plan, tmp_path):
    with StateStore(tmp_path) as state:
        state.create(plan)
        state.create(plan)
        with pytest.raises(ValueError, match="immutable"):
            state.create(RunPlan.model_validate(plan.model_dump() | {"logic_version": "b" * 64}))
        with pytest.raises(ValueError, match="unfinished"):
            state.create(RunPlan.model_validate(plan.model_dump() | {"run_id": "r2"}))
        with pytest.raises(ValueError, match="Invalid phase"):
            state.advance("r1", Phase.PLANNED, Phase.COMMITTED)
        with pytest.raises(ValueError, match="before acknowledged"):
            state.accept(plan, {})


@pytest.mark.parametrize(
    "phase", ["freeze", "calculate", "prepare", "publish_verified", "activate", "accept"]
)
def test_restart_after_failure_and_replay(plan, tmp_path, phase):
    backend = Backend()
    backend.fail = phase
    service = GoldService(tmp_path, backend, version=lambda: plan.logic_version)
    service.create(plan)
    with pytest.raises(RuntimeError):
        service.resume("r1")
    with StateStore(tmp_path) as state:
        assert state.status("r1")["last_error_type"] == "RuntimeError"
        if phase != "accept":
            assert state.db.execute("SELECT count(*) FROM control.accepted").fetchone()[0] == 0
    # Fresh service reopens on-disk DB, reuses frozen dates and remote identity.
    result = GoldService(tmp_path, backend, version=lambda: plan.logic_version).resume("r1")
    assert result["phase"] == Phase.COMMITTED
    calls = list(backend.calls)
    assert service.resume("r1")["phase"] == Phase.COMMITTED
    assert backend.calls == calls
    assert len(backend.remote) == 6


def test_resume_code_identity_fence(plan, tmp_path):
    backend = Backend()
    GoldService(tmp_path, backend, version=lambda: plan.logic_version).create(plan)
    with pytest.raises(ValueError, match="different logic"):
        GoldService(tmp_path, backend, version=lambda: "b" * 64).resume("r1")
    assert not backend.calls


def test_output_and_phase_marker_rollback_together(plan, tmp_path):
    with StateStore(tmp_path) as state:
        state.create(plan)
        with pytest.raises(RuntimeError), state.transaction() as db:
            db.execute("CREATE TABLE control.test_output AS SELECT 1 x")
            state.advance("r1", Phase.PLANNED, Phase.INPUTS_FROZEN)
            raise RuntimeError("crash")
    with StateStore(tmp_path) as state:
        assert state.phase("r1") == Phase.PLANNED
        assert not state.db.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='test_output'"
        ).fetchone()[0]


@pytest.mark.parametrize(
    "change",
    [
        {"observation_day": "2026-11-06"},
        {"source_cutoff": "2026-11-05"},
        {"event_end": "2026-12-31"},
        {"reference_version": "configured_v2"},
    ],
)
def test_new_incremental_plan_preserves_definition_and_watermarks(plan, tmp_path, change):
    backend = Backend()
    service = GoldService(tmp_path, backend, version=lambda: plan.logic_version)
    service.create(plan)
    service.resume("r1")
    raw = plan.model_dump() | {"run_id": "r2", "mode": "incremental", "base_release": "r1"} | change
    with StateStore(tmp_path) as state, pytest.raises(ValueError):
        state.create(RunPlan.model_validate(raw))
