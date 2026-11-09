import pytest

from procurement.gold.application.recovery import backup, restore
from procurement.gold.application.state import StateStore


def test_restore_preserves_pending_run_outbox_and_rejects_corruption(plan, tmp_path):
    root = tmp_path / "state"
    with StateStore(root) as state:
        state.create(plan)
        state.db.execute("CREATE SCHEMA outbox")
        state.db.execute(
            "CREATE TABLE outbox.recovery_evidence AS SELECT 'owner1' owner_key,10::DECIMAL(38,6) amount"
        )
        state.checkpoint(plan.run_id, "test", {"snapshots": [1, 2]})
    manifest = backup(root, tmp_path / "backup")
    assert manifest["remote_data_backed_up"] is False
    assert restore(tmp_path / "backup", tmp_path / "restored") == manifest
    with StateStore(tmp_path / "restored") as state:
        assert state.plan(plan.run_id) == plan
        assert state.receipt(plan.run_id, "test") == {"snapshots": [1, 2]}
        assert state.db.execute("SELECT amount FROM outbox.recovery_evidence").fetchone()[0] == 10
    with pytest.raises(FileExistsError):
        restore(tmp_path / "backup", tmp_path / "restored")
    with (tmp_path / "backup" / "state.duckdb").open("ab") as stream:
        stream.write(b"corruption")
    with pytest.raises(ValueError, match="integrity"):
        restore(tmp_path / "backup", tmp_path / "invalid")
    assert not (tmp_path / "invalid").exists()
