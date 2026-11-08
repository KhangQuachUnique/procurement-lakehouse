from datetime import date

import pytest

from procurement.gold.v2.backend import PipelineBackend
from procurement.gold.v2.contracts import RunPlan, Snapshot
from procurement.gold.v2.service import GoldService
from procurement.gold.v2.state import StateStore
from procurement.silver.application.change_journal import record
from tests.gold_v2.test_business import populated
from tests.gold_v2.test_pipeline import Publisher, Reader


def first_run(tmp_path, db, plan):
    populated(db)
    reader = Reader(db)
    publisher = Publisher()
    request = reader.plan(plan)
    service = GoldService(
        tmp_path, PipelineBackend(reader, publisher), version=lambda: plan.logic_version
    )
    service.create(request)
    service.resume("r1")
    return reader, publisher


def test_equivalent_new_run_advances_source_only_not_publication(tmp_path, db, plan):
    reader, publisher = first_run(tmp_path, db, plan)
    request = reader.plan(plan, "r2", "r1")
    service = GoldService(
        tmp_path, PipelineBackend(reader, publisher), version=lambda: plan.logic_version
    )
    service.create(request)
    result = service.resume("r2")
    assert result["equivalent_source"]["reused"] is True
    assert publisher.active == "r1" and set(publisher.releases) == {"r1"}
    with StateStore(tmp_path) as state:
        assert state.db.execute("SELECT run_id,release_id FROM control.accepted").fetchone() == (
            "r2",
            "r1",
        )
    # Next update bases off public release r1, NOT the source-only run ID.
    third = reader.plan(plan, "r3", "r1", date(2026, 11, 8))
    service.create(third)
    service.resume("r3")
    assert publisher.active == "r3"


def test_continuous_journal_avoids_source_reads_and_missing_chain_falls_back(tmp_path, db, plan):
    reader, publisher = first_run(tmp_path / "state", db, plan)
    before = reader.tables["package_revision"]
    db.execute("UPDATE silver.package_revision SET bid_price=bid_price+1 WHERE entity_id='p1'")
    changed = Reader(db)
    after = changed.tables["package_revision"]
    record(
        tmp_path / "journal",
        "silver",
        "package_revision",
        1,
        2,
        "uuid_package_revision",
        before,
        after,
    )
    request = changed.plan(plan, "r2", "r1")
    source = request.source.model_dump()
    source["tables"] = tuple(
        Snapshot.model_validate(
            t.model_dump() | ({"snapshot_id": 2} if t.table == "package_revision" else {})
        )
        for t in request.source.tables
    )
    raw = request.model_dump() | {"source": source, "change_journal": str(tmp_path / "journal")}
    request = RunPlan.model_validate(raw)
    service = GoldService(
        tmp_path / "state", PipelineBackend(changed, publisher), version=lambda: plan.logic_version
    )
    service.create(request)
    service.resume("r2")
    assert changed.calls == []  # 1 receipt edge + 7 identical snapshot caches
    with StateStore(tmp_path / "state") as state:
        assert set(state.receipt("r2", "freeze")["capture_strategies"].values()) == {"journal"}
    # Same contents but physical snapshot changes without receipt: full pin read.
    replay = changed.plan(plan, "r3", "r2")
    source = replay.source.model_dump()
    source["tables"] = tuple(
        Snapshot.model_validate(
            t.model_dump() | ({"snapshot_id": 3} if t.table == "package_revision" else {})
        )
        for t in replay.source.tables
    )
    replay = RunPlan.model_validate(
        replay.model_dump() | {"source": source, "change_journal": str(tmp_path / "journal")}
    )
    service.create(replay)
    result = service.resume("r3")
    assert changed.calls == ["package_revision"]
    assert result["equivalent_source"]["release_id"] == "r2"


def test_equivalent_source_refuses_remote_pointer_conflict(tmp_path, db, plan):
    reader, publisher = first_run(tmp_path, db, plan)
    publisher.active = "foreign_writer"
    service = GoldService(
        tmp_path, PipelineBackend(reader, publisher), version=lambda: plan.logic_version
    )
    service.create(reader.plan(plan, "r2", "r1"))
    with pytest.raises(ValueError, match="pointer differs"):
        service.resume("r2")
    with StateStore(tmp_path) as state:
        assert state.db.execute("SELECT run_id FROM control.accepted").fetchone()[0] == "r1"


def test_source_only_checkpoint_keeps_future_rows_for_later_cutoff(tmp_path, db, plan):
    _reader, publisher = first_run(tmp_path, db, plan)
    # Duplicate the actual source shape, but this correction is observed only at
    # the next cutoff. Raw cache must preserve it even while selected inputs match.
    db.execute(
        "INSERT INTO silver.observation BY NAME SELECT * REPLACE('future_obs' AS observation_id,DATE '2026-11-07' AS source_date,'99999' AS source_version) FROM silver.observation WHERE observation_id='obs_p1'"
    )
    db.execute(
        "INSERT INTO silver.package_revision BY NAME SELECT * REPLACE('future_revision' AS revision_id,'future_obs' AS observation_id,bid_price+1 AS bid_price) FROM silver.package_revision WHERE revision_id='rev_p1'"
    )
    future = Reader(db)
    request = future.plan(plan, "r2", "r1")
    raw = request.model_dump()
    source = request.source.model_dump()
    source["source_end"] = date(2026, 11, 7)
    source["tables"] = tuple(
        Snapshot.model_validate(
            t.model_dump()
            | ({"snapshot_id": 2} if t.table in {"observation", "package_revision"} else {})
        )
        for t in request.source.tables
    )
    request = RunPlan.model_validate(raw | {"source": source})
    service = GoldService(
        tmp_path, PipelineBackend(future, publisher), version=lambda: plan.logic_version
    )
    service.create(request)
    assert service.resume("r2")["equivalent_source"]["reused"]
    third = RunPlan.model_validate(
        request.model_dump()
        | {
            "run_id": "r3",
            "source_cutoff": date(2026, 11, 7),
            "change_journal": str(tmp_path / "empty_journal"),
        }
    )
    service.create(third)
    service.resume("r3")
    assert publisher.active == "r3"
    with StateStore(tmp_path) as state:
        assert (
            state.db.execute(
                "SELECT count(*) FROM input_cache.package_revision WHERE revision_id='future_revision'"
            ).fetchone()[0]
            == 1
        )
