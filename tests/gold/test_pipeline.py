"""On-disk pipeline acceptance with real SQL and emulated publication, no network."""

from datetime import date

import pytest

from procurement.gold.application.backend import PipelineBackend
from procurement.gold.domain.models import INPUT_TABLES, RunPlan, SilverReadyInput, Snapshot
from procurement.gold.domain.schema import TABLES
from procurement.gold.application.gold_service import GoldService
from procurement.gold.application.state import StateStore
from tests.gold.test_business import populated


class Reader:
    def __init__(self, db):
        self.tables = {
            n: db.execute("SELECT * FROM silver." + n).to_arrow_table() for n in INPUT_TABLES
        }
        self.types = {
            n: tuple((r[0], r[1]) for r in db.execute("DESCRIBE silver." + n).fetchall())
            for n in INPUT_TABLES
        }
        self.calls = []

    def verify(self, ready):
        assert all(t.rows == len(self.tables[t.table]) for t in ready.tables)

    def columns(self, snapshot):
        return self.types[snapshot.table]

    def batches(self, snapshot):
        self.calls.append(snapshot.table)
        yield from self.tables[snapshot.table].to_batches(max_chunksize=2)

    def plan(self, base, run_id="r1", release=None, day=None, batch_rows=10000):
        raw = base.model_dump()
        source = base.source.model_dump()
        source["tables"] = tuple(
            Snapshot(
                table=n,
                table_uuid=next(t.table_uuid for t in base.source.tables if t.table == n),
                snapshot_id=1,
                rows=len(self.tables[n]),
            )
            for n in INPUT_TABLES
        )
        source["input_id"] = run_id + "_inputs"
        raw.update(
            run_id=run_id,
            mode="incremental" if release else "backfill",
            base_release=release,
            source=SilverReadyInput.model_validate(source),
            observation_day=day or base.observation_day,
            budget={"memory_limit": "512MB", "threads": 1, "batch_rows": batch_rows},
        )
        return RunPlan.model_validate(raw)


class Publisher:
    """Snapshot/activation semantics emulator, not an Iceberg acceptance claim."""

    def __init__(self):
        self.releases = {}
        self.active = None
        self.fail = False

    def publish_verified(self, db, plan, schema):
        data = {
            n: db.execute(f"SELECT * FROM {schema}.{n} ORDER BY ALL").fetchall() for n in TABLES
        }
        if plan.run_id in self.releases:
            assert self.releases[plan.run_id] == data
        self.releases[plan.run_id] = data
        if self.fail:
            self.fail = False
            raise RuntimeError("remote commit acknowledgement lost")
        return {"release_id": plan.run_id, "tables": len(TABLES)}

    def activate(self, plan):
        self.active = plan.run_id
        return {"release_id": plan.run_id, "plan_identity": plan.identity}

    def is_active(self, plan):
        return self.active == plan.run_id


def test_full_pipeline_incremental_replay_rebuild_and_new_status_day(tmp_path, db, plan):
    populated(db)
    reader = Reader(db)
    publisher = Publisher()
    root = tmp_path / "state"
    first = reader.plan(plan)
    service = GoldService(
        root, PipelineBackend(reader, publisher), version=lambda: plan.logic_version
    )
    service.create(first)
    assert service.resume("r1")["phase"] == "COMMITTED"
    previous = publisher.releases["r1"]
    assert len(reader.calls) == len(INPUT_TABLES)
    assert service.resume("r1")["phase"] == "COMMITTED" and len(reader.calls) == len(INPUT_TABLES)
    db.execute("UPDATE silver.package_revision SET bid_price=bid_price+10 WHERE entity_id='p1'")
    changed = Reader(db)
    second = changed.plan(plan, "r2", "r1")
    service = GoldService(
        root, PipelineBackend(changed, publisher), version=lambda: plan.logic_version
    )
    service.create(second)
    service.resume("r2")
    assert publisher.releases["r2"]["fact_demand_daily"] != previous["fact_demand_daily"]
    # Independent clean state; neither incremental cache nor ledger is reused.
    cleanpub = Publisher()
    clean = changed.plan(plan, "clean")
    cleanservice = GoldService(
        tmp_path / "clean", PipelineBackend(changed, cleanpub), version=lambda: plan.logic_version
    )
    cleanservice.create(clean)
    cleanservice.resume("clean")
    assert publisher.releases["r2"] == cleanpub.releases["clean"]
    # Unchanged source next observation day retains old snapshots.
    third = changed.plan(plan, "r3", "r2", date(2026, 11, 8))
    service.create(third)
    service.resume("r3")
    assert len(publisher.releases["r3"]["fact_status_daily"]) > len(
        publisher.releases["r2"]["fact_status_daily"]
    )
    assert (
        publisher.releases["r3"]["fact_demand_daily"]
        == publisher.releases["r2"]["fact_demand_daily"]
    )


def test_batch_checkpoint_survives_new_process_connection(tmp_path, db, plan):
    populated(db)
    # Separate business components, not an arbitrary month split. The original
    # fixture intentionally links nearly every package to the same plan.
    db.execute("UPDATE silver.package_revision SET plan_id=NULL,plan_no=NULL")
    reader = Reader(db)
    publisher = Publisher()
    # Force at least a small set of complete component batches, without splitting.
    request = reader.plan(plan, batch_rows=60)

    def stop(done, total):
        if done == 1:
            raise RuntimeError("stop after durable batch")

    service = GoldService(
        tmp_path,
        PipelineBackend(reader, publisher, on_batch=stop),
        version=lambda: plan.logic_version,
    )
    service.create(request)
    with pytest.raises(RuntimeError, match="durable batch"):
        service.resume("r1")
    with StateStore(tmp_path) as state:
        assert state.phase("r1") == "INPUTS_FROZEN"
        assert state.db.execute("SELECT count(*) FROM control.completed_batches").fetchone()[0] == 1
        assert state.db.execute("SELECT count(*) FROM control.accepted").fetchone()[0] == 0
    completed = []
    service = GoldService(
        tmp_path,
        PipelineBackend(reader, publisher, on_batch=lambda done, total: completed.append(done)),
        version=lambda: plan.logic_version,
    )
    service.resume("r1")
    assert 1 not in completed and len(reader.calls) == len(INPUT_TABLES)
    clean_publisher = Publisher()
    clean_request = reader.plan(plan, run_id="clean", batch_rows=10000)
    clean_service = GoldService(
        tmp_path / "independent",
        PipelineBackend(reader, clean_publisher),
        version=lambda: plan.logic_version,
    )
    clean_service.create(clean_request)
    clean_service.resume("clean")
    assert publisher.releases["r1"] == clean_publisher.releases["clean"]


def test_uncertain_publication_does_not_advance_accepted_state(tmp_path, db, plan):
    populated(db)
    reader = Reader(db)
    publisher = Publisher()
    publisher.fail = True
    service = GoldService(
        tmp_path, PipelineBackend(reader, publisher), version=lambda: plan.logic_version
    )
    service.create(reader.plan(plan))
    with pytest.raises(RuntimeError, match="acknowledgement"):
        service.resume("r1")
    with StateStore(tmp_path) as state:
        assert state.phase("r1") == "PREPARED"
        assert state.db.execute("SELECT count(*) FROM control.accepted").fetchone()[0] == 0
    assert publisher.active is None
    service.resume("r1")
    assert publisher.active == "r1" and len(publisher.releases) == 1


@pytest.mark.parametrize(
    "edit",
    [
        "UPDATE silver.package_location SET province_code='DNG',province_name='Da Nang' WHERE revision_id='rev_p1'",
        "DELETE FROM silver.award_contractor WHERE revision_id='rev_res_1_2024'",
        "UPDATE silver.result_revision SET status_raw='HUY' WHERE revision_id='rev_res_1_2024'",
        "UPDATE silver.notice_revision SET package_id='p2',package_no='PKG_002' WHERE entity_id='not1'",
    ],
)
def test_historical_edit_delete_and_reparent_equal_clean_rebuild(tmp_path, db, plan, edit):
    populated(db)
    initial = Reader(db)
    published = Publisher()
    service = GoldService(
        tmp_path / "incremental",
        PipelineBackend(initial, published),
        version=lambda: plan.logic_version,
    )
    service.create(initial.plan(plan))
    service.resume("r1")
    before = db.execute("SELECT count(*) FROM silver.observation").fetchone()[0]
    db.execute(edit)
    # No new event day required: changed old data must still trigger correction.
    assert db.execute("SELECT count(*) FROM silver.observation").fetchone()[0] == before
    changed = Reader(db)
    request = changed.plan(plan, "r2", "r1")
    service = GoldService(
        tmp_path / "incremental",
        PipelineBackend(changed, published),
        version=lambda: plan.logic_version,
    )
    service.create(request)
    service.resume("r2")
    clean = Publisher()
    clean_request = changed.plan(plan, "clean")
    full = GoldService(
        tmp_path / "clean", PipelineBackend(changed, clean), version=lambda: plan.logic_version
    )
    full.create(clean_request)
    full.resume("clean")
    assert published.releases["r2"] == clean.releases["clean"]
