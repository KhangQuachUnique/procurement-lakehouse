import pytest

from procurement.gold.domain.models import INPUT_TABLES
from procurement.gold.domain.clustering.planning import plan_components
from procurement.gold.domain.clustering.relationship_index import accept_index
from tests.gold.reference_planning import plan_components as full_planner
from tests.gold.test_planning import populated


def graph_result(db):
    return {
        name: db.execute(f"SELECT * FROM graph.{name} ORDER BY ALL").fetchall()
        for name in ("impacted", "sizes", "batches")
    }


@pytest.mark.parametrize(
    "change",
    [
        "SELECT 1",
        "UPDATE frozen.package_revision SET bid_price=bid_price+10 WHERE entity_id='p1'",
        "UPDATE frozen.notice_revision SET package_id='reparented',package_no='other'",
        "UPDATE frozen.package_revision SET plan_id=NULL,plan_no=NULL WHERE entity_id='p1'",
        "DELETE FROM frozen.package_revision WHERE entity_id='p1'",
        "DELETE FROM frozen.award_contractor",
        "DELETE FROM frozen.package_location",
        "UPDATE frozen.observation SET source_date=source_date+INTERVAL 1 DAY",
        "INSERT INTO frozen.notice_revision SELECT * FROM frozen.notice_revision",
    ],
)
def test_frontier_and_batch_assignment_match_original_full_planner(db, change):
    populated(db)
    db.execute(change)
    full_planner(db, batch_rows=10000)
    expected = graph_result(db)
    plan_components(db, batch_rows=10000)
    assert graph_result(db) == expected


def isolated(db):
    populated(db)
    for scope in ("frozen", "input_cache"):
        for name in INPUT_TABLES:
            db.execute(f"DELETE FROM {scope}.{name}")
        for key in ("a", "b", "unrelated"):
            db.execute(
                f"INSERT INTO {scope}.package_revision(entity_id,revision_id,package_no,observation_id) VALUES (?,?,?,?)",
                [key, key + "_v1", key, key + "_observation"],
            )
        db.execute(
            f"INSERT INTO {scope}.notice_revision(entity_id,revision_id,package_id,package_no,observation_id) VALUES ('bridge','bridge_v1','a','b','bridge_observation')"
        )


def test_accepted_index_reused_old_new_bridge_and_later_split(db):
    isolated(db)
    plan_components(db, cache_epoch="base")
    original_links = db.execute("SELECT * FROM relation_cache.links ORDER BY ALL").fetchall()
    # Removing a bridge must still retract BOTH old sides, but not unrelated.
    db.execute("DELETE FROM frozen.notice_revision")
    result = plan_components(db, cache_epoch="base")
    assert result["index_strategy"] == "accepted_index"
    assert db.execute("SELECT node FROM graph.impacted ORDER BY node").fetchall() == [
        ("notice_revision:bridge",),
        ("package_revision:a",),
        ("package_revision:b",),
    ]
    # Candidate planning must not have mutated accepted adjacency.
    assert (
        db.execute("SELECT * FROM relation_cache.links ORDER BY ALL").fetchall() == original_links
    )
    db.execute("BEGIN")
    accept_index(db, "candidate")
    db.execute("ROLLBACK")
    assert db.execute("SELECT epoch FROM relation_cache.meta").fetchone()[0] == "base"
    assert (
        db.execute("SELECT * FROM relation_cache.links ORDER BY ALL").fetchall() == original_links
    )
    db.execute("BEGIN")
    for name in INPUT_TABLES:
        db.execute(f"DELETE FROM input_cache.{name}")
        db.execute(f"INSERT INTO input_cache.{name} SELECT * FROM frozen.{name}")
    accept_index(db, "candidate")
    db.execute("COMMIT")
    db.execute("UPDATE frozen.package_revision SET title='changed' WHERE entity_id='a'")
    result = plan_components(db, cache_epoch="candidate")
    assert result["index_strategy"] == "accepted_index"
    assert db.execute("SELECT node FROM graph.impacted").fetchall() == [("package_revision:a",)]
    # Stale epoch must rebuild from accepted source, never trust stale links.
    result = plan_components(db, cache_epoch="new_epoch")
    assert result["index_strategy"] == "rebuilt_from_accepted_cache"


def test_transaction_failure_preserves_accepted_cache(db):
    isolated(db)
    plan_components(db, cache_epoch="base")
    links = db.execute("SELECT * FROM relation_cache.links ORDER BY ALL").fetchall()
    db.execute("UPDATE frozen.package_revision SET title='changed' WHERE entity_id='a'")
    db.execute("BEGIN")
    with pytest.raises(ValueError, match="Whole dependency component"):
        plan_components(db, cache_epoch="base", batch_rows=1)
    db.execute("ROLLBACK")
    assert db.execute("SELECT * FROM relation_cache.links ORDER BY ALL").fetchall() == links
