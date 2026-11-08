import duckdb
import pytest

from procurement.gold.v2.planning import LINKS, ROOTS, plan_components
from procurement.gold.v2.relationship_index import source_links
from procurement.gold.v2.relationship_stage import prepare_stage, verify_stage
from tests.gold_v2.reference_planning import plan_components as reference
from tests.gold_v2.test_relationship_index import graph_result, isolated


def test_link_failure_rolls_back_nodes_and_cursor_together(db):
    isolated(db)
    db.execute("CREATE TABLE attempted_nodes(node VARCHAR)")
    db.execute(
        "CREATE TABLE attempted_links(node VARCHAR,token VARCHAR CHECK(NOT starts_with(token,'package_code:')))"
    )
    db.execute(
        "CREATE TABLE link_cursor(root VARCHAR PRIMARY KEY,last_key VARCHAR,complete BOOLEAN)"
    )
    with pytest.raises(duckdb.ConstraintException):
        source_links(
            db,
            "frozen",
            "attempted_nodes",
            "attempted_links",
            ROOTS,
            LINKS,
            chunk_nodes=1,
            checkpoints="link_cursor",
        )
    assert db.execute("SELECT count(*) FROM attempted_nodes").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM attempted_links").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM link_cursor").fetchone()[0] == 0


def test_chunks_keep_all_versions_and_deduplicate_aliases_without_art(db):
    isolated(db)
    db.execute("UPDATE frozen.package_revision SET root_json=json_object('id',entity_id)")
    db.execute(
        "INSERT INTO frozen.package_revision SELECT * FROM frozen.package_revision WHERE entity_id='a'"
    )
    db.execute("CREATE TABLE chunk_nodes(node VARCHAR)")
    db.execute("CREATE TABLE chunk_links(node VARCHAR,token VARCHAR)")
    chunks = []
    source_links(
        db,
        "frozen",
        "chunk_nodes",
        "chunk_links",
        ROOTS,
        LINKS,
        chunk_nodes=1,
        on_chunk=lambda *args: chunks.append(args),
    )
    assert chunks and all(count == 1 for _, count, _ in chunks)
    assert (
        db.execute("SELECT count(*) FROM chunk_nodes WHERE node='package_revision:a'").fetchone()[0]
        == 1
    )
    assert (
        db.execute(
            "SELECT count(*) FROM chunk_links WHERE node='package_revision:a' AND token='package:a'"
        ).fetchone()[0]
        == 1
    )
    assert (
        db.execute(
            "SELECT count(*) FROM (SELECT node,token FROM chunk_links GROUP BY ALL HAVING count(*)>1)"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM duckdb_indexes() WHERE table_name IN ('chunk_nodes','chunk_links')"
        ).fetchone()[0]
        == 0
    )


def test_stage_resume_is_exact_and_different_run_cannot_reuse_it(db, plan):
    isolated(db)
    db.execute("CREATE SCHEMA control")
    db.execute("CREATE TABLE control.runs(run_id VARCHAR,phase VARCHAR)")
    db.execute("INSERT INTO control.runs VALUES ('r1','INPUTS_FROZEN')")
    db.execute("UPDATE frozen.package_revision SET title='new title'")
    reference(db)
    expected = graph_result(db)

    def interrupted(*_):
        raise RuntimeError("after durable relationship chunk")

    with pytest.raises(RuntimeError, match="durable relationship chunk"):
        prepare_stage(db, plan, ROOTS, LINKS, chunk_nodes=1, on_chunk=interrupted)
    assert db.execute("SELECT count(*) FROM relation_stage.new_nodes").fetchone()[0] == 1
    with pytest.raises(ValueError, match="incomplete"):
        verify_stage(db, plan.identity)
    other = plan.model_copy(update={"run_id": "other"})
    with pytest.raises(ValueError, match="unfinished relationship staging"):
        prepare_stage(db, other, ROOTS, LINKS, chunk_nodes=1)
    prepare_stage(db, plan, ROOTS, LINKS, chunk_nodes=1)
    verify_stage(db, plan.identity)
    assert db.execute("SELECT count(*) FROM relation_stage.new_nodes").fetchone()[0] == 3
    assert (
        db.execute(
            "SELECT count(*) FROM (SELECT node,token FROM relation_stage.new_links GROUP BY ALL HAVING count(*)>1)"
        ).fetchone()[0]
        == 0
    )
    plan_components(db, stage_identity=plan.identity)
    assert graph_result(db) == expected
    before = db.execute("SELECT * FROM relation_stage.new_links ORDER BY ALL").fetchall()
    prepare_stage(db, plan, ROOTS, LINKS, chunk_nodes=1)
    assert db.execute("SELECT * FROM relation_stage.new_links ORDER BY ALL").fetchall() == before
