import json

import duckdb
import pytest

from procurement.gold.v2.uf_graph import prepare, solve
from procurement.gold.v2.union_find import UnionFind


def inputs(db, nodes, edges):
    db.execute("CREATE TABLE input_nodes(node VARCHAR)")
    db.execute("CREATE TABLE input_edges(node VARCHAR,token VARCHAR)")
    if nodes:
        db.executemany("INSERT INTO input_nodes VALUES (?)", [(n,) for n in nodes])
    if edges:
        db.executemany("INSERT INTO input_edges VALUES (?,?)", edges)


@pytest.mark.parametrize(
    "nodes,edges,expected",
    [
        ([], [], []),
        (["isolated"], [], [("isolated", "isolated")]),
        (
            ["a", "b", "c"],
            [("a", "x"), ("b", "x"), ("b", "y"), ("c", "y")],
            [("a", "a"), ("b", "a"), ("c", "a")],
        ),
        (
            ["a", "b", "c"],
            [("a", "x"), ("b", "x"), ("b", "y")],
            [("a", "a"), ("b", "a"), ("c", "c")],
        ),
        (
            ["a", "b", "a", "c"],
            [("a", "x"), ("a", "x"), ("b", "x")],
            [("a", "a"), ("b", "a"), ("c", "c")],
        ),
    ],
)
def test_numeric_bipartite_graph_and_exact_canonical_labels(db, tmp_path, nodes, edges, expected):
    inputs(db, nodes, edges)
    prepare(db, "input_nodes", "input_edges", "fixture", buckets=3)
    result = solve(db, tmp_path, "fixture", checkpoint_edges=1)
    assert db.execute("SELECT * FROM uf_graph.components ORDER BY node").fetchall() == expected
    assert result["array_bytes"] == 9 * len(set(nodes))
    # Replaying completed encoding/union/output must neither change nor append rows.
    prepare(db, "input_nodes", "input_edges", "fixture", buckets=3)
    solve(db, tmp_path, "fixture", checkpoint_edges=1)
    assert db.execute("SELECT * FROM uf_graph.components ORDER BY node").fetchall() == expected


def test_checkpoint_inside_hot_token_and_corruption_rejected(tmp_path):
    uf = UnionFind(20000)
    uf.feed(("hot", i) for i in range(1000))
    uf.save(tmp_path, "same-input")
    resumed = UnionFind.load(tmp_path, "same-input")
    resumed.feed(("hot", i) for i in range(1000, 20000))
    assert all(resumed.label(i) == 0 for i in range(20000))
    assert resumed.tokens == 1 and resumed.edges == 20000
    with pytest.raises(ValueError, match="identity"):
        UnionFind.load(tmp_path, "different-input")
    pointer = json.loads((tmp_path / "CURRENT").read_text())
    path = tmp_path / pointer["generation"] / "parent.bin"
    with path.open("r+b") as stream:
        stream.write(b"bad!")
    with pytest.raises(ValueError, match="checksum"):
        UnionFind.load(tmp_path, "same-input")


def test_real_planner_fixture_matches_original_label_propagation(db, tmp_path):
    from tests.gold_v2.reference_planning import plan_components
    from tests.gold_v2.test_planning import populated

    populated(db)
    plan_components(db, bootstrap=True)
    expected = db.execute("SELECT * FROM graph.nodes ORDER BY node").fetchall()
    prepare(db, "graph.nodes", "graph.links", "original-fixture", buckets=4)
    solve(db, tmp_path, "original-fixture", checkpoint_edges=5)
    assert db.execute("SELECT * FROM uf_graph.components ORDER BY node").fetchall() == expected


def test_full_uf_planner_charges_and_batches_match_original(db, plan, tmp_path):
    from procurement.gold.v2.contracts import INPUT_TABLES
    from procurement.gold.v2.full_uf_planner import prepare_full
    from procurement.gold.v2.planning import LINKS, ROOTS
    from procurement.gold.v2.relationship_stage import prepare_stage
    from tests.gold_v2.reference_planning import plan_components
    from tests.gold_v2.test_planning import populated
    from tests.gold_v2.test_relationship_index import graph_result

    populated(db)
    for name in INPUT_TABLES:
        db.execute(f"DELETE FROM input_cache.{name}")
    plan_components(db, bootstrap=True)
    expected = graph_result(db)
    prepare_stage(db, plan, ROOTS, LINKS)
    result = prepare_full(db, plan, tmp_path, buckets=4)
    assert graph_result(db) == expected
    assert result["affected_nodes"] == len(expected["impacted"])
    # Complete private planning checkpoint is safe to reuse without appending.
    assert prepare_full(db, plan, tmp_path, buckets=4) == result


def test_unknown_node_and_unsorted_edges_fail_closed(db):
    inputs(db, ["a"], [("missing", "x")])
    with pytest.raises(ValueError, match="absent node"):
        prepare(db, "input_nodes", "input_edges", "bad", buckets=1)
    uf = UnionFind(2)
    with pytest.raises(ValueError, match="sorted"):
        uf.feed([("z", 0), ("a", 1)])


def test_union_checkpoint_resume_does_not_drop_cross_bucket_links(tmp_path):
    with duckdb.connect(config={"threads": "1", "memory_limit": "128MB"}) as db:
        inputs(db, ["a", "b", "c", "d"], [("a", "x"), ("b", "x"), ("b", "y"), ("c", "y")])
        prepare(db, "input_nodes", "input_edges", "resume", buckets=7)

        def interrupt(*_):
            raise RuntimeError("after union checkpoint")

        with pytest.raises(RuntimeError, match="union checkpoint"):
            solve(db, tmp_path, "resume", checkpoint_edges=1, on_progress=interrupt)
        solve(db, tmp_path, "resume", checkpoint_edges=1)
        assert db.execute("SELECT * FROM uf_graph.components ORDER BY node").fetchall() == [
            ("a", "a"),
            ("b", "a"),
            ("c", "a"),
            ("d", "d"),
        ]
