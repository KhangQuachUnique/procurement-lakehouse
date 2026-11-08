from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.silver.domain.schema import get_primary_key
from procurement.tools.gold_history_batches import SUPPORTED
from procurement.tools.silver_bootstrap_views import create_views
from procurement.tools.silver_bootstrap_writer import load_empty


def files(root, ordinal, **rows):
    out = {}
    for name in SUPPORTED:
        columns = [get_primary_key(name)]
        if name in {"award_lot", "award_contractor"} and "revision_id" not in columns:
            columns.append("revision_id")
        if name in {"quality_issue", "quarantine"} and "observation_id" not in columns:
            columns.append("observation_id")
        schema = pa.schema([(c, pa.string()) for c in columns + ["marker"]])
        p = root / str(ordinal) / (name + ".parquet")
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows.get(name, []), schema=schema), p)
        out[name] = str(p)
    return ordinal, out


def test_final_cohort_preserves_all_its_rows_not_arbitrary_row_number(tmp_path):
    entries = [
        files(
            tmp_path,
            0,
            plan_revision=[
                {"revision_id": "p", "marker": "old"},
                {"revision_id": "q", "marker": "keep"},
            ],
        ),
        files(
            tmp_path,
            1,
            plan_revision=[
                {"revision_id": "p", "marker": "new1"},
                {"revision_id": "p", "marker": "new2"},
            ],
        ),
    ]
    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB"}) as db:
        views = create_views(db, entries)["views"]
        assert db.execute(f"SELECT * FROM {views['plan_revision']} ORDER BY marker").fetchall() == [
            ("q", "keep"),
            ("p", "new1"),
            ("p", "new2"),
        ]


def test_empty_child_refresh_and_unrelated_owners_then_new_children(tmp_path):
    entries = [
        files(
            tmp_path,
            0,
            result_revision=[
                {"revision_id": "r", "marker": "old"},
                {"revision_id": "s", "marker": "keep"},
            ],
            award_lot=[
                {"award_lot_id": "a", "revision_id": "r", "marker": "remove"},
                {"award_lot_id": "c", "revision_id": "s", "marker": "keep"},
            ],
        ),
        files(tmp_path, 1, result_revision=[{"revision_id": "r", "marker": "empty"}]),
        files(
            tmp_path,
            2,
            result_revision=[{"revision_id": "r", "marker": "changed"}],
            award_lot=[{"award_lot_id": "d", "revision_id": "r", "marker": "new"}],
        ),
    ]
    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB"}) as db:
        first = create_views(db, entries[:2], prefix="first")["views"]
        assert db.execute(f"SELECT award_lot_id FROM {first['award_lot']}").fetchall() == [("c",)]
        final = create_views(db, entries, prefix="final")["views"]
        assert db.execute(
            f"SELECT award_lot_id FROM {final['award_lot']} ORDER BY 1"
        ).fetchall() == [("c",), ("d",)]


def test_governance_clean_refresh_clears_old_errors_but_preserves_null_owners(tmp_path):
    entries = [
        files(
            tmp_path,
            0,
            observation=[{"observation_id": "o", "marker": "old"}],
            quality_issue=[
                {"issue_id": "i", "observation_id": "o", "marker": "remove"},
                {"issue_id": "global", "observation_id": None, "marker": "oldglobal"},
            ],
            quarantine=[{"observation_id": "o", "marker": "remove"}],
        ),
        files(
            tmp_path,
            1,
            observation=[{"observation_id": "o", "marker": "new"}],
            quality_issue=[{"issue_id": "global", "observation_id": None, "marker": "newglobal"}],
        ),
    ]
    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB"}) as db:
        views = create_views(db, entries)["views"]
        assert db.execute(f"SELECT count(*) FROM {views['quarantine']}").fetchone()[0] == 0
        assert sorted(db.execute(f"SELECT marker FROM {views['quality_issue']}").fetchall()) == [
            ("newglobal",),
            ("oldglobal",),
        ]


def test_bulk_never_touches_production_or_blindly_adopts_nonempty_table(tmp_path):
    db, cat = Mock(), Mock()
    with pytest.raises(ValueError, match="isolated"):
        load_empty(db, cat, "silver", {}, tmp_path, "id")
    db.assert_not_called()
    cat.assert_not_called()
    cat.table.return_value = {
        "metadata": {"format-version": 2, "current-snapshot-id": 1, "table-uuid": "uuid"}
    }
    with pytest.raises(ValueError, match="existing nonempty"):
        load_empty(db, cat, "test_silver_v2_new", {"observation": "selected"}, tmp_path, "id")
    assert not any("INSERT INTO" in c.args[0] for c in db.execute.call_args_list)


def test_incremental_merge_and_owner_retraction_match_full_even_with_duplicates(tmp_path):
    import re

    from procurement.tools.silver_bootstrap_views import create_views
    from procurement.tools.silver_bootstrap_writer import apply_delta, mismatch

    entries = [
        files(
            tmp_path,
            0,
            plan_revision=[
                {"revision_id": "p", "marker": "old1"},
                {"revision_id": "p", "marker": "old2"},
            ],
            result_revision=[{"revision_id": "r", "marker": "old"}],
            award_lot=[{"award_lot_id": "a", "revision_id": "r", "marker": "old"}],
            observation=[{"observation_id": "o", "marker": "old"}],
            quality_issue=[{"issue_id": "warning", "observation_id": "o", "marker": "old"}],
        ),
        files(
            tmp_path,
            1,
            plan_revision=[{"revision_id": "p", "marker": "new"}],
            result_revision=[{"revision_id": "r", "marker": "empty_children"}],
            observation=[{"observation_id": "o", "marker": "new"}],
        ),
    ]
    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB"}) as db:
        pre = create_views(db, entries[:1], prefix="pre")
        delta = create_views(db, entries[1:], prefix="delta")
        full = create_views(db, entries, prefix="full")
        db.execute("ATTACH ':memory:' AS lake")
        ns = "test_silver_v2_fixture"
        db.execute("CREATE SCHEMA lake." + ns)
        base = {}
        snapshots = {}
        for n, v in pre["views"].items():
            db.execute(f"CREATE TABLE lake.{ns}.{n} AS SELECT * FROM {v}")
            base[n] = {"table_uuid": "uuid_" + n, "snapshot_id": 1}
            snapshots[n] = 1

        class SQLTracker:
            current = None

            def __getattr__(self, n):
                return getattr(db, n)

            def execute(self, sql, *args):
                match = re.search(
                    r"(?:MERGE INTO|DELETE FROM|INSERT INTO) lake\." + ns + r"\.([a-z_]+)", sql
                )
                if match:
                    self.current = match.group(1)
                result = db.execute(sql, *args)
                if sql == "COMMIT" and self.current:
                    snapshots[self.current] += 1
                    self.current = None
                return result

        cat = Mock()
        cat.table.side_effect = lambda _, n: {
            "metadata": {"table-uuid": "uuid_" + n, "current-snapshot-id": snapshots[n]}
        }
        pins = apply_delta(
            SQLTracker(), cat, ns, delta, full["views"], base, tmp_path / "acks", "increment"
        )
        assert pins["plan_revision"]["operation"] == "upsert_preserving_cohort_multiplicity"
        assert pins["observation"]["operation"] == "merge"
        assert pins["award_lot"]["operation"] == "replace_owner_set"
        assert all(mismatch(db, f"lake.{ns}.{n}", v) == 0 for n, v in full["views"].items())
        before = snapshots.copy()
        apply_delta(
            SQLTracker(), cat, ns, delta, full["views"], base, tmp_path / "acks", "increment"
        )
        assert snapshots == before


def test_disabling_physical_insertion_order_preserves_cohort_and_owner_semantics(tmp_path):
    entries = [
        files(tmp_path, 0,
            plan_revision=[{'revision_id':'p','marker':'old'}],
            result_revision=[{'revision_id':'r','marker':'old'}],
            award_lot=[{'award_lot_id':'lot','revision_id':'r','marker':'old'}]),
        files(tmp_path, 1,
            plan_revision=[{'revision_id':'p','marker':'new1'},{'revision_id':'p','marker':'new2'}],
            result_revision=[{'revision_id':'r','marker':'empty children'}]),
    ]
    results=[]
    for flag in ('true','false'):
        with duckdb.connect(config={'memory_limit':'256MB','threads':1}) as db:
            db.execute('SET preserve_insertion_order='+flag)
            selected=create_views(db,entries)['views']
            results.append({n:db.execute('SELECT * FROM '+v+' ORDER BY ALL').fetchall() for n,v in selected.items()})
    assert results[0]==results[1]
    assert results[1]['award_lot']==[]
    assert len(results[1]['plan_revision'])==2
