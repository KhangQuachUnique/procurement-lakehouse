import json

import duckdb
import pyarrow as pa
import pytest

from procurement.silver.application.change_journal import chain, record, replay


def rows(values):
    return pa.table(
        {
            "id": pa.array([v[0] for v in values], type=pa.string()),
            "parent": pa.array([v[1] for v in values], type=pa.string()),
        }
    )


def test_new_update_delete_old_new_relationships_and_nonordered_snapshot_ids(tmp_path):
    record(
        tmp_path,
        "silver",
        "package_revision",
        99,
        11,
        "uuid",
        rows([("a", "old"), ("gone", "p")]),
        rows([("a", "new"), ("b", "new")]),
    )
    record(
        tmp_path,
        "silver",
        "package_revision",
        11,
        72,
        "uuid",
        rows([("b", "new")]),
        rows([("b", "newer")]),
    )
    events = chain(tmp_path, "silver", "package_revision", "uuid", 99, 72)
    with duckdb.connect() as db:
        db.execute("CREATE TABLE cached AS SELECT 'a' id,'old' parent UNION ALL SELECT 'gone','p'")
        assert replay(db, "cached", events, "incoming") == 6
        assert db.execute("SELECT * FROM incoming ORDER BY id").fetchall() == [
            ("a", "new"),
            ("b", "newer"),
        ]
        # Replaying from the same immutable baseline is idempotent.
        replay(db, "cached", events, "incoming")
        assert db.execute("SELECT count(*) FROM incoming").fetchone()[0] == 2
        db.execute("DELETE FROM cached WHERE id='a'")
        with pytest.raises(ValueError, match="before image"):
            replay(db, "cached", events, "incoming")


def test_gaps_wrong_identity_corruption_and_staging_not_silently_accepted(tmp_path):
    event = record(tmp_path, "silver", "observation", 1, 2, "uuid", rows([]), rows([("a", "p")]))
    for identity, before, after in (("different", 1, 2), ("uuid", 0, 2), ("uuid", 1, 3)):
        with pytest.raises(ValueError, match="Missing/non-contiguous"):
            chain(tmp_path, "silver", "observation", identity, before, after)
    (event / "new.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="integrity"):
        chain(tmp_path, "silver", "observation", "uuid", 1, 2)
    staging = tmp_path / "silver" / "observation" / "staging_incomplete"
    staging.mkdir()
    (staging / "receipt.json").write_text(json.dumps({"ignored": True}))
    assert chain(tmp_path, "silver", "observation", "uuid", 2, 2) == []




def test_event_identity_and_explicit_old_new_keys_are_idempotent(tmp_path):
    old=pa.table({'revision_id':['changed','gone'],'entity_id':['old_entity','deleted_entity']})
    new=pa.table({'revision_id':['changed','new'],'entity_id':['new_entity','inserted_entity']})
    event=record(tmp_path,'silver','package_revision',1,2,'uuid',old,new)
    assert record(tmp_path,'silver','package_revision',1,2,'uuid',old,new)==event
    receipt=json.loads((event/'receipt.json').read_text())
    changes={r['key']:r for r in receipt['changes']}
    assert changes['changed']['type']=='UPDATE'
    assert changes['changed']['old_relationships']['entity_id']=='old_entity'
    assert changes['changed']['new_relationships']['entity_id']=='new_entity'
    assert changes['gone']['type']=='DELETE'
    assert changes['new']['type']=='INSERT'
    assert len(chain(tmp_path,'silver','package_revision','uuid',1,2))==1
