import json
from datetime import date, timedelta

import duckdb
import pytest

from procurement.tools.silver_partitioned_insert import load_partitioned


class DB:
    def __init__(self, raw, cat):
        self.raw, self.cat, self.inserts = raw, cat, 0

    def execute(self, sql, *args):
        result = self.raw.execute(sql, *args)
        if sql.startswith("INSERT INTO"):
            self.inserts += 1
        if sql == "COMMIT":
            parent = self.cat.sid
            self.cat.sid = (parent or 0) + 1
            self.cat.snapshots.append({"snapshot-id": self.cat.sid, "parent-snapshot-id": parent})
        return result


class Catalog:
    def __init__(self):
        self.sid = None
        self.snapshots = []

    def table(self, *_):
        return {
            "metadata": {
                "table-uuid": "same",
                "current-snapshot-id": self.sid,
                "snapshots": self.snapshots.copy(),
            }
        }


@pytest.fixture
def source():
    raw = duckdb.connect(config={"threads": 1, "memory_limit": "256MB"})
    raw.execute("ATTACH ':memory:' AS lake")
    raw.execute("CREATE SCHEMA lake.silver_v2_test")
    raw.execute(
        "CREATE TABLE lake.silver_v2_test.notice_revision(day DATE,revision_id INTEGER,v VARCHAR)"
    )
    raw.execute("CREATE TABLE expected(day DATE,revision_id INTEGER,v VARCHAR)")
    for i in range(19):
        raw.execute(
            "INSERT INTO expected VALUES (?,?,?)",
            [date(2022, 1, 1) + timedelta(days=i), i, "v" + str(i)],
        )
    raw.execute("INSERT INTO expected VALUES (NULL,99,'null'),('2022-01-01',0,'v0')")
    cat = Catalog()
    db = DB(raw, cat)
    yield db, cat
    raw.close()


def call(db, cat, path, **kwargs):
    return load_partitioned(
        db, cat, "silver_v2_test", "notice_revision", "expected", path, "frozen", "day", **kwargs
    )


def test_chunk_ack_loss_resumes_exactly_including_duplicates_nulls_and_noop(source, tmp_path):
    db, cat = source

    def fail(i, phase):
        if i == 1 and phase == "committed_before_ack":
            raise RuntimeError("lost ACK")

    with pytest.raises(RuntimeError, match="lost ACK"):
        call(db, cat, tmp_path, after_chunk=fail)
    before = db.inserts
    result = call(db, cat, tmp_path)
    assert result["chunks"] == 4 and result["rows"] == 21
    assert db.inserts == before + 2
    assert json.loads((tmp_path / "00001.json").read_text())["reconciled_ack_loss"] is True
    assert (
        db.raw.execute("SELECT * FROM expected ORDER BY ALL").fetchall()
        == db.raw.execute(
            "SELECT * FROM lake.silver_v2_test.notice_revision ORDER BY ALL"
        ).fetchall()
    )
    final = db.inserts
    assert call(db, cat, tmp_path) == result
    assert db.inserts == final


def test_unknown_nonempty_table_is_refused_and_changed_plan_rejected(source, tmp_path):
    db, cat = source
    cat.sid = 10
    with pytest.raises(ValueError, match="Unknown nonempty"):
        call(db, cat, tmp_path)
    cat.sid = None
    call(db, cat, tmp_path)
    with pytest.raises(ValueError, match="plan changed"):
        call(db, cat, tmp_path, partitions_per_insert=4)


def test_uncertain_wrong_rows_fail_closed_before_any_more_appends(source, tmp_path):
    db, cat = source

    def fail(i, phase):
        if phase == "committed_before_ack":
            raise RuntimeError("lost ACK")

    with pytest.raises(RuntimeError):
        call(db, cat, tmp_path, after_chunk=fail)
    db.raw.execute("UPDATE lake.silver_v2_test.notice_revision SET v='wrong'")
    before = db.inserts
    with pytest.raises(ValueError, match="differs"):
        call(db, cat, tmp_path)
    assert db.inserts == before


def test_hot_partition_guard_blocks_before_commit(source, tmp_path):
    db, cat = source
    with pytest.raises(ValueError, match="row guard"):
        call(db, cat, tmp_path, max_rows=1)
    # NULL chunk may commit; the next physical day range must not partially write.
    assert (
        db.raw.execute(
            "SELECT count(*) FROM lake.silver_v2_test.notice_revision WHERE day IS NOT NULL"
        ).fetchone()[0]
        == 0
    )


def test_uncertain_commit_also_checks_earlier_prefix_not_just_total_counts(source, tmp_path):
    db, cat = source

    def fail(i, phase):
        if i == 1 and phase == "committed_before_ack":
            raise RuntimeError("lost ACK")

    with pytest.raises(RuntimeError):
        call(db, cat, tmp_path, after_chunk=fail)
    db.raw.execute(
        "UPDATE lake.silver_v2_test.notice_revision SET v='corrupt old prefix' WHERE day=DATE '2022-01-01'"
    )
    before = db.inserts
    with pytest.raises(ValueError, match="differs"):
        call(db, cat, tmp_path)
    assert db.inserts == before


def test_hot_null_partition_splits_by_whole_source_keys_with_row_bound(source, tmp_path):
    db, cat = source
    db.raw.execute("INSERT INTO expected SELECT NULL,1000+i,'hot' FROM range(510) t(i)")
    result = call(db, cat, tmp_path, max_rows=100)
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert max(plan["rows"]) <= 100
    assert result["rows"] == 531
    assert (
        db.raw.execute("SELECT * FROM expected ORDER BY ALL").fetchall()
        == db.raw.execute(
            "SELECT * FROM lake.silver_v2_test.notice_revision ORDER BY ALL"
        ).fetchall()
    )


def test_empty_loader_integration_progress_ack_and_rerun(source, tmp_path, monkeypatch):
    from procurement.tools import silver_bootstrap_writer as writer

    db, cat = source
    db.raw.execute(
        "ALTER TABLE lake.silver_v2_test.notice_revision RENAME COLUMN day TO public_date"
    )
    db.raw.execute("CREATE VIEW selected AS SELECT day public_date,revision_id,v FROM expected")
    monkeypatch.setattr(
        writer,
        "to_iceberg_ddl",
        lambda *a, **k: (
            "CREATE TABLE IF NOT EXISTS lake.silver_v2_test.notice_revision(public_date DATE,revision_id INTEGER,v VARCHAR)"
        ),
    )
    original = cat.table

    def metadata(*args):
        result = original(*args)
        result["metadata"]["format-version"] = 2
        return result

    monkeypatch.setattr(cat, "table", metadata)
    progress = []
    kwargs = {
        "allow_managed_namespace": True,
        "partition_batch_size": 8,
        "after_partition_chunk": lambda *args: progress.append(args),
    }
    pins = writer.load_empty(
        db, cat, "silver_v2_test", {"notice_revision": "selected"}, tmp_path, "frozen", **kwargs
    )
    assert pins["notice_revision"]["chunks"] == 4
    assert progress[-1] == ("notice_revision", 4, 4)
    inserted = db.inserts
    assert (
        writer.load_empty(
            db, cat, "silver_v2_test", {"notice_revision": "selected"}, tmp_path, "frozen", **kwargs
        )
        == pins
    )
    assert db.inserts == inserted
