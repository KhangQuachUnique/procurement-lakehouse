"""Monthly publication must clear obsolete children and preserve unrelated rows."""

import importlib.util
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

spec = importlib.util.spec_from_file_location(
    "silver_staged", Path(__file__).parents[3] / "scripts/silver_staged.py"
)
staged = importlib.util.module_from_spec(spec)
spec.loader.exec_module(staged)


def test_readback_does_not_accept_metadata_counts_when_files_are_missing(monkeypatch):
    class Connection:
        def execute(self, sql):
            assert "AT (VERSION => 7)" in sql
            assert "hash(" in sql
            raise OSError("Missing manifest/data file")

    class Catalog:
        def table(self, *_):
            return {"metadata": {"current-snapshot-id": 7}}

    @contextmanager
    def connection(*_, **__):
        yield Connection()

    @contextmanager
    def catalog(*_):
        yield Catalog()

    monkeypatch.setattr(staged, "connect", connection)
    monkeypatch.setattr(staged, "RestCatalog", catalog)
    with pytest.raises(OSError, match="Missing manifest/data file"):
        staged.verify_synced_tables(object())


@pytest.fixture
def db(monkeypatch):
    connection = duckdb.connect()
    connection.execute("ATTACH ':memory:' AS lake")
    connection.execute("CREATE SCHEMA lake.silver")
    schemas = {
        "observation": "observation_id VARCHAR, payload VARCHAR",
        "result_revision": "revision_id VARCHAR, observation_id VARCHAR, payload VARCHAR",
        "award_lot": "lot_id VARCHAR, revision_id VARCHAR, payload VARCHAR",
        "award_contractor": "contractor_id VARCHAR, revision_id VARCHAR, payload VARCHAR",
        "quality_issue": "issue_id VARCHAR, observation_id VARCHAR, payload VARCHAR",
        "quarantine": "observation_id VARCHAR, payload VARCHAR",
    }
    for name, schema in schemas.items():
        connection.execute(f"CREATE TABLE lake.silver.{name} ({schema})")
        connection.execute(f"CREATE TEMP TABLE src_{name} ({schema})")
    monkeypatch.setattr(
        staged,
        "columns",
        lambda name: ",".join(
            row[0] for row in connection.execute("DESCRIBE lake.silver." + name).fetchall()
        ),
    )
    connection.execute(
        "INSERT INTO lake.silver.observation VALUES ('o1','immutable'),('other','2026')"
    )
    connection.execute(
        "INSERT INTO lake.silver.result_revision VALUES ('old','o1','old logic'),('other','other','2026')"
    )
    connection.execute(
        "INSERT INTO lake.silver.award_lot VALUES ('stale','old','old logic'),('keep','other','2026')"
    )
    connection.execute(
        "INSERT INTO lake.silver.award_contractor VALUES ('stale','old','old logic'),('keep','other','2026')"
    )
    connection.execute(
        "INSERT INTO lake.silver.quality_issue VALUES ('stale','o1','old logic'),('keep','other','2026')"
    )
    connection.execute("INSERT INTO src_observation VALUES ('o1','immutable'),('o2','new source')")
    connection.execute(
        "INSERT INTO src_result_revision VALUES ('new','o1','new logic'),('new2','o2','new logic')"
    )
    yield connection
    connection.close()


def contents(db):
    return {
        name: db.execute("SELECT * FROM lake.silver." + name + " ORDER BY ALL").fetchall()
        for name in (*staged.DOMAIN, *staged.GOV)
    }


def test_month_replaces_obsolete_children_preserves_unrelated_and_replays(db):
    staged.sync_month(db)
    first = contents(db)
    assert first["award_lot"] == [("keep", "other", "2026")]
    assert first["award_contractor"] == [("keep", "other", "2026")]
    assert first["quality_issue"] == [("keep", "other", "2026")]
    assert ("other", "other", "2026") in first["result_revision"]
    assert len(first["observation"]) == 3
    staged.sync_month(db)
    assert contents(db) == first


def test_failure_rolls_back_deletes_and_inserts(db):
    before = contents(db)
    db.execute("ALTER TABLE src_award_contractor DROP COLUMN payload")
    with pytest.raises(duckdb.Error):
        staged.sync_month(db)
    assert contents(db) == before


def test_result_governance_does_not_overwrite_other_resources():
    from procurement.silver.application.resources import get_silver_resource

    groups = get_silver_resource("result").candidate_groups("bucket/silver")
    paths = {name: prefix for prefix, names in groups for name in names}
    assert paths["result_revision"] == "bucket/silver"
    assert paths["observation"] == "bucket/silver/_governance/contractor_result"
    assert (
        paths["quality_issue"]
        != get_silver_resource("project").candidate_groups("bucket/silver")[0][0]
    )


def test_parquet_only_uses_committed_days_and_disables_iceberg(monkeypatch, tmp_path):
    from datetime import date
    from types import SimpleNamespace

    import procurement.silver.adapters.driven.bronze.arrow_adapter as bronze
    from procurement.infrastructure import database

    monkeypatch.setattr(staged, "ROOT", tmp_path)
    monkeypatch.setattr(staged, "START", date(2025, 1, 1))
    monkeypatch.setattr(staged, "END", date(2025, 1, 3))
    monkeypatch.setattr(staged, "get_storage", lambda: ("fs", "bucket"))
    monkeypatch.setattr(
        database, "create_application_engine", lambda: SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(
        bronze,
        "ArrowBronzeRepositoryAdapter",
        lambda **kwargs: SimpleNamespace(
            select_committed_partitions=lambda *args: [
                SimpleNamespace(source_date=day) for day in ("2025-01-01", "2025-01-03")
            ]
        ),
    )
    calls = []

    def process(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "completed"}

    monkeypatch.setattr(staged, "execute_resource", process)
    staged.parquet()
    assert len(calls) == 2
    assert all(kwargs["iceberg"] is False for _, kwargs in calls)
    assert [(args[3], args[4]) for args, _ in calls] == [
        (date(2025, 1, 1), date(2025, 1, 1)),
        (date(2025, 1, 3), date(2025, 1, 3)),
    ]
    import json

    report = json.loads((tmp_path / "parquet.json").read_text())
    assert report["status"] == "completed"
    assert report["missing_bronze_days"] == ["2025-01-02"]
