"""Real SeaweedFS + DuckDB contract, opt in with ICEBERG_TEST=1."""

import os
from decimal import Decimal
from uuid import uuid4

import pytest

from procurement.storage.iceberg import IcebergConfig, RestCatalog, connect

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv("ICEBERG_TEST") != "1", reason="requires isolated Iceberg test stack")]


def test_v2_crud_merge_evolution_pinned_time_travel_and_restart():
    config = IcebergConfig("http://localhost:18181", "s3://warehouse", "http://localhost:18333",
                           "integration", "integration-test-only")
    catalog = RestCatalog(config)
    name = "proof_" + uuid4().hex[:12]
    table = "lake.poc." + name
    with connect(config, install=True) as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS lake.poc")
        db.execute(f"CREATE TABLE {table} (id INTEGER, value VARCHAR)")
        db.execute(f"INSERT INTO {table} VALUES (1,'one'),(2,'two')")
        metadata = catalog.table("poc", name)["metadata"]
        assert metadata["format-version"] == 2
        original = metadata["current-snapshot-id"]
        catalog.pin("poc", name, original, name)
        db.execute(f"""MERGE INTO {table} t USING (VALUES (2,'changed'),(3,'three')) s(id,value)
            ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value
            WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.value)""")
        db.execute(f"ALTER TABLE {table} ADD COLUMN amount DECIMAL(38,6)")
        db.execute(f"UPDATE {table} SET amount=12.345678 WHERE id=2")
        db.execute(f"DELETE FROM {table} WHERE id=1")
    # New connection proves catalog persistence and recovery independent of client state.
    with connect(config) as db:
        assert db.sql(f"SELECT * FROM {table} ORDER BY id").fetchall() == [
            (2, "changed", Decimal("12.345678")), (3, "three", None)]
        assert db.sql(f"SELECT id,value FROM {table} AT (VERSION => {original}) ORDER BY id").fetchall() == [
            (1, "one"), (2, "two")]
        db.execute("BEGIN")
        db.execute(f"INSERT INTO {table} (id,value) VALUES (4,'uncommitted')")
        db.execute("ROLLBACK")
        assert db.sql(f"SELECT count(*) FROM {table}").fetchone()[0] == 2
    assert catalog.table("poc", name)["metadata"]["refs"]["release_" + name]["snapshot-id"] == original
