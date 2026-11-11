"""Unit tests for Iceberg DDL generation and partition specifications."""

import pytest

from procurement.silver.application.warehouse import (
    get_partition_spec,
    to_iceberg_ddl,
)
from procurement.silver.domain.schema import (
    SILVER_PARTITION_SPECS,
    get_silver_schemas,
)


def test_silver_partition_specs_defined_for_core_entities():
    """Verify that all core revision tables and observation table have partition specs."""
    # Tier 1
    assert SILVER_PARTITION_SPECS.get("observation") == "source_date"

    # Tier 2 Core Entities
    for entity in (
        "notice_revision",
        "project_revision",
        "plan_revision",
        "result_revision",
        "opening_revision",
        "package_revision",
    ):
        spec = get_partition_spec(entity)
        assert spec == "public_date", f"Expected public_date partition spec for {entity}"


def test_get_partition_spec_unpartitioned_tables():
    """Verify that governance/sub-entity tables without explicit partition return None."""
    assert get_partition_spec("quality_issue") is None
    assert get_partition_spec("quarantine") is None
    assert get_partition_spec("unknown_table") is None


def test_to_iceberg_ddl_generation_partitioned():
    """Verify to_iceberg_ddl generates complete SQL with PARTITIONED BY clause."""
    ddl = to_iceberg_ddl("notice_revision", namespace="silver")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS lake.silver.notice_revision (")
    assert ddl.endswith(") PARTITIONED BY (public_date)")
    assert "revision_id VARCHAR" in ddl
    assert "public_date DATE" in ddl
    assert "DECIMAL(38,6)" in ddl
    assert "TIMESTAMPTZ" in ddl


def test_to_iceberg_ddl_generation_unpartitioned():
    """Verify unpartitioned tables do not include PARTITIONED BY."""
    ddl = to_iceberg_ddl("quality_issue", namespace="silver")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS lake.silver.quality_issue (")
    assert "PARTITIONED BY" not in ddl
    assert "issue_id VARCHAR" in ddl


def test_to_iceberg_ddl_custom_namespace():
    """Verify custom namespace parameter propagates into lake.{namespace}.{table}."""
    ddl = to_iceberg_ddl("project_revision", namespace="custom_lake")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS lake.custom_lake.project_revision (")
    assert "PARTITIONED BY (public_date)" in ddl


def test_to_iceberg_ddl_unknown_table_raises_error():
    """Verify unknown table name without provided schema raises ValueError."""
    with pytest.raises(ValueError, match="Unknown silver table schema"):
        to_iceberg_ddl("non_existent_table")


def test_all_silver_schemas_generate_valid_iceberg_ddl():
    """Verify every table defined in get_silver_schemas() produces a non-empty Iceberg DDL."""
    schemas = get_silver_schemas()
    assert len(schemas) >= 15
    for table_name in schemas:
        ddl = to_iceberg_ddl(table_name, namespace="silver")
        assert f"lake.silver.{table_name}" in ddl
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS ")


def test_get_primary_key_resolution():
    """Verify primary key columns are correctly discovered across entity tiers."""
    from procurement.silver.domain.schema import get_primary_key

    assert get_primary_key("notice_revision") == "revision_id"
    assert get_primary_key("project_revision") == "revision_id"
    assert get_primary_key("plan_revision") == "revision_id"
    assert get_primary_key("observation") == "observation_id"
    assert get_primary_key("quality_issue") == "issue_id"
    assert get_primary_key("quarantine") == "observation_id"
    assert get_primary_key("non_existent_table") is None


def test_stage_partition_executes_dynamic_partition_overwrite():
    """Verify stage_partition executes targeted partition deletes without touching other days."""
    import datetime
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import stage_partition

    mock_db = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.table.return_value = {
        "metadata": {
            "format-version": 2,
            "current-snapshot-id": 101,
        }
    }
    mock_db.sql.return_value.fetchone.return_value = [5]

    executed_sql = []

    def _mock_execute(sql):
        executed_sql.append(sql)
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, "raw_json", "VARCHAR"),
            (1, "bid_result", "BIGINT"),
            (2, "price_source", "VARCHAR"),
            (3, "currency", "VARCHAR"),
        ]
        return cursor

    mock_db.execute.side_effect = _mock_execute

    candidate_rows = [
        {"revision_id": "r1", "entity_id": "IB1", "public_date": datetime.date(2024, 5, 1)},
        {"revision_id": "r2", "entity_id": "IB2", "public_date": datetime.date(2024, 5, 2)},
    ]

    snapshots = stage_partition(
        mock_db,
        mock_catalog,
        namespace="silver",
        tables={"notice_revision": candidate_rows},
        partition_date=datetime.date(2024, 5, 1),
        release_id="test_rel",
    )

    assert "notice_revision" in snapshots
    assert snapshots["notice_revision"]["snapshot_id"] == 101
    assert snapshots["notice_revision"]["row_count"] == 5

    # Verify DDL was created with partition
    assert any("PARTITIONED BY (public_date)" in sql for sql in executed_sql)
    # Verify transaction bounds
    assert "BEGIN" in executed_sql
    assert "COMMIT" in executed_sql
    # Verify True Key-Level UPSERT delete was executed on primary key
    assert any(
        "DELETE FROM lake.silver.notice_revision WHERE revision_id IN (SELECT revision_id FROM _incoming_cand)"
        in sql
        for sql in executed_sql
    )
    # Verify insert was executed
    assert any("INSERT INTO lake.silver.notice_revision BY NAME" in sql for sql in executed_sql)
    # Verify catalog was pinned
    mock_catalog.pin.assert_called_once_with("silver", "notice_revision", 101, "test_rel")
    # Verify views were ensured
    assert any("CREATE OR REPLACE VIEW silver.v_latest_notice" in sql for sql in executed_sql)


def test_stage_partition_unpartitioned_uses_primary_key_subquery():
    """Verify stage_partition on unpartitioned tables deletes only matching incoming primary keys."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import stage_partition

    mock_db = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.table.return_value = {
        "metadata": {
            "format-version": 2,
            "current-snapshot-id": 202,
        }
    }
    mock_db.sql.return_value.fetchone.return_value = [2]

    executed_sql = []

    def _mock_execute(sql):
        executed_sql.append(sql)
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, "raw_json", "VARCHAR"),
            (1, "bid_result", "BIGINT"),
            (2, "price_source", "VARCHAR"),
            (3, "currency", "VARCHAR"),
        ]
        return cursor

    mock_db.execute.side_effect = _mock_execute

    issues = [
        {"issue_id": "iss-1", "code": "E01", "path": "p", "severity": "WARN"},
        {"issue_id": "iss-2", "code": "E02", "path": "p", "severity": "INFO"},
    ]

    snapshots = stage_partition(
        mock_db,
        mock_catalog,
        namespace="silver",
        tables={"quality_issue": issues},
    )

    assert "quality_issue" in snapshots
    # Verify subquery delete on primary key
    assert any(
        "DELETE FROM lake.silver.quality_issue WHERE issue_id IN (SELECT issue_id FROM _incoming_cand)"
        in sql
        for sql in executed_sql
    )


def test_stage_partition_rolls_back_on_error():
    """Verify stage_partition rolls back DuckDB transaction when an exception occurs."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import stage_partition

    mock_db = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.table.return_value = {
        "metadata": {
            "format-version": 2,
            "current-snapshot-id": 303,
        }
    }

    executed_sql = []

    def failing_execute(sql):
        executed_sql.append(sql)
        if "INSERT INTO" in sql:
            raise RuntimeError("DuckDB connection error during write")

    mock_db.execute.side_effect = failing_execute

    with pytest.raises(RuntimeError, match="DuckDB connection error during write"):
        stage_partition(
            mock_db,
            mock_catalog,
            namespace="silver",
            tables={"observation": [{"observation_id": "obs1", "source_date": "2024-05-01"}]},
        )

    assert "BEGIN" in executed_sql
    assert "ROLLBACK" in executed_sql
    assert "COMMIT" not in executed_sql


def test_ensure_views_generation():
    """Verify ensure_views correctly constructs window function queries for all core views."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import ensure_views

    mock_db = MagicMock()
    executed_sql = []

    def _mock_execute(sql):
        executed_sql.append(sql)
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, "raw_json", "VARCHAR"),
            (1, "bid_result", "BIGINT"),
            (2, "price_source", "VARCHAR"),
            (3, "currency", "VARCHAR"),
        ]
        return cursor

    mock_db.execute.side_effect = _mock_execute

    views = ensure_views(mock_db, namespace="silver", view_schema="custom_schema")

    assert len(views) == 10
    assert "custom_schema.v_latest_notice" in views
    assert "custom_schema.v_latest_project" in views
    assert "custom_schema.v_latest_plan" in views
    assert "custom_schema.v_latest_package" in views
    assert "custom_schema.v_latest_result" in views
    assert "custom_schema.v_latest_opening" in views

    # Check SQL syntax for v_latest_notice
    notice_sql = next(s for s in executed_sql if "v_latest_notice" in s)
    assert "ROW_NUMBER() OVER" in notice_sql
    assert "PARTITION BY entity_id" in notice_sql
    assert "ORDER BY notify_version DESC NULLS LAST" in notice_sql
    assert "FROM lake.silver.notice_revision" in notice_sql
    assert "WHERE rn = 1" in notice_sql


def test_stage_partition_deletes_stale_award_contractor_children_when_new_set_empty():
    """Verify stage_partition deletes award_contractor children for replaced revisions even when new set empty."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import stage_partition

    mock_db = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.table.return_value = {
        "metadata": {
            "format-version": 2,
            "current-snapshot-id": 501,
            "snapshots": [{"snapshot-id": 501, "summary": {"total-records": "0"}}],
        }
    }

    executed_sql = []
    registered_tables = {}

    def _mock_execute(sql):
        executed_sql.append(sql)
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, "raw_json", "VARCHAR"),
            (1, "bid_result", "BIGINT"),
            (2, "price_source", "VARCHAR"),
            (3, "currency", "VARCHAR"),
        ]
        return cursor

    mock_db.execute.side_effect = _mock_execute
    mock_db.register.side_effect = lambda name, tbl: registered_tables.update({name: tbl})

    # Replaced revision with zero winners in new set
    tables = {
        "result_revision": [{"revision_id": "rev-res-1", "entity_id": "ent-1"}],
        "award_contractor": [],
    }

    snapshots = stage_partition(
        mock_db,
        mock_catalog,
        namespace="silver",
        tables=tables,
    )

    assert "award_contractor" in snapshots
    # Verify candidate revision IDs registered
    assert "_cand_result_rids" in registered_tables
    rids = registered_tables["_cand_result_rids"]["revision_id"].to_pylist()
    assert rids == ["rev-res-1"]

    # Verify targeted revision deletion executed on award_contractor
    delete_sql = next(
        s
        for s in executed_sql
        if "DELETE FROM lake.silver.award_contractor WHERE revision_id IN" in s
    )
    assert "SELECT revision_id FROM _cand_result_rids" in delete_sql

    # Verify no INSERT was executed for award_contractor because candidate is empty
    assert not any("INSERT INTO lake.silver.award_contractor" in s for s in executed_sql)


def test_stage_partition_award_contractor_preserves_unrelated_revisions():
    """Verify award_contractor replacement deletes only matching revision IDs and inserts new records."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import stage_partition

    mock_db = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.table.return_value = {
        "metadata": {
            "format-version": 2,
            "current-snapshot-id": 502,
            "snapshots": [{"snapshot-id": 502, "summary": {"total-records": "1"}}],
        }
    }

    executed_sql = []
    registered_tables = {}

    def _mock_execute(sql):
        executed_sql.append(sql)
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, "raw_json", "VARCHAR"),
            (1, "bid_result", "BIGINT"),
            (2, "price_source", "VARCHAR"),
            (3, "currency", "VARCHAR"),
        ]
        return cursor

    mock_db.execute.side_effect = _mock_execute
    mock_db.register.side_effect = lambda name, tbl: registered_tables.update({name: tbl})

    candidate_contractors = [
        {
            "award_contractor_id": "c1",
            "award_lot_id": "lot1",
            "revision_id": "rev-res-1",
            "contractor_code": "vn001",
            "contractor_name": "Nhà thầu 1",
            "is_consortium": False,
            "awarded_price": 5000000,
        }
    ]

    tables = {
        "result_revision": [{"revision_id": "rev-res-1", "entity_id": "ent-1"}],
        "award_contractor": candidate_contractors,
    }

    snapshots = stage_partition(
        mock_db,
        mock_catalog,
        namespace="silver",
        tables=tables,
    )

    assert "award_contractor" in snapshots
    assert any(
        "DELETE FROM lake.silver.award_contractor WHERE revision_id IN" in s for s in executed_sql
    )
    assert any("INSERT INTO lake.silver.award_contractor BY NAME" in s for s in executed_sql)


def test_stateful_cleanup_real_duckdb_removes_obsolete_losers_and_preserves_unrelated():
    """Real DuckDB stateful test: verify obsolete losers removed, 0-winner replacement and unrelated retained."""
    import re

    import duckdb

    from procurement.silver.application.warehouse import stage_partition

    class LocalDuckDBShim:
        def __init__(self):
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("ATTACH ':memory:' AS lake")

        def execute(self, sql):
            if "PARTITIONED BY" in sql:
                sql = sql.split(" PARTITIONED BY ")[0]
            return self.conn.execute(sql)

        def sql(self, query):
            cleaned_query = re.sub(
                r"\s+AT\s*\(\s*VERSION\s*=>\s*\d+\s*\)", "", query, flags=re.IGNORECASE
            )
            return self.conn.sql(cleaned_query)

        def register(self, name, table):
            return self.conn.register(name, table)

        def unregister(self, name):
            return self.conn.unregister(name)

    class LocalCatalogShim:
        def __init__(self, db_shim):
            self.db_shim = db_shim

        def table(self, namespace, name):
            import contextlib

            count = 0
            with contextlib.suppress(Exception):
                count = self.db_shim.conn.sql(
                    f"SELECT count(*) FROM lake.{namespace}.{name}"
                ).fetchone()[0]
            return {
                "metadata": {
                    "format-version": 2,
                    "current-snapshot-id": 1,
                    "snapshots": [{"snapshot-id": 1, "summary": {"total-records": str(count)}}],
                }
            }

        def pin(self, namespace, name, snapshot_id, release_id):
            pass

    db = LocalDuckDBShim()
    cat = LocalCatalogShim(db)

    # 1. Initial state: rev-1 has winner c1 and obsolete loser c2; rev-2 has winner c3 (unrelated)
    cand_initial = [
        {
            "award_contractor_id": "c1",
            "award_lot_id": "l1",
            "revision_id": "rev-1",
            "contractor_code": "v1",
            "contractor_name": "N1",
            "is_consortium": False,
            "awarded_price": 100,
        },
        {
            "award_contractor_id": "c2",
            "award_lot_id": "l1",
            "revision_id": "rev-1",
            "contractor_code": "v2",
            "contractor_name": "N2",
            "is_consortium": False,
            "awarded_price": 200,
        },
        {
            "award_contractor_id": "c3",
            "award_lot_id": "l2",
            "revision_id": "rev-2",
            "contractor_code": "v3",
            "contractor_name": "N3",
            "is_consortium": False,
            "awarded_price": 300,
        },
    ]
    stage_partition(db, cat, "silver", {"award_contractor": cand_initial})

    rows_initial = db.conn.execute(
        "SELECT award_contractor_id, revision_id FROM lake.silver.award_contractor ORDER BY award_contractor_id"
    ).fetchall()
    assert rows_initial == [("c1", "rev-1"), ("c2", "rev-1"), ("c3", "rev-2")]

    # 2. Rerun rev-1 with ZERO winners: must delete both c1 and c2 from rev-1, and preserve unrelated rev-2
    stage_partition(
        db, cat, "silver", {"result_revision": [{"revision_id": "rev-1"}], "award_contractor": []}
    )
    rows_after_zero = db.conn.execute(
        "SELECT award_contractor_id, revision_id FROM lake.silver.award_contractor ORDER BY award_contractor_id"
    ).fetchall()
    assert rows_after_zero == [("c3", "rev-2")]

    # 3. Rerun rev-1 with winner c1 only (c2 was obsolete loser): must insert c1, leave c2 removed, and keep rev-2
    cand_rev1 = [
        {
            "award_contractor_id": "c1",
            "award_lot_id": "l1",
            "revision_id": "rev-1",
            "contractor_code": "v1",
            "contractor_name": "N1",
            "is_consortium": False,
            "awarded_price": 100,
        },
    ]
    stage_partition(
        db,
        cat,
        "silver",
        {"result_revision": [{"revision_id": "rev-1"}], "award_contractor": cand_rev1},
    )
    rows_final = db.conn.execute(
        "SELECT award_contractor_id, revision_id FROM lake.silver.award_contractor ORDER BY award_contractor_id"
    ).fetchall()
    assert rows_final == [("c1", "rev-1"), ("c3", "rev-2")]


def test_real_duckdb_schema_migration_and_idempotency():
    """Real DuckDB test: verify explicit schema migration for award_lot/award_contractor and idempotent rerun."""
    import re

    import duckdb

    from procurement.silver.application.warehouse import stage_partition

    class LocalDuckDBShim:
        def __init__(self):
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("ATTACH ':memory:' AS lake")

        def execute(self, sql):
            if "PARTITIONED BY" in sql:
                sql = sql.split(" PARTITIONED BY ")[0]
            return self.conn.execute(sql)

        def sql(self, query):
            cleaned_query = re.sub(
                r"\s+AT\s*\(\s*VERSION\s*=>\s*\d+\s*\)", "", query, flags=re.IGNORECASE
            )
            return self.conn.sql(cleaned_query)

        def register(self, name, table):
            return self.conn.register(name, table)

        def unregister(self, name):
            return self.conn.unregister(name)

    class LocalCatalogShim:
        def __init__(self, db_shim):
            self.db_shim = db_shim

        def table(self, namespace, name):
            import contextlib

            count = 0
            with contextlib.suppress(Exception):
                count = self.db_shim.conn.sql(
                    f"SELECT count(*) FROM lake.{namespace}.{name}"
                ).fetchone()[0]
            return {
                "metadata": {
                    "format-version": 2,
                    "current-snapshot-id": 1,
                    "snapshots": [{"snapshot-id": 1, "summary": {"total-records": str(count)}}],
                }
            }

        def pin(self, namespace, name, snapshot_id, release_id):
            pass

    db = LocalDuckDBShim()
    cat = LocalCatalogShim(db)

    # 1. Pre-create old-schema tables without new columns
    db.conn.execute("CREATE SCHEMA lake.silver")
    db.conn.execute(
        "CREATE TABLE lake.silver.award_lot (award_lot_id VARCHAR, revision_id VARCHAR, lot_no VARCHAR)"
    )
    db.conn.execute("INSERT INTO lake.silver.award_lot VALUES ('lot_old', 'rev_old', '1')")

    db.conn.execute(
        "CREATE TABLE lake.silver.award_contractor (award_contractor_id VARCHAR, award_lot_id VARCHAR, revision_id VARCHAR, contractor_code VARCHAR, contractor_name VARCHAR, is_consortium BOOLEAN)"
    )
    db.conn.execute(
        "INSERT INTO lake.silver.award_contractor VALUES ('c_old', 'lot_old', 'rev_old', 'vn0', 'Old Contractor', False)"
    )

    # 2. Stage candidate rows containing the new columns
    new_lots = [
        {
            "award_lot_id": "lot_new",
            "revision_id": "rev_new",
            "lot_no": "2",
            "source_lot_id": "slot_123",
        }
    ]
    new_contractors = [
        {
            "award_contractor_id": "c_new",
            "award_lot_id": "lot_new",
            "revision_id": "rev_new",
            "contractor_code": "vn1",
            "contractor_name": "New Contractor",
            "is_consortium": False,
            "bid_result": 1,
            "price_source": "bidWiningPrice",
            "currency": "VND",
        }
    ]

    stage_partition(db, cat, "silver", {"award_lot": new_lots, "award_contractor": new_contractors})

    # Verify columns were added via ALTER TABLE ADD COLUMN
    lot_cols = [
        c[1] for c in db.conn.execute("PRAGMA table_info('lake.silver.award_lot')").fetchall()
    ]
    assert "source_lot_id" in lot_cols

    contractor_cols = [
        c[1]
        for c in db.conn.execute("PRAGMA table_info('lake.silver.award_contractor')").fetchall()
    ]
    assert "bid_result" in contractor_cols
    assert "price_source" in contractor_cols
    assert "currency" in contractor_cols

    # Verify old data preserved with NULL for new column
    old_lot = db.conn.execute(
        "SELECT award_lot_id, source_lot_id FROM lake.silver.award_lot WHERE award_lot_id = 'lot_old'"
    ).fetchone()
    assert old_lot == ("lot_old", None)

    # Verify new data inserted with new column populated
    new_lot = db.conn.execute(
        "SELECT award_lot_id, source_lot_id FROM lake.silver.award_lot WHERE award_lot_id = 'lot_new'"
    ).fetchone()
    assert new_lot == ("lot_new", "slot_123")

    # 3. Idempotent rerun: run stage_partition again, must succeed without schema errors
    stage_partition(db, cat, "silver", {"award_lot": new_lots, "award_contractor": new_contractors})


def test_real_duckdb_schema_migration_fails_on_incompatible_schema():
    """Verify migration raises ValueError when an existing column has an incompatible type."""
    import duckdb

    from procurement.silver.application.warehouse import _migrate_result_schema_if_needed

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA lake")
    conn.execute("CREATE TABLE lake.award_lot (award_lot_id VARCHAR, source_lot_id INTEGER)")

    with pytest.raises(ValueError, match="Incompatible schema for lake.award_lot.source_lot_id"):
        _migrate_result_schema_if_needed(conn, "lake.award_lot", "award_lot")


def test_process_silver_service_fails_closed_when_iceberg_fails():
    """Verify ProcessSilverService fails closed for all resources when Iceberg publication requested but fails."""
    from datetime import date
    from unittest.mock import MagicMock

    from procurement.silver.application.process_silver_service import ProcessSilverService

    bronze, storage = MagicMock(), MagicMock()
    broken_iceberg = {"uri": "http://127.0.0.1:59999", "s3.endpoint": "http://127.0.0.1:59999"}
    service = ProcessSilverService(bronze, storage, "bucket", "contractor_result", broken_iceberg)

    with pytest.raises(
        RuntimeError,
        match="Iceberg initialization failed for contractor_result; publication was requested",
    ):
        service.execute(date(2024, 9, 16), date(2024, 9, 16))


def test_migrate_result_schema_fails_closed_on_introspection_error():
    """Verify _migrate_result_schema_if_needed fails closed with sanitized RuntimeError on introspection error."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import _migrate_result_schema_if_needed

    mock_db = MagicMock()
    mock_db.execute.side_effect = RuntimeError("database connection failed or corrupted")

    with pytest.raises(RuntimeError, match="Failed to inspect schema for lake.silver.award_lot"):
        _migrate_result_schema_if_needed(mock_db, "lake.silver.award_lot", "award_lot")


def test_migrate_result_schema_fails_closed_on_empty_table_info():
    """Verify _migrate_result_schema_if_needed fails closed when PRAGMA table_info returns empty."""
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import _migrate_result_schema_if_needed

    mock_db = MagicMock()
    mock_db.execute.return_value.fetchall.return_value = []

    with pytest.raises(
        RuntimeError, match="Table inspection returned no columns for lake.silver.award_lot"
    ):
        _migrate_result_schema_if_needed(mock_db, "lake.silver.award_lot", "award_lot")


def test_stage_partition_governance_replaces_by_exact_incoming_observation_cohort():
    """Verify quality_issue/quarantine rows for incoming observation cohort are replaced when empty, while preserving unrelated observations."""
    import duckdb

    from procurement.silver.application.warehouse import stage_partition

    class LocalDuckDBShim:
        def __init__(self):
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("ATTACH ':memory:' AS lake")

        def execute(self, query):
            if "PARTITIONED BY" in query:
                query = query.split(" PARTITIONED BY ")[0]
            return self.conn.execute(query)

        def sql(self, query):
            import re

            cleaned_query = re.sub(
                r"\s+AT\s*\(\s*VERSION\s*=>\s*\d+\s*\)", "", query, flags=re.IGNORECASE
            )
            return self.conn.sql(cleaned_query)

        def register(self, name, table):
            return self.conn.register(name, table)

        def unregister(self, name):
            return self.conn.unregister(name)

    class LocalCatalogShim:
        def __init__(self, db_shim):
            self.db_shim = db_shim

        def table(self, namespace, name):
            import contextlib

            count = 0
            with contextlib.suppress(Exception):
                count = self.db_shim.conn.sql(
                    f"SELECT count(*) FROM lake.{namespace}.{name}"
                ).fetchone()[0]
            return {
                "metadata": {
                    "format-version": 2,
                    "current-snapshot-id": 1,
                    "snapshots": [{"snapshot-id": 1, "summary": {"total-records": str(count)}}],
                }
            }

        def pin(self, namespace, name, snapshot_id, release_id):
            pass

    db = LocalDuckDBShim()
    cat = LocalCatalogShim(db)

    # 1. Initial state: obs-1 has issue-1; obs-2 (unrelated) has issue-2
    initial_issues = [
        {
            "issue_id": "iss-1",
            "observation_id": "obs-1",
            "code": "E1",
            "path": "p1",
            "severity": "WARNING",
        },
        {
            "issue_id": "iss-2",
            "observation_id": "obs-2",
            "code": "E2",
            "path": "p2",
            "severity": "WARNING",
        },
    ]
    stage_partition(db, cat, "silver", {"quality_issue": initial_issues})

    rows_init = db.conn.execute(
        "SELECT issue_id, observation_id FROM lake.silver.quality_issue ORDER BY issue_id"
    ).fetchall()
    assert rows_init == [("iss-1", "obs-1"), ("iss-2", "obs-2")]

    # 2. Rerun obs-1 with empty quality issues: must delete issue-1 for obs-1, while preserving issue-2 for obs-2
    stage_partition(
        db,
        cat,
        "silver",
        {
            "observation": [{"observation_id": "obs-1"}],
            "quality_issue": [],
        },
    )

    rows_after = db.conn.execute(
        "SELECT issue_id, observation_id FROM lake.silver.quality_issue ORDER BY issue_id"
    ).fetchall()
    assert rows_after == [("iss-2", "obs-2")]


def test_stage_partition_row_count_uses_logical_count_when_positional_deletes_exist():
    """Verify stage_partition row_count uses logical SQL count when positional deletes exist in metadata."""
    import duckdb

    from procurement.silver.application.warehouse import stage_partition

    class LocalDuckDBShim:
        def __init__(self):
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("ATTACH ':memory:' AS lake")

        def execute(self, query):
            if "PARTITIONED BY" in query:
                query = query.split(" PARTITIONED BY ")[0]
            return self.conn.execute(query)

        def sql(self, query):
            import re

            cleaned_query = re.sub(
                r"\s+AT\s*\(\s*VERSION\s*=>\s*\d+\s*\)", "", query, flags=re.IGNORECASE
            )
            return self.conn.sql(cleaned_query)

        def register(self, name, table):
            return self.conn.register(name, table)

        def unregister(self, name):
            return self.conn.unregister(name)

    class DeleteFilesCatalogShim:
        def __init__(self, db_shim):
            self.db_shim = db_shim

        def table(self, namespace, name):
            return {
                "metadata": {
                    "format-version": 2,
                    "current-snapshot-id": 1,
                    "snapshots": [
                        {
                            "snapshot-id": 1,
                            "summary": {
                                "total-records": "1627972",
                                "total-position-deletes": "55305",
                                "total-delete-files": "10",
                            },
                        }
                    ],
                }
            }

        def pin(self, namespace, name, snapshot_id, release_id):
            pass

    db = LocalDuckDBShim()
    cat = DeleteFilesCatalogShim(db)

    # Insert 5 actual rows into the table
    cand = [
        {"participation_id": f"p{i}", "revision_id": "r1", "lot_no": "1"}
        for i in range(5)
    ]
    snapshots = stage_partition(db, cat, "silver", {"bid_participation": cand})

    entry = snapshots["bid_participation"]
    # row_count must be the logical count (5), NOT physical 1627972
    assert entry["row_count"] == 5
    assert entry["physical_record_count"] == 1627972


def test_stateful_cleanup_real_duckdb_removes_stale_award_lots_and_preserves_unrelated():
    """Real DuckDB stateful test: verify stale award_lot rows removed on exact revision replay,

    including partial lot removal and zero-lot replacement, while unrelated revisions survive.
    """
    import re

    import duckdb

    from procurement.silver.application.warehouse import stage_partition

    class LocalDuckDBShim:
        def __init__(self):
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("ATTACH ':memory:' AS lake")

        def execute(self, sql):
            if "PARTITIONED BY" in sql:
                sql = sql.split(" PARTITIONED BY ")[0]
            return self.conn.execute(sql)

        def sql(self, query):
            cleaned_query = re.sub(
                r"\s+AT\s*\(\s*VERSION\s*=>\s*\d+\s*\)", "", query, flags=re.IGNORECASE
            )
            return self.conn.sql(cleaned_query)

        def register(self, name, table):
            return self.conn.register(name, table)

        def unregister(self, name):
            return self.conn.unregister(name)

    class LocalCatalogShim:
        def __init__(self, db_shim):
            self.db_shim = db_shim

        def table(self, namespace, name):
            import contextlib

            count = 0
            with contextlib.suppress(Exception):
                count = self.db_shim.conn.sql(
                    f"SELECT count(*) FROM lake.{namespace}.{name}"
                ).fetchone()[0]
            return {
                "metadata": {
                    "format-version": 2,
                    "current-snapshot-id": 1,
                    "snapshots": [{"snapshot-id": 1, "summary": {"total-records": str(count)}}],
                }
            }

        def pin(self, namespace, name, snapshot_id, release_id):
            pass

    db = LocalDuckDBShim()
    cat = LocalCatalogShim(db)

    # 1. Initial state: rev-1 has lots 'a' and 'b'; unrelated rev-2 has lot 'c'
    initial_lots = [
        {"award_lot_id": "a", "revision_id": "rev-1", "lot_no": "1"},
        {"award_lot_id": "b", "revision_id": "rev-1", "lot_no": "2"},
        {"award_lot_id": "c", "revision_id": "rev-2", "lot_no": "1"},
    ]
    stage_partition(db, cat, "silver", {"award_lot": initial_lots})

    rows_initial = db.conn.execute(
        "SELECT award_lot_id, revision_id FROM lake.silver.award_lot ORDER BY award_lot_id"
    ).fetchall()
    assert rows_initial == [("a", "rev-1"), ("b", "rev-1"), ("c", "rev-2")]

    # 2. Replay rev-1 with only lot 'a': must remove lot 'b' and preserve unrelated rev-2 ('c')
    cand_rev1_fewer = [
        {"award_lot_id": "a", "revision_id": "rev-1", "lot_no": "1"},
    ]
    stage_partition(
        db,
        cat,
        "silver",
        {
            "result_revision": [{"revision_id": "rev-1"}],
            "award_lot": cand_rev1_fewer,
        },
    )
    rows_after_fewer = db.conn.execute(
        "SELECT award_lot_id, revision_id FROM lake.silver.award_lot ORDER BY award_lot_id"
    ).fetchall()
    assert rows_after_fewer == [("a", "rev-1"), ("c", "rev-2")]

    # 3. Replay rev-1 with empty award_lot: must remove lot 'a' and preserve unrelated rev-2 ('c')
    stage_partition(
        db,
        cat,
        "silver",
        {
            "result_revision": [{"revision_id": "rev-1"}],
            "award_lot": [],
        },
    )
    rows_after_empty = db.conn.execute(
        "SELECT award_lot_id, revision_id FROM lake.silver.award_lot ORDER BY award_lot_id"
    ).fetchall()
    assert rows_after_empty == [("c", "rev-2")]
