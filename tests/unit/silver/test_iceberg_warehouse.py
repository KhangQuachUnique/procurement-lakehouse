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
    from unittest.mock import MagicMock
    from procurement.silver.application.warehouse import stage_partition
    import pyarrow as pa
    import datetime

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
    mock_db.execute.side_effect = lambda sql: executed_sql.append(sql)

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
    assert any("DELETE FROM lake.silver.notice_revision WHERE revision_id IN (SELECT revision_id FROM _incoming_cand)" in sql for sql in executed_sql)
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
    mock_db.execute.side_effect = lambda sql: executed_sql.append(sql)

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
    assert any("DELETE FROM lake.silver.quality_issue WHERE issue_id IN (SELECT issue_id FROM _incoming_cand)" in sql for sql in executed_sql)


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
    mock_db.execute.side_effect = lambda sql: executed_sql.append(sql)

    views = ensure_views(mock_db, namespace="silver", view_schema="custom_schema")

    assert len(views) == 6
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
