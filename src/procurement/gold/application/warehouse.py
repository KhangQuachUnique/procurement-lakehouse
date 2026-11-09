"""Warehouse provisioning and view promotion for the Gold Dimensional Layer.

Responsible for:
1. Provisioning 16 Iceberg Format v2 tables in lake.gold_v2:
   - 7 Facts partitioned by month(report_date)
   - 9 unpartitioned Dimensions
2. Seeding dim_geography with the authoritative 63 provinces dictionary
3. Promoting zero-copy logical views in schema gold.*
"""

import threading
from typing import Any

import duckdb

from procurement.gold.domain.geography import get_geography_records
from procurement.gold.domain.models import identifier
from procurement.gold.domain.schema import FACTS, TABLES, Table

_ICEBERG_COMMIT_LOCK = threading.RLock()


def _has_lake_catalog(db: Any) -> bool:
    try:
        catalogs = {
            r[0]
            for r in db.execute(
                "SELECT catalog_name FROM information_schema.schemata"
            ).fetchall()
        }
        return "lake" in catalogs
    except (duckdb.Error, RuntimeError):
        return False


def _schema_prefix(db: Any, namespace: str) -> str:
    clean_ns = identifier(namespace)
    if _has_lake_catalog(db):
        return f"lake.{clean_ns}"
    return clean_ns


def get_table_ddl(table: Table, partitioned: bool = False) -> str:
    """Generate DDL for a Gold contract table."""
    cols = ", ".join(f"{c.name} {c.sql_type}" for c in table.columns)
    partition = " PARTITIONED BY (month(report_date))" if partitioned else ""
    return f"({cols}){partition}"


def ensure_gold_tables(
    db: Any,
    catalog: Any = None,
    namespace: str = "gold_v2",
) -> dict[str, str]:
    """Ensure all 16 Gold Iceberg tables exist in lake.{namespace} with correct partitioning."""
    clean_ns = identifier(namespace)

    if catalog is not None:
        if hasattr(catalog, "create_namespace_if_not_exists"):
            catalog.create_namespace_if_not_exists(clean_ns)
        elif hasattr(catalog, "catalog") and hasattr(catalog.catalog, "create_namespace_if_not_exists"):
            catalog.catalog.create_namespace_if_not_exists(clean_ns)

    has_lake = _has_lake_catalog(db)
    prefix = _schema_prefix(db, clean_ns)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {prefix}")

    results = {}
    with _ICEBERG_COMMIT_LOCK:
        for name, table in TABLES.items():
            is_fact = name in FACTS
            cols_ddl = ", ".join(f"{c.name} {c.sql_type}" for c in table.columns)
            partition = " PARTITIONED BY (month(report_date))" if (is_fact and has_lake) else ""
            sql = f"CREATE TABLE IF NOT EXISTS {prefix}.{name} ({cols_ddl}){partition}"
            db.execute(sql)
            results[name] = sql

        # Seed dim_geography if empty or missing canonical records
        seed_geography(db, clean_ns)

    return results


def seed_geography(db: Any, namespace: str = "gold_v2") -> int:
    """Seed dim_geography with the authoritative 63 provinces of Vietnam."""
    clean_ns = identifier(namespace)
    prefix = _schema_prefix(db, clean_ns)
    table_ref = f"{prefix}.dim_geography"

    # Check if table exists
    try:
        count = db.execute(f"SELECT count(*) FROM {table_ref}").fetchone()[0]
    except (duckdb.Error, RuntimeError):
        return 0

    if count >= 63:
        return count

    records = get_geography_records()
    for r in records:
        key = r["geography_key"]
        v_id = r["geography_version"]
        p_code = r["province_code"]
        p_name = r["province_name"]
        reg_name = r["region_name"]
        is_multi = r["is_multi_geography"]
        is_un = r["is_unknown"]

        db.execute(
            f"""
            INSERT INTO {table_ref} (
                geography_key, geography_version, province_code, province_name,
                region_name, is_multi_geography, is_unknown, valid_from, valid_to, is_current
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, NULL, NULL, TRUE
            WHERE NOT EXISTS (SELECT 1 FROM {table_ref} WHERE geography_key = ?)
            """,
            [key, v_id, p_code, p_name, reg_name, is_multi, is_un, key],
        )

    return db.execute(f"SELECT count(*) FROM {table_ref}").fetchone()[0]


def promote_views(
    db: Any,
    source_namespace: str = "gold_v2",
    target_schema: str = "gold",
) -> list[str]:
    """Promote physical lake.{source_namespace}.* tables to zero-copy logical views in {target_schema}.*."""
    clean_src = identifier(source_namespace)
    clean_tgt = identifier(target_schema)

    db.execute(f"CREATE SCHEMA IF NOT EXISTS {clean_tgt}")
    promoted = []

    src_prefix = _schema_prefix(db, clean_src)

    for name in TABLES:
        src_table = f"{src_prefix}.{name}"
        tgt_view = f"{clean_tgt}.{name}"
        db.execute(f"CREATE OR REPLACE VIEW {tgt_view} AS SELECT * FROM {src_table}")
        promoted.append(tgt_view)

    return promoted
