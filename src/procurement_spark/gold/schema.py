"""Iceberg DDL and contract registry for Two-Tier Gold (Tier 1 Core + Tier 2 Public Marts).

Contracts are loaded from self-contained JSON files (``core_schema.json`` and
``mart_schema.json``) so ``procurement_spark`` has zero runtime dependency on
legacy ``procurement.*`` packages.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

_DIR = Path(__file__).resolve().parent

# Tier 1: Atomic entity-grain tables persisted in ``lake.gold_spark_core.*``
# Used both for BI drill-down and for Iceberg MERGE INTO incremental updates.
CORE_MERGE_KEYS: dict[str, tuple[str, ...]] = {
    "fact_procurement_demand": ("package_key",),
    "fact_tender_notice": ("notice_business_key",),
    "fact_award": ("award_business_key",),
    "fact_savings": ("comparison_scope_key",),
    "fact_selection_round": ("round_business_key",),
    "fact_bidding_competition": ("opening_key",),
    "dim_package": ("package_key",),
    "dim_scope": ("scope_key",),
    "bridge_location": ("location_set_key", "geography_key", "location_type"),
    "bridge_award_recipient": ("award_business_key", "scope_key", "party_key"),
    "bridge_party_member": ("party_key", "member_party_key"),
    "bridge_savings_award": ("comparison_scope_key", "award_business_key", "scope_key"),
}

CORE_TABLES: tuple[str, ...] = tuple(CORE_MERGE_KEYS.keys())

_TYPE_MAP = (
    (re.compile(r"\bTIMESTAMPTZ\b", re.IGNORECASE), "TIMESTAMP"),
    (re.compile(r"\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b", re.IGNORECASE), "TIMESTAMP"),
    (re.compile(r"\bVARCHAR\b", re.IGNORECASE), "STRING"),
    (re.compile(r"\bINTEGER\b", re.IGNORECASE), "INT"),
)


def _duckdb_type_to_spark(sql_type: str) -> str:
    out = sql_type.strip()
    for pattern, replacement in _TYPE_MAP:
        out = pattern.sub(replacement, out)
    return out


def _translate_ddl_columns(raw_cols_ddl: str) -> str:
    """Convert a comma-separated DuckDB column DDL string to Spark SQL Iceberg types."""
    out = raw_cols_ddl.strip()
    for pattern, replacement in _TYPE_MAP:
        out = pattern.sub(replacement, out)
    return out


@dataclass(frozen=True)
class MartTableSpec:
    name: str
    is_fact: bool
    primary_key: str
    partition_col: str | None
    columns: tuple[tuple[str, str], ...]  # (col_name, spark_type)


def _load_mart_specs() -> dict[str, MartTableSpec]:
    raw = json.loads((_DIR / "mart_schema.json").read_text(encoding="utf-8"))
    specs: dict[str, MartTableSpec] = {}
    for table_name, cols in raw.items():
        pks = [c["name"] for c in cols if c.get("role") == "PK"]
        if len(pks) != 1:
            raise ValueError(f"Expected single PK for mart table {table_name}, got {pks}")
        is_fact = table_name.startswith("fact_")
        col_tuples = tuple((c["name"], _duckdb_type_to_spark(c["type"])) for c in cols)
        partition_col = "report_date" if is_fact else None
        specs[table_name] = MartTableSpec(
            name=table_name,
            is_fact=is_fact,
            primary_key=pks[0],
            partition_col=partition_col,
            columns=col_tuples,
        )
    return specs


MART_SPECS: dict[str, MartTableSpec] = _load_mart_specs()
MART_DIMENSIONS: tuple[str, ...] = tuple(k for k, v in MART_SPECS.items() if not v.is_fact)
MART_FACTS: tuple[str, ...] = tuple(k for k, v in MART_SPECS.items() if v.is_fact)
MART_PRIMARY_KEYS: dict[str, str] = {k: v.primary_key for k, v in MART_SPECS.items()}


def _build_mart_ddls() -> dict[str, str]:
    ddls: dict[str, str] = {}
    for name, spec in MART_SPECS.items():
        cols_sql = ",\n        ".join(f"{col} {dtype}" for col, dtype in spec.columns)
        part_clause = (
            f"\n    PARTITIONED BY (months({spec.partition_col}))" if spec.partition_col else ""
        )
        ddls[name] = (
            f"CREATE TABLE IF NOT EXISTS {{full_name}} (\n"
            f"        {cols_sql}\n"
            f"    ) USING iceberg{part_clause}\n"
            f"    TBLPROPERTIES (\n"
            f"        'format-version'='2',\n"
            f"        'write.wap.enabled'='true',\n"
            f"        'write.format.default'='parquet',\n"
            f"        'write.parquet.compression-codec'='zstd'\n"
            f"    )"
        )
    return ddls


def _build_core_ddls() -> dict[str, str]:
    raw = json.loads((_DIR / "core_schema.json").read_text(encoding="utf-8"))
    ddls: dict[str, str] = {}
    for name in CORE_TABLES:
        if name not in raw:
            raise KeyError(f"Core table {name!r} missing from core_schema.json")
        cols_sql = _translate_ddl_columns(raw[name])
        # Drop legacy DuckDB batching flag if present
        cols_sql = re.sub(r",\s*is_in_event_period\s+BOOLEAN", "", cols_sql, flags=re.IGNORECASE)
        ddls[name] = (
            f"CREATE TABLE IF NOT EXISTS {{full_name}} (\n"
            f"        {cols_sql}\n"
            f"    ) USING iceberg\n"
            f"    TBLPROPERTIES (\n"
            f"        'format-version'='2',\n"
            f"        'write.wap.enabled'='true',\n"
            f"        'write.format.default'='parquet',\n"
            f"        'write.parquet.compression-codec'='zstd'\n"
            f"    )"
        )
    return ddls


GOLD_MART_DDLS: dict[str, str] = _build_mart_ddls()
GOLD_CORE_DDLS: dict[str, str] = _build_core_ddls()
