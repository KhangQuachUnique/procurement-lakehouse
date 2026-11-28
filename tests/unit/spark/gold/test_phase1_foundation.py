"""Unit tests for Gold Spark Phase 1 foundation (schema, native SQL UDFs, source contract)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from procurement_spark.gold.functions import (
    norm_bid_price_sql,
    recover_award_amount_sql,
    register_gold_functions,
)
from procurement_spark.gold.schema import (
    CORE_MERGE_KEYS,
    CORE_TABLES,
    GOLD_CORE_DDLS,
    GOLD_MART_DDLS,
    MART_DIMENSIONS,
    MART_FACTS,
    MART_PRIMARY_KEYS,
    MART_SPECS,
)
from procurement_spark.gold.source import SILVER_INPUT_TABLES
from procurement_spark.platform.config import SparkSilverSettings, _detect_java_home
from procurement_spark.platform.spark_session import build_spark_session


@pytest.fixture(scope="module")
def spark():
    cfg = SparkSilverSettings(
        app_database_url="postgresql://test:test@localhost/test",
        spark_master="local[2]",
        spark_driver_memory="1g",
        spark_shuffle_partitions=4,
        java_home=_detect_java_home(),
    )
    session = build_spark_session(cfg, local_unit_test=True)
    register_gold_functions(session)
    yield session
    session.stop()


def test_mart_schema_matches_legacy_public_contract() -> None:
    legacy_path = Path("src/procurement/gold/domain/schema.json")
    legacy = json.loads(legacy_path.read_text(encoding="utf-8"))

    assert set(GOLD_MART_DDLS.keys()) == set(legacy.keys())
    assert len(MART_DIMENSIONS) == 9
    assert len(MART_FACTS) == 8
    assert len(MART_PRIMARY_KEYS) == 17

    for fact_name in MART_FACTS:
        assert MART_PRIMARY_KEYS[fact_name] == "group_key"
        assert "PARTITIONED BY (months(report_date))" in GOLD_MART_DDLS[fact_name]

    for dim_name in MART_DIMENSIONS:
        assert "PARTITIONED BY" not in GOLD_MART_DDLS[dim_name]

    for table_name, cols in legacy.items():
        expected_col_names = [c["name"] for c in cols]
        actual_col_names = [name for name, _ in MART_SPECS[table_name].columns]
        assert actual_col_names == expected_col_names


def test_core_schema_ddls_and_merge_keys() -> None:
    assert len(CORE_TABLES) == 12
    assert set(GOLD_CORE_DDLS.keys()) == set(CORE_MERGE_KEYS.keys())
    for table_name, ddl in GOLD_CORE_DDLS.items():
        assert "USING iceberg" in ddl
        assert "VARCHAR" not in ddl
        assert "TIMESTAMPTZ" not in ddl
        for key_col in CORE_MERGE_KEYS[table_name]:
            assert key_col in ddl


def test_silver_input_tables_contract() -> None:
    assert len(SILVER_INPUT_TABLES) == 12
    assert "package_revision" in SILVER_INPUT_TABLES
    assert "contract_performance_revision" in SILVER_INPUT_TABLES


def test_norm_bid_price_native_sql_udf(spark) -> None:
    cases = [
        ("NULL", "1000000000", None),
        ("0", "1000000000", None),
        ("950000000", "1000000000", Decimal("950000000.000000")),
        ("950000000000", "1000000000", Decimal("950000000.000000")),  # 1000x typo
        ("950000000000000", "1000000000", Decimal("950000000.000000")),  # 1,000,000x typo
        ("95000000000", "1000000000", Decimal("950000000.000000")),  # 100x typo
        ("9500000000", "1000000000", Decimal("950000000.000000")),  # 10x typo
        ("99999999999999999", "1000000000", None),  # unrecoverable outlier
        ("60000000000000", "NULL", Decimal("60000000000.000000")),  # >10T, /1000 <= 500B
        ("1000000000000000", "NULL", None),  # >50T unrecoverable without baseline
    ]
    for raw_sql, ref_sql, expected in cases:
        expr = norm_bid_price_sql(f"CAST({raw_sql} AS DECIMAL(38,6))", f"CAST({ref_sql} AS DECIMAL(38,6))")
        got = spark.sql(f"SELECT {expr}").first()[0]
        assert got == expected, f"norm_bid_price({raw_sql}, {ref_sql}): got {got}, expected {expected}"


def test_recover_award_amount_native_sql_udf(spark) -> None:
    cases = [
        ("NULL", "1000000000", None),
        ("980000000", "1000000000", Decimal("980000000.000000")),
        ("980000000000", "1000000000", Decimal("980000000.000000")),  # 1000x typo
        ("980000000000000", "1000000000", Decimal("980000000.000000")),  # 1,000,000x typo
        # Concatenated prefix: raw_val starts with ref_baseline (1000000000) -> recovers ref_baseline
        ("100000000099", "1000000000", Decimal("1000000000.000000")),
        # No baseline, >10T, divisible by 1e6 after /1000, <=500B
        ("25000000000000", "NULL", Decimal("25000000000.000000")),
    ]
    for raw_sql, ref_sql, expected in cases:
        expr = recover_award_amount_sql(f"CAST({raw_sql} AS DECIMAL(38,6))", f"CAST({ref_sql} AS DECIMAL(38,6))")
        got = spark.sql(f"SELECT {expr}").first()[0]
        assert got == expected, f"recover_award_amount({raw_sql}, {ref_sql}): got {got}, expected {expected}"
