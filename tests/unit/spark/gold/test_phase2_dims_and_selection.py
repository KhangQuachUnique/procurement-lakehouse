"""Unit tests for Gold Spark Phase 2 (base dimensions, selection/linkage, business dimensions)."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from procurement.gold.core.dimensions_base import GoldBuildConfig, build_base_dimensions
from procurement.gold.core.dimensions_biz import build_business_dimensions
from procurement.gold.core.selection import build_selection_views
from procurement.spark.config import SparkSilverSettings, _detect_java_home
from procurement.spark.spark_session import build_spark_session


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
    yield session
    session.stop()


def _register_fixture_silver_views(spark) -> None:
    ts1 = datetime(2025, 6, 1, 8, 0, tzinfo=timezone.utc)
    ts2 = datetime(2025, 6, 2, 8, 0, tzinfo=timezone.utc)

    # package_revision: pkg1 has 2 versions (ts2 wins); pkg_tie has same ts with different price; garbage date 1202
    pkg_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("source_id", StringType(), True),
            StructField("source_date", DateType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("public_date", DateType(), True),
            StructField("package_no", StringType(), True),
            StructField("title", StringType(), True),
            StructField("plan_id", StringType(), True),
            StructField("plan_no", StringType(), True),
            StructField("bid_price", DecimalType(38, 6), True),
            StructField("currency", StringType(), True),
            StructField("bid_field", StringType(), True),
            StructField("bid_form", StringType(), True),
            StructField("bid_mode", StringType(), True),
            StructField("is_domestic", BooleanType(), True),
            StructField("is_internet", BooleanType(), True),
            StructField("contract_type", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("rev_p1_v1", "pkg1", "src_p1", date(2025, 6, 1), ts1, date(2025, 6, 1), "PKG001", "Goi 1 old", "plan1", "PL001", Decimal("1000000000"), "VND", "HH", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI"),
            ("rev_p1_v2", "pkg1", "src_p1", date(2025, 6, 2), ts2, date(2025, 6, 2), "PKG001", "Goi 1 new", "plan1", "PL001", Decimal("1200000000"), "VND", "HH", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI"),
            ("rev_tie_a", "pkg_tie", "src_tie", date(2025, 6, 1), ts1, date(1202, 10, 18), "PKGTIE", "Tie A", None, None, Decimal("500000000"), "JPY", "XL", "CDT", None, False, False, None),
            ("rev_tie_b", "pkg_tie", "src_tie", date(2025, 6, 1), ts1, date(2025, 6, 1), "PKGTIE", "Tie B", None, None, Decimal("600000000"), "JPY", "XL", "CDT", None, False, False, None),
        ],
        schema=pkg_schema,
    ).createOrReplaceTempView("silver_package_revision")

    # package_location: '101' -> HNI, '824' -> MULTI_824, 'XYZ' -> unmapped custom GEO_XYZ
    loc_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("province_code", StringType(), True),
            StructField("province_name", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("rev_p1_v2", "101", "Ha Noi"),
            ("rev_tie_b", "824", "Toan quoc"),
            ("rev_tie_a", "XYZ", "Custom Region"),
        ],
        schema=loc_schema,
    ).createOrReplaceTempView("silver_package_location")
    spark.createDataFrame([], schema=loc_schema).createOrReplaceTempView("silver_notice_location")

    # plan_revision
    plan_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("source_id", StringType(), True),
            StructField("plan_no", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("project_no", StringType(), True),
            StructField("public_date", DateType(), True),
        ]
    )
    spark.createDataFrame(
        [("rev_pl1", "plan1", "src_pl1", "PL001", "prj1", "PRJ001", date(2025, 5, 15))],
        schema=plan_schema,
    ).createOrReplaceTempView("silver_plan_revision")

    # notice_revision
    not_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("source_family", StringType(), True),
            StructField("notify_no", StringType(), True),
            StructField("notify_version", StringType(), True),
            StructField("source_version", StringType(), True),
            StructField("source_date", DateType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("public_date", DateType(), True),
            StructField("bid_close_date", TimestampType(), True),
            StructField("cancelled_at", TimestampType(), True),
            StructField("package_id", StringType(), True),
            StructField("package_no", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("reoffer_currency", StringType(), True),
            StructField("bid_field", StringType(), True),
            StructField("bid_form", StringType(), True),
            StructField("bid_mode", StringType(), True),
            StructField("is_domestic", BooleanType(), True),
            StructField("is_internet", BooleanType(), True),
            StructField("contract_type", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("rev_n1", "not1", "VN", "IB2500001", "01", "01", date(2025, 6, 3), ts2, ts2, date(2025, 6, 3), ts2, None, "src_p1", "PKG001", "VND", None, "HH", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI"),
        ],
        schema=not_schema,
    ).createOrReplaceTempView("silver_notice_revision")

    # result_revision: res1 matches pkg1 by bid_id; res_unmatched has no package
    res_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("result_id", StringType(), True),
            StructField("result_version", StringType(), True),
            StructField("source_version", StringType(), True),
            StructField("source_date", DateType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("public_date", DateType(), True),
            StructField("decision_date", DateType(), True),
            StructField("bid_id", StringType(), True),
            StructField("package_no", StringType(), True),
            StructField("notify_no", StringType(), True),
            StructField("currency", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("rev_r1", "res1", "rid1", "00", "00", date(2025, 6, 10), ts2, date(2025, 6, 10), date(2025, 6, 9), "src_p1", "PKG001", "IB2500001", "VND"),
            ("rev_r2", "res2", "rid2", "00", "00", date(2025, 6, 11), ts2, date(2025, 6, 11), date(2099, 1, 1), "missing_bid", "PKG_NONE", None, "VND"),
        ],
        schema=res_schema,
    ).createOrReplaceTempView("silver_result_revision")

    # award_lot: res1 has a lot, res2 has no lot (so res2 gets :PACKAGE:ROUND: scope)
    lot_schema = StructType(
        [
            StructField("award_lot_id", StringType(), False),
            StructField("revision_id", StringType(), False),
            StructField("lot_no", StringType(), True),
            StructField("lot_name", StringType(), True),
            StructField("currency", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [("lot_id_1", "rev_r1", "1", "Phan lo 1", "VND")],
        schema=lot_schema,
    ).createOrReplaceTempView("silver_award_lot")


def test_phase2_end_to_end_dims_and_selection(spark) -> None:
    _register_fixture_silver_views(spark)
    cfg = GoldBuildConfig(
        event_start=date(2025, 6, 1),
        event_end=date(2025, 6, 11),
        observation_day=date(2025, 6, 12),
    )

    base = build_base_dimensions(spark, cfg)
    sel = build_selection_views(spark)
    biz = build_business_dimensions(spark, cfg)

    # 1. dim_date: garbage dates (1202, 2099) excluded; weekday & ISO week match DuckDB
    dates = {r["date_key"]: r for r in base["dim_date"].collect()}
    assert date(1202, 10, 18) not in dates
    assert date(2099, 1, 1) not in dates
    assert date(2025, 5, 15) in dates  # sparse historical plan date
    assert date(2025, 6, 12) in dates  # observation_day
    # 2025-06-01 is Sunday -> day_of_week=0, is_weekend=True
    assert dates[date(2025, 6, 1)]["day_of_week"] == 0
    assert dates[date(2025, 6, 1)]["is_weekend"] is True
    # 2025-06-02 is Monday -> day_of_week=1, is_weekend=False
    assert dates[date(2025, 6, 2)]["day_of_week"] == 1
    assert dates[date(2025, 6, 2)]["is_weekend"] is False
    assert dates[date(2025, 6, 7)]["day_of_week"] == 6
    assert dates[date(2025, 6, 7)]["is_weekend"] is True

    # 2. dim_currency includes standard 4 + JPY
    currencies = {r["currency_key"] for r in base["dim_currency"].collect()}
    assert {"VND", "USD", "EUR", "CUR_UNKNOWN", "JPY"} <= currencies

    # 3. dim_geography has 65 canonical + GEO_XYZ
    geos = {r["geography_key"] for r in base["dim_geography"].collect()}
    assert len(geos) == 66
    assert {"GEO_HNI", "GEO_HCM", "GEO_MULTI_UNALLOCATED", "GEO_UNKNOWN", "GEO_XYZ"} <= geos

    # 4. selection dedup + tie_conflict + geography rollup + linkage
    pkgs = {r["entity_id"]: r for r in sel["_gtmp_cand_package"].collect()}
    assert pkgs["pkg1"]["revision_id"] == "rev_p1_v2"
    assert pkgs["pkg1"]["package_resolution_state"] == "resolved_deterministic"
    assert pkgs["pkg_tie"]["package_resolution_state"] == "tie_conflict"

    pgeo = {r["revision_id"]: r for r in sel["_gtmp_pkg_geography"].collect()}
    assert pgeo["rev_p1_v2"]["single_province_code"] == "HNI"
    assert pgeo["rev_p1_v2"]["prov_count"] == 1
    assert pgeo["rev_tie_b"]["prov_count"] == 2
    assert pgeo["rev_tie_b"]["loc_hash"] == "MULTI_824"

    rmatch = {r["result_entity_id"]: r for r in sel["_gtmp_result_package_match"].collect()}
    assert rmatch["res1"]["package_key"] == "PKG:pkg1"
    assert rmatch["res1"]["package_resolution_state"] == "resolved_exact_source_id"
    assert rmatch["res2"]["package_key"] == "PKG_UNRESOLVED:RESULT:res2"

    # 5. dim_package has resolved packages + unmatched result placeholder
    dim_pkg = {r["package_key"]: r for r in biz["dim_package"].collect()}
    assert dim_pkg["PKG:pkg1"]["project_no"] == "PRJ001"
    assert "PKG_UNRESOLVED:RESULT:res2" in dim_pkg

    # 6. dim_scope has :PACKAGE, :LOT:, and :PACKAGE:ROUND:
    scopes = {r["scope_key"]: r for r in biz["dim_scope"].collect()}
    assert "PKG:pkg1:PACKAGE" in scopes
    assert "PKG:pkg1:LOT:1:REVISION:lot_id_1:ROUND:RND:NOT:VN:IB2500001" in scopes
    assert "PKG_UNRESOLVED:RESULT:res2:PACKAGE:ROUND:RESULT:res2" in scopes

    # 7. dim_procurement_method exact MD5 parity with DuckDB
    expected_md5 = "MTH:" + hashlib.md5(b"DTRR|1_GIAI_DOAN|true|true|TRON_GOI|v1").hexdigest()
    methods = {r["method_key"]: r for r in biz["dim_procurement_method"].collect()}
    assert "METHOD_UNKNOWN" in methods
    assert expected_md5 in methods
    assert methods[expected_md5]["method_category"] == "Đấu thầu rộng rãi"
