"""Unit tests for Gold Spark Phase 3A (demand, notice, award + dim_party + bridges)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from procurement.gold.core.award import build_award
from procurement.gold.core.demand import build_demand
from procurement.gold.core.dimensions_base import GoldBuildConfig, build_base_dimensions
from procurement.gold.core.dimensions_biz import build_business_dimensions
from procurement.gold.core.notice import build_notice
from procurement.gold.core.selection import build_selection_views
from procurement.gold.functions import register_gold_functions
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
    register_gold_functions(session)
    yield session
    session.stop()


def _register_silver_fixtures(spark) -> None:
    t_pub = datetime(2025, 6, 1, 8, 0, tzinfo=timezone.utc)
    t_close = datetime(2025, 6, 11, 8, 0, tzinfo=timezone.utc)  # exactly 240.00 hours later

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
            StructField("bid_start_year", IntegerType(), True),
            StructField("bid_start_month", IntegerType(), True),
            StructField("bid_start_quarter", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("rev_p1", "pkg1", "src_p1", date(2025, 6, 1), t_pub, date(2025, 6, 1), "PKG001", "Goi 1", "plan1", "PL001", Decimal("2000000000"), "VND", "HH", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI", 2025, 7, 3),
            ("rev_p2", "pkg2", "src_p2", date(2025, 6, 2), t_pub, date(2025, 6, 2), "PKG002", "Goi 2 huy", "plan1", "PL001", Decimal("500000000"), "VND", "XL", "CDT", None, True, False, None, 2025, None, None),
        ],
        schema=pkg_schema,
    ).createOrReplaceTempView("silver_package_revision")

    loc_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("province_code", StringType(), True),
            StructField("province_name", StringType(), True),
        ]
    )
    spark.createDataFrame([("rev_p1", "101", "Ha Noi")], schema=loc_schema).createOrReplaceTempView(
        "silver_package_location"
    )
    spark.createDataFrame([], schema=loc_schema).createOrReplaceTempView("silver_notice_location")

    plan_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("source_id", StringType(), True),
            StructField("plan_no", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("project_no", StringType(), True),
            StructField("investor_name", StringType(), True),
            StructField("public_date", DateType(), True),
        ]
    )
    spark.createDataFrame(
        [("rev_pl1", "plan1", "src_pl1", "PL001", "prj1", "PRJ001", "So Y Te Ha Noi", date(2025, 5, 20))],
        schema=plan_schema,
    ).createOrReplaceTempView("silver_plan_revision")

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
            StructField("original_published_at", TimestampType(), True),
            StructField("public_date", DateType(), True),
            StructField("bid_close_date", TimestampType(), True),
            StructField("cancelled_at", TimestampType(), True),
            StructField("cancel_reason", StringType(), True),
            StructField("status_raw", StringType(), True),
            StructField("package_id", StringType(), True),
            StructField("package_no", StringType(), True),
            StructField("investor_code", StringType(), True),
            StructField("investor_name", StringType(), True),
            StructField("procuring_entity_code", StringType(), True),
            StructField("procuring_entity_name", StringType(), True),
            StructField("bid_price", DecimalType(38, 6), True),
            StructField("currency", StringType(), True),
            StructField("initial_price", DecimalType(38, 6), True),
            StructField("reoffer_currency", StringType(), True),
            StructField("guarantee_amount", DecimalType(38, 6), True),
            StructField("guarantee_form", StringType(), True),
            StructField("contract_duration_unit", StringType(), True),
            StructField("contract_duration_raw", StringType(), True),
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
            (
                "rev_n1", "not1", "VN", "IB2500001", "01", "01", date(2025, 6, 1), t_pub,
                t_pub, t_pub, date(2025, 6, 1), t_close, None, None, "IS_PUBLISH",
                "src_p1", "PKG001", "INV01", "So Y Te Ha Noi", "PROC01", "Ben moi thau A",
                Decimal("2000000000"), "VND", None, None, Decimal("20000000"), "Bao lanh",
                "DAY", "90 ngay", "HH", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI",
            ),
        ],
        schema=not_schema,
    ).createOrReplaceTempView("silver_notice_revision")

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
            StructField("total_winning_price", DecimalType(38, 6), True),
            StructField("status_raw", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("rev_r1", "res1", "rid1", "00", "00", date(2025, 6, 20), t_close, date(2025, 6, 20), date(2025, 6, 19), "src_p1", "PKG001", "IB2500001", "VND", Decimal("1800000000"), "IS_PUBLISH"),
            ("rev_r2", "res2", "rid2", "00", "00", date(2025, 6, 21), t_close, date(2025, 6, 21), date(2025, 6, 20), "src_p2", "PKG002", None, "VND", Decimal("450000000"), "CANCELED"),
        ],
        schema=res_schema,
    ).createOrReplaceTempView("silver_result_revision")

    lot_schema = StructType(
        [
            StructField("award_lot_id", StringType(), False),
            StructField("revision_id", StringType(), False),
            StructField("lot_no", StringType(), True),
            StructField("lot_name", StringType(), True),
            StructField("lot_price", DecimalType(38, 6), True),
            StructField("winning_price", DecimalType(38, 6), True),
            StructField("currency", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [
            # Lot 1: 1000x typo in winning_price (950B vs 1B baseline) -> recovered to 950M
            ("lot_1", "rev_r1", "1", "Lo 1", Decimal("1000000000"), Decimal("950000000000"), "VND"),
            # Lot 2: consortium lot
            ("lot_2", "rev_r1", "2", "Lo 2", Decimal("1000000000"), Decimal("850000000"), "VND"),
        ],
        schema=lot_schema,
    ).createOrReplaceTempView("silver_award_lot")

    ctr_schema = StructType(
        [
            StructField("award_contractor_id", StringType(), False),
            StructField("revision_id", StringType(), False),
            StructField("award_lot_id", StringType(), True),
            StructField("contractor_code", StringType(), True),
            StructField("contractor_name", StringType(), True),
            StructField("tax_code", StringType(), True),
            StructField("is_consortium", BooleanType(), True),
            StructField("venture_code", StringType(), True),
            StructField("venture_name", StringType(), True),
            StructField("awarded_price", DecimalType(38, 6), True),
            StructField("role", StringType(), True),
            StructField("bid_result", IntegerType(), True),
        ]
    )
    spark.createDataFrame(
        [
            ("ac1", "rev_r1", "lot_1", "VN01", "Cong ty A", "0101", False, None, None, Decimal("950000000"), "PRIMARY", 1),
            ("ac2", "rev_r1", "lot_2", "VN02", "Cong ty B", "0102", True, "LD01", "Lien danh BC", None, "LEAD", 1),
            ("ac3", "rev_r1", "lot_2", "VN03", "Cong ty C", "0103", True, "LD01", "Lien danh BC", None, "MEMBER", 1),
            ("ac4", "rev_r2", None, "VN04", "Cong ty D", "0104", False, None, None, Decimal("450000000"), "PRIMARY", 1),
        ],
        schema=ctr_schema,
    ).createOrReplaceTempView("silver_award_contractor")


def test_phase3a_demand_notice_award_end_to_end(spark) -> None:
    _register_silver_fixtures(spark)
    cfg = GoldBuildConfig(
        event_start=date(2025, 6, 1),
        event_end=date(2025, 6, 30),
        observation_day=date(2025, 6, 30),
    )

    build_base_dimensions(spark, cfg)
    build_selection_views(spark)
    build_business_dimensions(spark, cfg)
    dem_out = build_demand(spark, cfg)
    not_out = build_notice(spark, cfg)
    awd_out = build_award(spark, cfg)

    # 1. Demand checks
    dems = {r["package_key"]: r for r in dem_out["fact_procurement_demand"].collect()}
    assert dems["PKG:pkg1"]["planned_start_from"] == date(2025, 7, 1)
    assert dems["PKG:pkg1"]["planned_start_to"] == date(2025, 7, 31)
    assert dems["PKG:pkg1"]["planned_start_precision"] == "month"
    assert dems["PKG:pkg1"]["notice_observation_state"] == "observed"
    assert dems["PKG:pkg1"]["award_observation_state"] == "observed"
    assert dems["PKG:pkg1"]["reporting_geography_key"] == "GEO_HNI"
    assert dems["PKG:pkg2"]["notice_observation_state"] == "not_observed"

    # 2. Notice checks
    nots = {r["notice_business_key"]: r for r in not_out["fact_tender_notice"].collect()}
    n1 = nots["NOT:VN:IB2500001"]
    assert n1["preparation_hours"] == Decimal("240.000000")
    assert n1["is_preparation_eligible"] is True
    assert n1["cancellation_state"] == "none"
    assert n1["reporting_geography_key"] == "GEO_HNI"

    # 3. Award checks (typo recovery, consortium unallocated, single-L 'CANCELED' DQ fix)
    awds = {r["award_business_key"]: r for r in awd_out["fact_award"].collect()}
    assert awds["AWARD:rev_r1:LOT:lot_1"]["award_state"] == "effective"
    assert awds["AWARD:rev_r1:LOT:lot_1"]["award_amount"] == Decimal("950000000.000000")
    assert awds["AWARD:rev_r1:LOT:lot_1"]["recipient_set_key"] == "PARTY:VN01"
    assert awds["AWARD:rev_r1:LOT:lot_1"]["recipient_allocation_state"] == "single_recipient"

    assert awds["AWARD:rev_r1:LOT:lot_2"]["award_state"] == "effective"
    assert awds["AWARD:rev_r1:LOT:lot_2"]["recipient_set_key"] == "CON:LD01"
    assert awds["AWARD:rev_r1:LOT:lot_2"]["recipient_allocation_state"] == "unallocated_consortium"

    # DQ fix: single-L 'CANCELED' -> 'cancelled', amount set NULL
    assert awds["AWARD:rev_r2:PKG"]["award_state"] == "cancelled"
    assert awds["AWARD:rev_r2:PKG"]["is_amount_eligible"] is False
    assert awds["AWARD:rev_r2:PKG"]["award_amount"] is None

    # 4. Bridges & dim_party checks
    recips = {
        (r["award_business_key"], r["party_key"]): r
        for r in awd_out["bridge_award_recipient"].collect()
    }
    assert recips[("AWARD:rev_r1:LOT:lot_1", "PARTY:VN01")]["allocation_state"] == "verified_amount"
    assert recips[("AWARD:rev_r1:LOT:lot_1", "PARTY:VN01")]["attributed_amount"] == Decimal("950000000.000000")
    assert recips[("AWARD:rev_r1:LOT:lot_2", "PARTY:VN02")]["allocation_state"] == "unallocated"
    assert recips[("AWARD:rev_r1:LOT:lot_2", "PARTY:VN02")]["attributed_amount"] is None

    members = {(r["party_key"], r["member_party_key"]) for r in awd_out["bridge_party_member"].collect()}
    assert ("CON:LD01", "PARTY:VN02") in members
    assert ("CON:LD01", "PARTY:VN03") in members

    parties = [r["party_key"] for r in awd_out["dim_party"].collect()]
    assert len(parties) == len(set(parties))  # 0 duplicate PKs
    assert {"PARTY:VN01", "CON:LD01", "PARTY_INVESTOR:INV01", "PARTY_PROCURER:PROC01", "REC_NO_WINNER"} <= set(parties)
