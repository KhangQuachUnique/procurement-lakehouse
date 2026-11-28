"""Unit tests for Gold Spark Phase 3B (savings, duration, competition)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from procurement_spark.gold.core.award import build_award
from procurement_spark.gold.core.competition import build_competition
from procurement_spark.gold.core.demand import build_demand
from procurement_spark.gold.core.dimensions_base import GoldBuildConfig, build_base_dimensions
from procurement_spark.gold.core.dimensions_biz import build_business_dimensions
from procurement_spark.gold.core.duration import build_duration
from procurement_spark.gold.core.notice import build_notice
from procurement_spark.gold.core.savings import build_savings
from procurement_spark.gold.core.selection import build_selection_views
from procurement_spark.gold.functions import register_gold_functions
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


def _register_full_silver_fixtures(spark) -> None:
    t_pkg = datetime(2025, 6, 1, 8, 0, tzinfo=timezone.utc)
    t_not = datetime(2025, 6, 6, 8, 0, tzinfo=timezone.utc)
    t_close = datetime(2025, 6, 16, 8, 0, tzinfo=timezone.utc)  # 240h after notice
    t_open = datetime(2025, 6, 16, 10, 0, tzinfo=timezone.utc)  # 2h after close

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
            # pkg1: single-lot award + contract -> eligible for contract savings
            ("rev_p1", "pkg1", "src_p1", date(2025, 6, 1), t_pkg, date(2025, 6, 1), "PKG001", "Single lot pkg", "plan1", "PL001", Decimal("1000000000"), "VND", "HH", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI", 2025, 6, 2),
            # pkg2: multi-lot award + contract -> NOT eligible for ambiguous package-level contract savings
            ("rev_p2", "pkg2", "src_p2", date(2025, 6, 1), t_pkg, date(2025, 6, 1), "PKG002", "Multi lot pkg", "plan1", "PL001", Decimal("2000000000"), "VND", "XL", "DTRR", "1_GIAI_DOAN", True, True, "TRON_GOI", 2025, 6, 2),
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
    spark.createDataFrame([("rev_p1", "101", "Ha Noi")], schema=loc_schema).createOrReplaceTempView("silver_package_location")
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
        [("rev_pl1", "plan1", "src_pl1", "PL001", "prj1", "PRJ001", "Investor A", date(2025, 5, 15))],
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
                "rev_n1", "not1", "VN", "IB2500001", "01", "01", date(2025, 6, 6), t_not,
                t_not, t_not, date(2025, 6, 6), t_close, None, None, "IS_PUBLISH",
                "src_p1", "PKG001", "INV01", "Investor A", "PROC01", "Procurer A",
                Decimal("1000000000"), "VND", None, None, None, None,
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
            ("rev_r1", "res1", "rid1", "00", "00", date(2025, 6, 25), t_open, date(2025, 6, 25), date(2025, 6, 24), "src_p1", "PKG001", "IB2500001", "VND", Decimal("950000000"), "IS_PUBLISH"),
            ("rev_r2", "res2", "rid2", "00", "00", date(2025, 6, 26), t_open, date(2025, 6, 26), date(2025, 6, 25), "src_p2", "PKG002", None, "VND", Decimal("1800000000"), "IS_PUBLISH"),
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
            # Single-lot result rev_r1
            ("lot_1", "rev_r1", "1", "Lo duy nhat", Decimal("1000000000"), Decimal("950000000"), "VND"),
            # Multi-lot result rev_r2 (2 lots)
            ("lot_2a", "rev_r2", "1", "Lo 2A", Decimal("1000000000"), Decimal("900000000"), "VND"),
            ("lot_2b", "rev_r2", "2", "Lo 2B", Decimal("1000000000"), Decimal("900000000"), "VND"),
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
            ("ac2", "rev_r2", "lot_2a", "VN02", "Cong ty B", "0102", False, None, None, Decimal("900000000"), "PRIMARY", 1),
            ("ac3", "rev_r2", "lot_2b", "VN03", "Cong ty C", "0103", False, None, None, Decimal("900000000"), "PRIMARY", 1),
        ],
        schema=ctr_schema,
    ).createOrReplaceTempView("silver_award_contractor")

    # contract_revision + contract_performance_revision
    c_schema = StructType(
        [
            StructField("entity_id", StringType(), False),
            StructField("contract_code", StringType(), True),
            StructField("result_source_id", StringType(), True),
            StructField("notice_no", StringType(), True),
            StructField("price_after_tax", DecimalType(38, 6), True),
        ]
    )
    spark.createDataFrame(
        [
            ("ctr1", "HD001", "rid1", "IB2500001", Decimal("900000000")),
            ("ctr2", "HD002", "rid2", None, Decimal("1750000000")),
        ],
        schema=c_schema,
    ).createOrReplaceTempView("silver_contract_revision")

    cp_schema = StructType(
        [
            StructField("contract_code", StringType(), True),
            StructField("public_date", DateType(), True),
            StructField("planned_completion_date", DateType(), True),
            StructField("actual_completion_date", DateType(), True),
        ]
    )
    spark.createDataFrame(
        [("HD001", date(2025, 10, 20), date(2025, 9, 30), date(2025, 10, 15))],
        schema=cp_schema,
    ).createOrReplaceTempView("silver_contract_performance_revision")

    # opening_revision + opening_submission
    op_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("notify_no", StringType(), True),
            StructField("package_id", StringType(), True),
            StructField("package_no", StringType(), True),
            StructField("public_date", DateType(), True),
            StructField("scheduled_open_date", TimestampType(), True),
            StructField("actual_open_date", TimestampType(), True),
            StructField("total_bidders", LongType(), True),
        ]
    )
    spark.createDataFrame(
        [("rev_o1", "opn1", "IB2500001", "src_p1", "PKG001", date(2025, 6, 16), t_open, t_open, 3)],
        schema=op_schema,
    ).createOrReplaceTempView("silver_opening_revision")

    sub_schema = StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("submission_id", StringType(), False),
            StructField("contractor_code", StringType(), True),
            StructField("contractor_name", StringType(), True),
            StructField("tech_result", StringType(), True),
            StructField("discount_rate", DecimalType(10, 4), True),
            StructField("currency", StringType(), True),
            StructField("bid_price", DecimalType(38, 6), True),
            StructField("final_bid_price", DecimalType(38, 6), True),
        ]
    )
    spark.createDataFrame(
        [
            # VND bidder 1 with discount (980M -> 960M)
            ("rev_o1", "s1", "VN01", "Cong ty A", "1", Decimal("2.0408"), "VND", Decimal("980000000"), Decimal("960000000")),
            # VND bidder 2 normal (990M)
            ("rev_o1", "s2", "VN02", "Cong ty B", "0", Decimal("0"), "VND", Decimal("990000000"), Decimal("990000000")),
            # USD bidder 3: counted in bidder/tech counts, EXCLUDED from VND monetary sums!
            ("rev_o1", "s3", "US01", "Global Corp", "1", Decimal("0"), "USD", Decimal("50000"), Decimal("50000")),
        ],
        schema=sub_schema,
    ).createOrReplaceTempView("silver_opening_submission")


def test_phase3b_savings_duration_competition(spark) -> None:
    _register_full_silver_fixtures(spark)
    cfg = GoldBuildConfig(
        event_start=date(2025, 6, 1),
        event_end=date(2025, 6, 30),
        observation_day=date(2025, 10, 20),
    )

    build_base_dimensions(spark, cfg)
    build_selection_views(spark)
    build_business_dimensions(spark, cfg)
    build_demand(spark, cfg)
    build_notice(spark, cfg)
    build_award(spark, cfg)
    sav_out = build_savings(spark, cfg)
    dur_out = build_duration(spark, cfg)
    comp_out = build_competition(spark, cfg)

    # 1. Savings checks: single-lot contract recovered, multi-lot excluded
    savs = {r["package_key"]: r for r in sav_out["fact_savings"].collect() if r["package_key"] == "PKG:pkg1"}
    s1 = savs["PKG:pkg1"]
    assert s1["is_descriptive_eligible"] is True
    assert s1["descriptive_difference_amount"] == Decimal("50000000.000000")
    assert s1["is_contract_eligible"] is True
    assert s1["contract_amount"] == Decimal("900000000.000000")
    assert s1["contract_saving_amount"] == Decimal("100000000.000000")

    multi_savs = [r for r in sav_out["fact_savings"].collect() if r["package_key"] == "PKG:pkg2"]
    assert len(multi_savs) == 2
    assert all(r["is_contract_eligible"] is False and r["contract_amount"] is None for r in multi_savs)

    # 2. Duration checks (all 7 metrics)
    rounds = {r["round_business_key"]: r for r in dur_out["fact_selection_round"].collect()}
    r1 = rounds["RND:NOT:VN:IB2500001"]
    assert r1["plan_to_notice_days"] == 5
    assert r1["notice_to_close_hours"] == Decimal("240.000000")
    assert r1["close_to_open_hours"] == Decimal("2.000000")
    assert r1["close_to_first_award_days"] == 9
    assert r1["close_to_complete_award_days"] == 9
    assert r1["open_to_award_days"] == 9
    assert r1["planned_to_actual_delay_days"] == 15

    # 3. Competition checks (VND-only sums, non-VND bidder still counted)
    comps = {r["opening_key"]: r for r in comp_out["fact_bidding_competition"].collect()}
    c1 = comps["OPN:rev_o1"]
    assert c1["total_bidders"] == 3
    assert c1["multi_bidder_count"] == 1
    assert c1["tech_passed_count"] == 2
    assert c1["tech_failed_count"] == 1
    assert c1["discounted_bidder_count"] == 1
    assert c1["total_bid_amount"] == Decimal("1970000000.000000")
    assert c1["total_discount_amount"] == Decimal("20000000.000000")
    assert c1["total_final_bid_amount"] == Decimal("1950000000.000000")
