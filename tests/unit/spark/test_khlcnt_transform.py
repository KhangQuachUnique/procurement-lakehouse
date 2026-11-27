"""Unit tests for Spark Silver Vertical Slice #2 (khlcnt: plan + package + location + lot)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from procurement_spark.config import SparkSilverSettings
from procurement_spark.resources.khlcnt import transform_khlcnt
from procurement_spark.spark_session import build_spark_session


@pytest.fixture(scope="module")
def spark():
    cfg = SparkSilverSettings(
        app_database_url="postgresql://unused",
        spark_master="local[2]",
        spark_driver_memory="512m",
        spark_shuffle_partitions=4,
        java_home="/usr/lib/jvm/java-21-openjdk",
    )
    session = build_spark_session(cfg, local_unit_test=True)
    yield session
    session.stop()


def _bronze_df(spark, rows: list[dict]):
    schema = T.StructType(
        [
            T.StructField("payload", T.StringType(), True),
            T.StructField("source_id", T.StringType(), True),
            T.StructField("source_version", T.StringType(), True),
            T.StructField("source_date", T.DateType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("ingested_at", T.TimestampType(), True),
            T.StructField("content_hash", T.StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def test_khlcnt_transform_plan_package_locations_lots_and_quarantine(spark):
    ts = datetime(2025, 1, 15, 8, 0, tzinfo=UTC)
    plan_payload = json.dumps(
        {
            "bidPoBidpPlanProjectDetailView": {
                "id": "plan-uuid-1",
                "planNo": "PL2500001",
                "name": "Kế hoạch LCNT năm 2025",
                "investorName": "Sở GTVT",
                "pid": "proj-uuid-1",
                "pno": "PR2500001",
                "planType": "DTPT",
                "investTotal": "9876543210987654321.123456",
                "investTotalUnit": "VND",
                "decisionDate": "2025-01-10",
                "decisionNo": "QD-10",
                "decisionAgency": "UBND",
                "bidPack": "5",
                "location": "Hà Nội",
                "publicDate": "2025-01-15",
                "status": "APPROVED",
            }
        }
    )
    pkg_payload = json.dumps(
        {
            "id": "pkg-uuid-1",
            "bidNo": "BP2500001",
            "bidName": "Gói thầu số 1",
            "planId": "plan-uuid-1",
            "planNo": "PL2500001",
            "bidPrice": 104834825617.4,  # unquoted large JSON float -> exact via primitivesAsString=true
            "bidPriceUnit": "VND",
            "processApply": "LDT",
            "bidField": "XL",
            "bidForm": "DTRR",
            "bidMode": "1_MTHS",
            "ctype": "TG",
            "cperiod": "180",
            "cperiodUnit": "D",
            "isDomestic": "1",
            "isInternet": "true",
            "bidStartYear": "2025",
            "bidStartQuarter": "1",
            "bidStartMonth": "0",  # 0 sentinel -> NULL
            "createdDate": "2025-01-15T09:00:00Z",
            "bidLocation": [
                {
                    "provCode": "|01|",
                    "provName": "Hà Nội",
                    "districtCode": "001",
                    "districtName": "Ba Đình",
                }
            ],
            "bidpBidLotList": [
                {
                    "id": "lot-1",
                    "lotNo": "L01",
                    "lotName": "Phần 1",
                    "lotPrice": "2500000000.25",
                    # no lotPriceUnit -> inherits package currency "VND"
                    "quantity": "10.5",
                    "uom": "bộ",
                    "pricePlan": "238095238.119048",
                }
            ],
        }
    )
    bad_plan_payload = json.dumps({"unexpected": True})

    plan_df = _bronze_df(
        spark,
        [
            {
                "payload": plan_payload,
                "source_id": "plan-uuid-1",
                "source_version": "00",
                "source_date": date(2025, 1, 15),
                "run_id": "run-1",
                "ingested_at": ts,
                "content_hash": "hp1",
            },
            {
                "payload": bad_plan_payload,
                "source_id": "bad-plan",
                "source_version": "00",
                "source_date": date(2025, 1, 15),
                "run_id": "run-1",
                "ingested_at": ts,
                "content_hash": "hp2",
            },
        ],
    )
    pkg_df = _bronze_df(
        spark,
        [
            {
                "payload": pkg_payload,
                "source_id": "pkg-uuid-1",
                "source_version": "00",
                "source_date": date(2025, 1, 15),
                "run_id": "run-1",
                "ingested_at": ts,
                "content_hash": "hk1",
            }
        ],
    )

    out = transform_khlcnt(
        {"khlcnt_plan_detail": plan_df, "khlcnt_bid_package_detail": pkg_df}
    )
    plans = out.tables["plan_revision"].collect()
    pkgs = out.tables["package_revision"].collect()
    locs = out.tables["package_location"].collect()
    lots = out.tables["package_lot"].collect()
    dq_issues = out.tables["silver_dq_issues"].collect()
    quars = out.tables["silver_quarantine"].collect()
    metrics = out.per_day_metrics.collect()

    assert "dq_issues" not in out.tables["package_revision"].columns
    assert len(dq_issues) == 0
    assert len(plans) == 1
    assert plans[0]["plan_no"] == "PL2500001"
    assert plans[0]["total_investment"] == Decimal("9876543210987654321.123456")
    assert plans[0]["bid_pack"] == 5

    assert len(pkgs) == 1
    assert pkgs[0]["bid_price"] == Decimal("104834825617.400000")
    assert pkgs[0]["dq_status"] == "VALID"
    assert pkgs[0]["is_domestic"] is True
    assert pkgs[0]["is_internet"] is True
    assert pkgs[0]["bid_start_month"] is None  # 0 sentinel normalized to NULL

    assert len(locs) == 1
    assert locs[0]["province_code"] == "01"

    assert len(lots) == 1
    assert lots[0]["lot_price"] == Decimal("2500000000.250000")
    assert lots[0]["currency"] == "VND"  # inherited from package
    assert lots[0]["quantity"] == Decimal("10.500000")

    assert len(quars) == 1
    assert quars[0]["resource"] == "khlcnt"
    assert quars[0]["bronze_table"] == "khlcnt_plan_detail"
    assert quars[0]["reason"] == "missing_root"

    assert len(metrics) == 1
    assert metrics[0]["raw_input_rows"] == 3
    assert metrics[0]["valid_rows"] == 2
    assert metrics[0]["quarantined_rows"] == 1
