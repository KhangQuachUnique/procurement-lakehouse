"""Unit tests for Spark Silver resources: notice, contract_performance, contractor_result, bid_opening, and architecture."""

from __future__ import annotations

import ast
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import types as T

from procurement_spark.config import SparkSilverSettings
from procurement_spark.resources.bid_opening import transform_bid_opening
from procurement_spark.resources.contract_performance import transform_contract_performance
from procurement_spark.resources.contractor_result import transform_contractor_result
from procurement_spark.resources.notice import transform_notice
from procurement_spark.resources.registry import SPARK_RESOURCES
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


def _row(payload_obj: dict, source_id: str, source_date: date = date(2025, 1, 15)) -> dict:
    return {
        "payload": json.dumps(payload_obj),
        "source_id": source_id,
        "source_version": "00",
        "source_date": source_date,
        "run_id": "run-1",
        "ingested_at": datetime(2025, 1, 15, 8, 0, tzinfo=UTC),
        "content_hash": f"hash-{source_id}",
    }


def test_registry_has_all_six_resources():
    assert set(SPARK_RESOURCES.keys()) == {
        "project",
        "khlcnt",
        "notice",
        "contract_performance",
        "contractor_result",
        "bid_opening",
    }


def test_notice_transform_all_three_families(spark):
    std_payload = {
        "bidoNotifyContractorM": {
            "id": "n-std-1",
            "notifyNo": "IB2500001",
            "notifyVersion": "00",
            "bidId": "pkg-1",
            "bidNo": "PKG-01",
            "bidName": "Gói thầu 1",
            "bidPrice": 104834825617.4,
            "bidPriceUnit": "VND",
            "bidEstimatePrice": 100000000000,
            "isDomestic": True,
            "publicDate": "2025-01-15T08:00:00Z",
            "bidOpenDate": "2025-01-25T09:00:00Z",
            "bidCloseDate": "2025-01-25T08:00:00Z",
            "lotDTOList": [{"id": "lot-1", "lotNo": "1", "lotName": "Phần 1", "lotPrice": 50000000000}],
        },
        "bidpPlanDetail": {"id": "pkg-1", "planId": "plan-1", "isMultiLot": True, "bidField": "XL"},
        "bidpBidLocationList": [{"id": "loc-1", "provCode": "|01|", "provName": "Hà Nội"}],
    }
    vk_payload = {
        "bidoNotifyContractorP": {
            "id": "n-vk-1",
            "notifyNo": "IB2500002",
            "notifyVersion": "00",
            "bidName": "Gói ADB",
            "bidPrice": 2500000,
            "bidPriceUnit": "USD",
            "bidGuaranteeValue": 50000,
            "bidGuaranteeUnit": "USD",
            "publicDate": "2025-01-15",
        }
    }
    reoffer_payload = {
        "id": "n-re-1",
        "reofferNo": "RE2500001",
        "reofferVersion": "00",
        "bidName": "Chào lại giá",
        "priceInit": 100000000,
        "reofferPriceUnit": "VND",
        "publicDate": "2025-01-15",
        "contractorsResult": [
            {
                "id": "sub-1",
                "reofferSubmissionId": "rs-1",
                "contractorCode": "vn001",
                "contractorName": "Công ty A",
                "reofferPrice": 95000000,
                "reofferPriceFinal": 94000000,
                "reofferPriceUnit": "VND",
                "isNewest": True,
            }
        ],
    }

    out = transform_notice(
        {
            "notify_contractor_standard_detail": _bronze_df(spark, [_row(std_payload, "n-std-1")]),
            "notify_contractor_vk_adb_detail": _bronze_df(spark, [_row(vk_payload, "n-vk-1")]),
            "notify_contractor_reoffer_detail": _bronze_df(spark, [_row(reoffer_payload, "n-re-1")]),
        }
    )
    revs = {r["source_family"]: r for r in out.tables["notice_revision"].collect()}
    assert set(revs.keys()) == {"standard", "vk_adb", "reoffer"}
    assert revs["standard"]["bid_price"] == Decimal("104834825617.400000")
    assert revs["standard"]["plan_id"] == "plan-1"
    assert revs["standard"]["estimate_currency"] == "VND"
    assert revs["vk_adb"]["guarantee_amount"] == Decimal("50000.000000")
    assert revs["reoffer"]["reoffer_no"] == "RE2500001"
    assert out.tables["notice_lot"].count() == 1
    assert out.tables["notice_location"].count() == 1
    assert out.tables["notice_reoffer_submission"].count() == 1


def test_contract_performance_transform(spark):
    c_payload = {
        "contract": {
            "id": "ctr-1",
            "contractCode": "HD2500001",
            "contractNo": "HD-01/2025",
            "contractName": "Hợp đồng xây lắp",
            "priceAfter": 5000000000.5,
            "priceAfterUnit": "VND",
            "contractSignDate": "2025-01-10",
            "contractEffectiveDate": "2025-01-12",
            "publicDate": "2025-01-15",
            "contractorCode": "vn001",
            "contractorName": "Nhà thầu A",
        }
    }
    p_payload = {
        "performance": {
            "id": "perf-1",
            "idNo": "DG2500001",
            "contractCode": "HD2500001",
            "contractStatus": "01",
            "isContractComplete": True,
            "publicDate": "2025-02-01",
        },
        "resolution": {"status": "resolved"},
    }
    out = transform_contract_performance(
        {
            "contract_detail": _bronze_df(spark, [_row(c_payload, "ctr-1")]),
            "contract_performance_detail": _bronze_df(spark, [_row(p_payload, "perf-1")]),
        }
    )
    c_rows = out.tables["contract_revision"].collect()
    p_rows = out.tables["contract_performance_revision"].collect()
    assert len(c_rows) == 1
    assert c_rows[0]["price_after_tax"] == Decimal("5000000000.500000")
    assert c_rows[0]["dq_status"] == "VALID"
    assert len(p_rows) == 1
    assert p_rows[0]["is_completed"] is True
    assert p_rows[0]["dq_status"] == "VALID"


def test_contractor_result_transform_winners_consortium_and_suspect_date(spark):
    clean_result = {
        "bideContractorInputResultDTO": {
            "id": "res-1",
            "resultVersion": "00",
            "notifyNo": "IB2500001",
            "bidPriceUnit": "VND",
            "decisionDate": "2025-01-10",
            "publicDate": "2025-01-15",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPrice": 1000000000,
                    "contractorList": [
                        {
                            "id": "c-win",
                            "orgCode": "vn001",
                            "orgFullname": "Công ty Thắng",
                            "bidResult": 1,
                            "bidWiningPrice": 950000000.25,
                        },
                        {
                            "id": "c-lose",
                            "orgCode": "vn002",
                            "orgFullname": "Công ty Thua",
                            "bidResult": 0,
                        },
                    ],
                }
            ],
        }
    }
    consortium_and_old_date = {
        "bideContractorInputResultDTO": {
            "id": "res-2",
            "notifyNo": "IB2500002",
            "bidPriceUnit": "VND",
            "decisionDate": "1987-02-20",
            "publicDate": "2025-01-15",
            "lotResultDTO": [
                {
                    "id": "lot-2",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c-v1",
                            "orgCode": "vn010",
                            "orgFullname": "Liên danh 1",
                            "ventureCode": "LD01",
                            "bidResult": "1",
                            "bidWiningPrice": 500000000,
                        },
                        {
                            "id": "c-v2",
                            "orgCode": "vn011",
                            "orgFullname": "Liên danh 2",
                            "ventureCode": "LD01",
                            "bidResult": "1",
                            "bidWiningPrice": 500000000,
                        },
                    ],
                }
            ],
        }
    }

    out = transform_contractor_result(
        {
            "contractor_result_detail": _bronze_df(
                spark,
                [_row(clean_result, "res-1"), _row(consortium_and_old_date, "res-2")],
            )
        }
    )
    revs = {r["result_id"]: r for r in out.tables["result_revision"].collect()}
    assert revs["res-1"]["total_winning_price"] == Decimal("950000000.250000")
    assert revs["res-1"]["dq_status"] == "VALID"

    # res-2 has 2 consortium winners -> total_winning_price suppressed + WARN + suspect_decision_date
    assert revs["res-2"]["total_winning_price"] is None
    assert revs["res-2"]["decision_date"] == date(1987, 2, 20)
    assert revs["res-2"]["dq_status"] == "WARN"

    # Only confirmed winners in award_contractor (1 from res-1 + 2 from res-2 = 3)
    assert out.tables["award_contractor"].count() == 3
    issues = {(r["issue_code"], r["field_name"]) for r in out.tables["silver_dq_issues"].collect()}
    assert ("unverified_consortium_allocation", "total_winning_price") in issues
    assert ("suspect_decision_date", "decision_date") in issues


def test_bid_opening_transform_ldt_and_khac_shapes(spark):
    ldt_single = {
        "notify": {
            "bidNoContractorResponse": {
                "bidNotification": {
                    "notifyNo": "IB2500100",
                    "notifyVersion": "00",
                    "bidName": "Gói LDT 1 túi",
                    "publicDate": "2025-01-10",
                    "bidOpenDate": "2025-01-20T09:00:00Z",
                }
            }
        },
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2025-01-20T09:05:00Z",
                "countContractor": 2,
                "isMultiLot": True,
            }
        },
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "sub-1",
                        "contractorCode": "vn001",
                        "contractorName": "Công ty 1",
                        "bidPrice": 1200000000,
                        "bidPriceUnit": "VND",
                        "saleNumber": 5.5,
                        "bidFinalPrice": 1134000000,
                    }
                ]
            }
        },
        "lot_open_detail": [
            {
                "id": "lot-d-1",
                "lotNo": "1",
                "contractorCode": "vn001",
                "lotPrice": 1200000000,
                "lotFinalPrice": 1134000000,
                "discountPercent": 5.5,
                "passedYn": True,
                "ranking": 1,
            }
        ],
    }
    khac_two = {
        "opening_out_technical": {
            "bidoNotifyContractorP": {
                "notifyNo": "IB2500200",
                "notifyVersion": "00",
                "bidName": "Gói KHAC 2 túi",
                "publicDate": "2025-01-12",
            },
            "bidoBidroundOutMngViewDTO": {
                "successBidOpenDate": "2025-01-22T10:00:00Z",
                "successBidOpenDateTc": None,
                "countContractor": 1,
            },
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "sub-k1",
                        "contractorCode": "vn099",
                        "contractorName": "Công ty KHAC",
                        "lotNoValueDTOList": [
                            {
                                "id": "nl-1",
                                "lotNo": "1",
                                "lotPrice": 800000000,
                                "lotPriceUnit": "VND",
                            }
                        ],
                    }
                ]
            },
        }
    }

    out = transform_bid_opening(
        {
            "bid_opening_detail": _bronze_df(
                spark,
                [_row(ldt_single, "IB2500100"), _row(khac_two, "IB2500200")],
            )
        }
    )
    revs = {r["notify_no"]: r for r in out.tables["opening_revision"].collect()}
    assert revs["IB2500100"]["opening_shape"] == "ldt_single_envelope"
    assert revs["IB2500100"]["collection_status"] == "complete"
    assert revs["IB2500100"]["total_bidders"] == 2
    assert revs["IB2500200"]["opening_shape"] == "khac_two_envelope"
    assert revs["IB2500200"]["collection_status"] == "awaiting_financial"

    # Phases: 1 (single) + 2 (technical, financial) = 3
    assert out.tables["opening_phase"].count() == 3
    assert out.tables["opening_submission"].count() == 2
    lots = {r["contractor_code"]: r for r in out.tables["opening_lot_submission"].collect()}
    assert lots["vn001"]["submission_id"] is not None
    assert lots["vn001"]["discount_rate"] == Decimal("5.5000")
    assert lots["vn099"]["lot_price"] == Decimal("800000000.000000")


def test_spark_package_has_zero_legacy_silver_imports():
    """Enforce clean package isolation: procurement_spark must never import procurement.silver."""
    root = Path(__file__).resolve().parents[3] / "src" / "procurement_spark"
    py_files = list(root.rglob("*.py"))
    assert len(py_files) > 15
    for path in py_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("procurement.silver"), f"Forbidden import in {path}: {alias.name}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("procurement.silver"), f"Forbidden import in {path}: {node.module}"
