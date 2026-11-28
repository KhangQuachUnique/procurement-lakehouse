"""Unit tests for Spark Silver Vertical Slice #1 (project) and batch planner."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from procurement_spark.platform.config import SparkSilverSettings
from procurement_spark.silver.control.manifest import BronzeDayManifest, BronzeFileSpec
from procurement_spark.silver.control.planner import plan_batches
from procurement_spark.silver.control.receipts import SilverReceipt
from procurement_spark.silver.resources.project import PROJECT_QUALITY_RULES, transform_project
from procurement_spark.silver.quality.rules import evaluate_inline_rules
from procurement_spark.platform.spark_session import build_spark_session


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


def test_project_transform_exact_decimal_locations_dq_and_quarantine(spark):
    ts = datetime(2025, 1, 15, 8, 0, tzinfo=UTC)
    valid_payload = json.dumps(
        {
            "projectDTO": {
                "id": "proj-uuid-1",
                "no": "PR2500001",
                "name": "  Dự án đường cao tốc  ",
                "investorCode": "vn0001",
                "investorName": "Ban QLDA 1",
                "investTotal": "12345678901234567890.123456",
                "investTotalUnit": "VND",
                "decisionNo": "QD-01",
                "decisionDate": "2024-12-01",
                "publicDate": "2025-01-15T10:00:00Z",
                "provCode": "|26|",
                "districtCode": "||",
                "plocation": json.dumps(
                    [
                        {
                            "provCode": "|26|",
                            "provName": "Tỉnh Vĩnh Phúc",
                            "districtCode": "243",
                            "districtName": "Thành phố Vĩnh Yên",
                            "wardCode": "08707",
                            "wardName": "Phường Tích Sơn",
                        }
                    ]
                ),
                "status": "PUBLISHED",
            }
        }
    )
    warn_payload = json.dumps(
        {
            "projectDTO": {
                "id": "proj-uuid-2",
                "no": "PR2500002",
                "name": "Dự án tương lai 2027",
                "investTotal": "5000000",
                # missing currency -> unknown_currency warn
                "decisionDate": "2027-06-01",  # valid 2027 preserved, > publicDate -> invalid_date_order
                "publicDate": "2202-01-16",  # known typo -> 2022-01-16
                "provCode": "|01|",
                "provName": "Hà Nội",
            }
        }
    )
    missing_id_payload = json.dumps({"projectDTO": {"no": "PR-NO-ID"}})

    df = _bronze_df(
        spark,
        [
            {
                "payload": valid_payload,
                "source_id": "proj-uuid-1",
                "source_version": "00",
                "source_date": date(2025, 1, 15),
                "run_id": "run-1",
                "ingested_at": ts,
                "content_hash": "h1",
            },
            {
                "payload": warn_payload,
                "source_id": "proj-uuid-2",
                "source_version": "00",
                "source_date": date(2025, 1, 16),
                "run_id": "run-1",
                "ingested_at": ts,
                "content_hash": "h2",
            },
            {
                "payload": missing_id_payload,
                "source_id": "bad-1",
                "source_version": "00",
                "source_date": date(2025, 1, 16),
                "run_id": "run-1",
                "ingested_at": ts,
                "content_hash": "h3",
            },
        ],
    )

    out = transform_project(df)
    revs = {r["project_no"]: r for r in out.tables["project_revision"].collect()}
    locs = out.tables["project_location"].collect()
    dq_issues = out.tables["silver_dq_issues"].collect()
    quars = out.tables["silver_quarantine"].collect()
    day_metrics = {r["source_date"]: r for r in out.per_day_metrics.collect()}

    assert "dq_issues" not in out.tables["project_revision"].columns
    assert len(revs) == 2
    r1 = revs["PR2500001"]
    assert r1["title"] == "Dự án đường cao tốc"
    assert r1["total_investment"] == Decimal("12345678901234567890.123456")
    assert r1["prov_code"] == "26"
    assert r1["district_code"] is None
    assert r1["dq_status"] == "VALID"

    r2 = revs["PR2500002"]
    assert r2["public_date"] == date(2022, 1, 16)
    assert r2["decision_date"] == date(2027, 6, 1)  # 2027 NOT corrupted!
    assert r2["dq_status"] == "WARN"
    assert {(i["resource"], i["table_name"], i["issue_code"]) for i in dq_issues} == {
        ("project", "project_revision", "unknown_currency"),
        ("project", "project_revision", "invalid_date_order"),
    }

    # Locations: 1 from plocation JSON array + 1 from root fallback
    assert len(locs) == 2
    loc_by_prov = {r["province_code"]: r for r in locs}
    assert loc_by_prov["26"]["location_name"] == "Phường Tích Sơn, Thành phố Vĩnh Yên, Tỉnh Vĩnh Phúc"
    assert loc_by_prov["01"]["province_name"] == "Hà Nội"

    # Shared quarantine
    assert len(quars) == 1
    assert quars[0]["resource"] == "project"
    assert quars[0]["reason"] == "missing_identity"

    # Per-day metrics are strictly scoped per day (not batch totals)
    assert day_metrics[date(2025, 1, 15)]["raw_input_rows"] == 1
    assert day_metrics[date(2025, 1, 15)]["quarantined_rows"] == 0
    assert day_metrics[date(2025, 1, 16)]["raw_input_rows"] == 2
    assert day_metrics[date(2025, 1, 16)]["quarantined_rows"] == 1


def test_plan_batches_backfill_vs_daily_and_idempotency():
    f1 = BronzeFileSpec("project_detail", "b", "k1.parquet", 10, "s1")
    manifests = {
        date(2025, 1, 1): BronzeDayManifest("project", date(2025, 1, 1), "c1", "a1", 10, (f1,)),
        date(2025, 1, 2): BronzeDayManifest("project", date(2025, 1, 2), "c2", "a2", 10, (f1,)),
        date(2025, 2, 1): BronzeDayManifest("project", date(2025, 2, 1), "c3", "a3", 10, (f1,)),
    }
    published = {
        date(2025, 1, 1): SilverReceipt(
            "project", date(2025, 1, 1), "silver_spark", "c1", "v1", "b1", "PUBLISHED", {}, {}
        ),
    }

    batches, missing = plan_batches(
        resource="project",
        transform_version="v1",
        manifests=manifests,
        published_receipts=published,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 2, 1),
        mode="backfill",
    )
    # 2025-01-01 skipped (already published with c1 & v1); remaining 2 days grouped into 2025-01 & 2025-02
    assert [b.label for b in batches] == ["2025-01", "2025-02"]
    assert [d.source_date for d in batches[0].days] == [date(2025, 1, 2)]
    assert [d.source_date for d in batches[1].days] == [date(2025, 2, 1)]
    assert len(missing) == 29  # Jan 3..31 missing in mock manifests

    # Inline rule evaluation test
    ok, _ = evaluate_inline_rules(PROJECT_QUALITY_RULES, {"raw_input_rows": 100, "quarantined_rows": 2})
    assert ok is True
    blocked, _ = evaluate_inline_rules(PROJECT_QUALITY_RULES, {"raw_input_rows": 100, "quarantined_rows": 10})
    assert blocked is False
