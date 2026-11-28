"""Unit tests for Gold Spark Tier 2 Marts (17 public tables) and gold-spark CLI."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest

from procurement.gold.cli import main as gold_spark_cli_main
from procurement.gold.core.award import build_award
from procurement.gold.core.competition import build_competition
from procurement.gold.core.demand import build_demand
from procurement.gold.core.dimensions_base import GoldBuildConfig, build_base_dimensions
from procurement.gold.core.dimensions_biz import build_business_dimensions
from procurement.gold.core.duration import build_duration
from procurement.gold.core.notice import build_notice
from procurement.gold.core.savings import build_savings
from procurement.gold.core.selection import build_selection_views
from procurement.gold.functions import register_gold_functions
from procurement.gold.marts.dimensions import build_public_dimensions
from procurement.gold.marts.facts import build_public_facts
from procurement.gold.schema import MART_PRIMARY_KEYS, MART_SPECS
from procurement.spark.config import SparkSilverSettings, _detect_java_home
from procurement.spark.spark_session import build_spark_session
from tests.unit.spark.gold.test_phase3b_savings_duration_competition import (
    _register_full_silver_fixtures,
)


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


def test_all_17_public_marts_schema_and_invariants(spark) -> None:
    _register_full_silver_fixtures(spark)
    cfg = GoldBuildConfig(
        event_start=date(2025, 6, 1),
        event_end=date(2025, 10, 31),
        observation_day=date(2025, 10, 31),
    )

    build_base_dimensions(spark, cfg)
    sel = build_selection_views(spark)
    for k in ("_gtmp_cand_package", "_gtmp_cand_notice", "_gtmp_cand_result"):
        sel[k].localCheckpoint(eager=True).createOrReplaceTempView(k)
    build_business_dimensions(spark, cfg)
    build_demand(spark, cfg)["fact_procurement_demand"].localCheckpoint(eager=True).createOrReplaceTempView(
        "core_fact_procurement_demand"
    )
    build_notice(spark, cfg)["fact_tender_notice"].localCheckpoint(eager=True).createOrReplaceTempView(
        "core_fact_tender_notice"
    )
    awd = build_award(spark, cfg)
    awd["fact_award"].localCheckpoint(eager=True).createOrReplaceTempView("core_fact_award")
    awd["dim_party"].localCheckpoint(eager=True).createOrReplaceTempView("core_dim_party")
    build_savings(spark, cfg)["fact_savings"].localCheckpoint(eager=True).createOrReplaceTempView(
        "core_fact_savings"
    )
    build_duration(spark, cfg)["fact_selection_round"].localCheckpoint(eager=True).createOrReplaceTempView(
        "core_fact_selection_round"
    )
    build_competition(spark, cfg)["fact_bidding_competition"].localCheckpoint(eager=True).createOrReplaceTempView(
        "core_fact_bidding_competition"
    )

    dims = build_public_dimensions(spark)
    facts = build_public_facts(spark, cfg)
    all_17 = {**dims, **facts}

    assert set(all_17.keys()) == set(MART_SPECS.keys())
    assert len(all_17) == 17

    for tbl_name, df in all_17.items():
        expected_cols = [c for c, _ in MART_SPECS[tbl_name].columns]
        assert df.columns == expected_cols, f"{tbl_name} columns mismatch: {df.columns} != {expected_cols}"
        pk = MART_PRIMARY_KEYS[tbl_name]
        rows = df.collect()
        pks = [r[pk] for r in rows]
        assert all(v is not None for v in pks), f"{tbl_name} has NULL PK"
        assert len(pks) == len(set(pks)), f"{tbl_name} has duplicate PKs"

    # Cross-fact award == supplier reconciliation
    awd_total = sum(r["award_amount_sum"] for r in facts["fact_award_daily"].collect())
    sup_total = sum(r["award_amount_sum"] for r in facts["fact_supplier_daily"].collect())
    assert awd_total == sup_total == Decimal("2750000000.000000")

    # Value band assigned (1B and 2B VND -> 500M_TO_5B)
    dem_rows = facts["fact_demand_daily"].collect()
    assert all(
        r["value_band_key"] == "BAND_REF_b5c428691904824ea4a65203419948536c9a83111424473c49e3f0f0dcba62b4"
        for r in dem_rows
    )

    # Contract savings recovered on single-lot award
    sav_rows = facts["fact_savings_daily"].collect()
    assert sum(r["contract_sample_count"] for r in sav_rows) == 1
    assert sum(r["contract_saving_sum"] for r in sav_rows) == Decimal("100000000.000000")

    # Competition VND-only sums
    cmp_rows = facts["fact_bidding_competition_daily"].collect()
    assert sum(r["total_bidders"] for r in cmp_rows) == 3
    assert sum(r["total_bid_amount"] for r in cmp_rows) == Decimal("1970000000.000000")


def test_gold_spark_cli_dispatch() -> None:
    with patch(
        "procurement.gold.cli.run_spark_gold_rebuild",
        return_value={
            "status": "PUBLISHED",
            "gold_namespace": "gold_spark",
            "silver_namespace": "silver_spark",
            "award_amount_sum": "0",
            "table_counts": {"dim_date": 10},
            "wall_s": 1.0,
        },
    ) as mock_rebuild, patch("procurement.gold.cli.settings_from_cli_args") as mock_cfg:
        mock_cfg.return_value.spark_master = "spark://192.168.1.11:7077"
        rc = gold_spark_cli_main(["rebuild", "--k3s", "--namespace", "gold_spark_test"])
        assert rc == 0
        assert mock_rebuild.call_args.kwargs["gold_namespace"] == "gold_spark_test"

    with patch("procurement.gold.cli.maintenance_main", return_value=0) as mock_maint:
        assert gold_spark_cli_main(["maintain", "--k3s", "--dry-run-orphans"]) == 0
        mock_maint.assert_called_once_with(["--namespace", "gold_spark", "--k3s", "--dry-run-orphans"])
