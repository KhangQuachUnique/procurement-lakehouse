"""Unit tests for Gold automated 3-part reconciliation."""

import duckdb
import pytest

from procurement.gold.application.reconciliation import reconcile_gold
from procurement.gold.application.warehouse import ensure_gold_tables


@pytest.fixture
def mem_db():
    conn = duckdb.connect(":memory:")
    ensure_gold_tables(conn, namespace="gold_v2")
    yield conn
    conn.close()


def test_reconciliation_on_empty_tables(mem_db):
    report = reconcile_gold(mem_db, gold_schema="gold_v2", silver_schema="silver")
    assert report.is_valid is True
    assert report.gold_planned_amount == 0
    assert report.gold_award_amount == 0
    assert len(report.orphan_fks) == 0
    assert len(report.errors) == 0


def test_reconciliation_detects_orphan_fks(mem_db):
    mem_db.execute(
        """
        INSERT INTO gold_v2.fact_demand_daily (
            group_key, report_date, geography_key, sector_key,
            value_band_key, currency_key, package_count, eligible_amount_count, planned_amount_sum
        ) VALUES (
            'test_grp', '2025-01-01', 'GEO_NONEXISTENT', 'SEC_GOODS',
            'BAND_1', 'VND', 1, 1, 1000000
        )
        """
    )
    report = reconcile_gold(mem_db, gold_schema="gold_v2", silver_schema="silver")
    assert report.is_valid is False
    assert "fact_demand_daily.geography_key" in report.orphan_fks
    assert report.orphan_fks["fact_demand_daily.geography_key"] == 1
