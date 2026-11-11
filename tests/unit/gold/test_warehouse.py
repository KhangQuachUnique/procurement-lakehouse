"""Unit tests for Gold warehouse provisioning, geography seeding, and view promotion."""

import duckdb
import pytest

from procurement.gold.application.warehouse import ensure_gold_tables, promote_views
from procurement.gold.domain.schema import TABLES


@pytest.fixture
def mem_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_ensure_gold_tables_creates_16_tables(mem_db):
    results = ensure_gold_tables(mem_db, namespace="gold_v2")
    assert len(results) == len(TABLES)
    assert set(results.keys()) == set(TABLES.keys())

    tables_in_lake = {
        r[0]
        for r in mem_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='gold_v2'"
        ).fetchall()
    }
    for t in TABLES:
        assert t in tables_in_lake


def test_seed_geography_populates_65_records(mem_db):
    ensure_gold_tables(mem_db, namespace="gold_v2")
    count = mem_db.execute("SELECT count(*) FROM gold_v2.dim_geography").fetchone()[0]
    assert count == 65

    hanoi = mem_db.execute(
        "SELECT province_code, province_name, region_name FROM gold_v2.dim_geography WHERE geography_key='GEO_HNI'"
    ).fetchone()
    assert hanoi == ("HNI", "Thành phố Hà Nội", "Đồng bằng sông Hồng")

    hcm = mem_db.execute(
        "SELECT province_code, province_name, region_name FROM gold_v2.dim_geography WHERE geography_key='GEO_HCM'"
    ).fetchone()
    assert hcm == ("HCM", "Thành phố Hồ Chí Minh", "Đông Nam Bộ")


def test_promote_views_creates_zero_copy_views(mem_db):
    ensure_gold_tables(mem_db, namespace="gold_v2")
    views = promote_views(mem_db, source_namespace="gold_v2", target_schema="gold")
    assert len(views) == len(TABLES)

    res = mem_db.execute("SELECT count(*) FROM gold.dim_geography").fetchone()[0]
    assert res == 65
