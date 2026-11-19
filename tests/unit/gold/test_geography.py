"""Unit tests for the authoritative 63 provinces dictionary, 3-digit Muasamcong codes, and Silver linkage index."""

import duckdb

from procurement.gold.domain.geography import (
    MUASAMCONG_3DIGIT_CODES,
    VIETNAM_63_PROVINCES,
    ensure_province_map_table,
    get_geography_records,
    normalize_province,
)
from procurement.silver.application.linkage_index import build_silver_linkage_index
from tests.gold.test_business import populated


def test_vietnam_has_exactly_63_canonical_provinces():
    assert len(VIETNAM_63_PROVINCES) == 63
    gso_codes = {p.gso_code for p in VIETNAM_63_PROVINCES}
    assert len(gso_codes) == 63
    short_codes = {p.short_code for p in VIETNAM_63_PROVINCES}
    assert len(short_codes) == 63


def test_economic_regions_are_valid():
    valid_regions = {
        "Đồng bằng sông Hồng",
        "Trung du và miền núi phía Bắc",
        "Bắc Trung Bộ và Duyên hải miền Trung",
        "Tây Nguyên",
        "Đông Nam Bộ",
        "Đồng bằng sông Cửu Long",
    }
    for p in VIETNAM_63_PROVINCES:
        assert p.region in valid_regions, f"Invalid region {p.region} for {p.name}"


def test_normalize_by_gso_numeric_code():
    hanoi = normalize_province("01")
    assert hanoi is not None
    assert hanoi.short_code == "HNI"
    assert hanoi.name == "Thành phố Hà Nội"

    hcm = normalize_province("79")
    assert hcm is not None
    assert hcm.short_code == "HCM"
    assert hcm.region == "Đông Nam Bộ"

    danang = normalize_province("48")
    assert danang is not None
    assert danang.short_code == "DNG"


def test_normalize_by_short_code():
    hanoi = normalize_province("HNI")
    assert hanoi is not None
    assert hanoi.gso_code == "01"

    hcm = normalize_province("hcm")
    assert hcm is not None
    assert hcm.gso_code == "79"

    danang = normalize_province("DNG")
    assert danang is not None
    assert danang.gso_code == "48"


def test_normalize_by_name_variants_and_accents():
    # Accent stripping & prefix removal
    p1 = normalize_province("TP. Hồ Chí Minh")
    assert p1 is not None and p1.gso_code == "79"

    p2 = normalize_province("Thành phố Hà Nội")
    assert p2 is not None and p2.gso_code == "01"

    p3 = normalize_province("tinh da nang")
    assert p3 is not None and p3.gso_code == "48"

    p4 = normalize_province("BRVT")
    assert p4 is not None and p4.gso_code == "77"


def test_get_geography_records_contains_63_plus_special():
    records = get_geography_records()
    # 63 provinces + 1 multi-geo + 1 unknown = 65 records
    assert len(records) == 65

    keys = {r["geography_key"] for r in records}
    assert "GEO_HNI" in keys
    assert "GEO_HCM" in keys
    assert "GEO_MULTI_UNALLOCATED" in keys
    assert "GEO_UNKNOWN" in keys


def test_normalize_by_muasamcong_3digit_codes():
    assert len(MUASAMCONG_3DIGIT_CODES) == 63
    assert normalize_province("101").short_code == "HNI"
    assert normalize_province("701").short_code == "HCM"
    assert normalize_province("501").short_code == "DNG"
    assert normalize_province("823").short_code == "CMU"

    conn = duckdb.connect(":memory:")
    ensure_province_map_table(conn)
    assert conn.execute("SELECT count(*) FROM _gtmp_canonical_geography").fetchone()[0] == 65
    assert (
        conn.execute(
            "SELECT geography_key FROM _gtmp_province_map WHERE raw_code='101'"
        ).fetchone()[0]
        == "GEO_HNI"
    )
    assert (
        conn.execute(
            "SELECT geography_key FROM _gtmp_province_map WHERE raw_code='824'"
        ).fetchone()[0]
        == "GEO_MULTI_UNALLOCATED"
    )


def test_silver_linkage_index_union_find():
    conn = duckdb.connect(":memory:")
    populated(conn)
    stats = build_silver_linkage_index(conn, source_schema="silver", target_table="silver.linkage_index")
    assert stats["nodes"] > 0
    assert stats["components"] > 0
    # Package p1, notice not1, result res1_2024, opening opn1, contract ctr1, contract_performance cp1
    # must all share the exact same connected component_id
    comp_ids = {
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT component_id
            FROM silver.linkage_index
            WHERE (entity_type='package_revision' AND entity_id='p1')
               OR (entity_type='notice_revision' AND entity_id='not1')
               OR (entity_type='result_revision' AND entity_id='res1_2024')
               OR (entity_type='opening_revision' AND entity_id='opn1')
               OR (entity_type='contract_revision' AND entity_id='ctr1')
               OR (entity_type='contract_performance_revision' AND entity_id='cp1')
            """
        ).fetchall()
    }
    assert len(comp_ids) == 1

