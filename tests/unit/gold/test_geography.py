"""Unit tests for the authoritative 63 provinces dictionary and normalization."""


from procurement.gold.domain.geography import (
    VIETNAM_63_PROVINCES,
    get_geography_records,
    normalize_province,
)


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
