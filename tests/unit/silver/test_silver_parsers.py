"""Tests for Silver parser and normalization utilities."""

import pytest

from procurement.silver.domain.parsers import (
    clean_text,
    lookup_path,
    parse_boolean,
    parse_date,
    parse_iso_datetime,
    parse_money,
)


def test_clean_text():
    assert clean_text("  abc  ") == "abc"
    assert clean_text("   ") is None
    assert clean_text(None) is None
    assert clean_text("00123") == "00123"  # Preserves leading zeros
    assert clean_text(123) == "123"
    assert clean_text(["item"]) is None
    assert clean_text({"key": "val"}) is None
    assert clean_text(True) is None
    # Sentinel values that represent missing data in raw Bronze
    assert clean_text("N/A") is None
    assert clean_text("n/a") is None
    assert clean_text("NA") is None
    assert clean_text("-") is None
    assert clean_text("--") is None
    assert clean_text("none") is None
    assert clean_text("null") is None


def test_parse_money_valid():
    assert parse_money("1000000") == "1000000.000000"
    assert parse_money("123.456") == "123.456000"
    assert parse_money("123.456789") == "123.456789"
    assert parse_money(500) == "500.000000"
    assert parse_money(None) is None
    assert parse_money("") is None


def test_parse_money_rejects_float():
    with pytest.raises(ValueError, match="float"):
        parse_money(123.45)


def test_parse_money_scale_overflow():
    # More than 6 decimal places should raise
    with pytest.raises(ValueError, match="exceeds maximum 6 fractional digits"):
        parse_money("100.1234567")


def test_parse_date():
    assert parse_date("2026-03-15") == "2026-03-15"
    assert parse_date("2026-03-15T14:30:00Z") == "2026-03-15"
    assert parse_date("2026-03-15 08:00:00") == "2026-03-15"
    assert parse_date(None) is None
    assert parse_date("") is None

    with pytest.raises(ValueError):
        parse_date("not-a-date")


def test_parse_iso_datetime():
    dt = parse_iso_datetime("2026-03-15T14:30:00Z")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 3
    assert dt.tzinfo is not None


def test_parse_boolean():
    assert parse_boolean(True) is True
    assert parse_boolean(False) is False
    assert parse_boolean("true") is True
    assert parse_boolean("false") is False
    assert parse_boolean("1") is True
    assert parse_boolean("0") is False  # Explicitly False, NOT True
    assert parse_boolean(1) is True
    assert parse_boolean(0) is False
    assert parse_boolean(None) is None


def test_lookup_path():
    data = {"projectDTO": {"details": {"id": "123"}}}
    assert lookup_path(data, "projectDTO.details.id") == "123"
    assert lookup_path(data, "projectDTO.missing") is None
    assert lookup_path(data, "") == data


@pytest.mark.parametrize(
    "wrong,right",
    [
        ("0222", "2022"),
        ("3023", "2023"),
        ("2202", "2022"),
        ("2222", "2022"),
        ("2206", "2026"),
        ("2223", "2023"),
        ("0201", "2021"),
        ("0202", "2022"),
    ],
)
def test_approved_year_repairs(wrong, right):
    assert parse_date(f"{wrong}-09-16T12:30:00Z") == f"{right}-09-16"
    dt = parse_iso_datetime(f"{wrong}-09-16T12:30:00+07:00")
    assert dt.year == int(right)
    assert dt.utcoffset().total_seconds() == 7 * 3600
    assert parse_date(parse_date(f"{wrong}-09-16")) == f"{right}-09-16"


@pytest.mark.parametrize("wrong,right", [("222", "2022"), ("202", "2022"), ("201", "2021")])
def test_three_digit_year_repairs(wrong, right):
    assert parse_date(f"{wrong}-11-04") == f"{right}-11-04"


def test_sanitize_procurement_date():
    from procurement.silver.domain.parsers import clean_currency, sanitize_procurement_date

    assert sanitize_procurement_date("2024-03-15") == "2024-03-15"
    assert sanitize_procurement_date("0222-11-04") == "2022-11-04"
    assert sanitize_procurement_date("201-11-04") == "2021-11-04"
    # Outliers outside 2010-2030 are safely neutralized to None
    assert sanitize_procurement_date("1984-05-12") is None
    # 3024 is repaired via regex to 2024
    assert sanitize_procurement_date("3024-01-01") == "2024-01-01"
    # Unrepairable future outlier
    assert sanitize_procurement_date("3500-01-01") is None
    assert sanitize_procurement_date(None) is None

    # Currency clean
    assert clean_currency("vnd") == "VND"
    assert clean_currency("vnđ") == "VND"
    assert clean_currency("ĐỒNG") == "VND"
    assert clean_currency("usd") == "USD"
    assert clean_currency(None) is None


def test_year_repairs_do_not_guess_other_dates():
    assert parse_date("2028-01-01") == "2028-01-01"
    assert parse_date("1999-01-01") == "1999-01-01"
    # 3024 is now aggressively repaired to 2024 via the 30XX regex rule
    assert parse_date("3024-01-01") == "2024-01-01"
    # 2066 is repaired to 2026 via double-tap regex rule
    assert parse_date("2066-01-01") == "2026-01-01"
    # Truly distant unrepairable future dates
    assert parse_date("3500-01-01") == "3500-01-01"
    with pytest.raises(ValueError):
        parse_date("0222-02-29")


def test_anchor_date_inference():
    # If anchor_date is 2024, unreasonable future year 2066 snaps to anchor year 2024
    assert parse_date("2066-08-20", anchor_date="2024-08-24") == "2024-08-20"
    assert parse_date("3024-12-31", anchor_date="2024-01-01") == "2024-12-31"
    
    # Valid historical projects (e.g. 1977, 1984, 2000) are safely preserved, not forced to anchor
    assert parse_date("1977-05-12", anchor_date="2024-08-24") == "1977-05-12"
    assert parse_date("1984-10-01", anchor_date="2024-08-24") == "1984-10-01"
    assert parse_date("2005-03-20", anchor_date="2024-08-24") == "2005-03-20"
