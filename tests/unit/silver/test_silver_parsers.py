"""Tests for Silver parser and normalization utilities."""

import pytest

from procurement.processing.silver.parsers import (
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
