"""Regression tests for Bronze JSON money precision and stable Silver revision IDs."""

import json
from decimal import Decimal

import pytest

from procurement.processing.silver.json_numbers import JsonFloatLexeme, loads_bronze_payload
from procurement.processing.silver.parsers import parse_money
from procurement.processing.silver.selection import digest
from procurement.processing.silver.transformers.base import canonical_json


RAW = (
    '{"id":"51b250f7-f747-46bd-a2dd-66443c025458",'
    '"bidPrice":302472435111.978,"bidpBidLotList":['
    '{"lotPrice":121101790.978,"pricePlan":533.333}]}'
)


def test_exact_money_for_package_and_lot():
    data = loads_bronze_payload(RAW)
    lot = data["bidpBidLotList"][0]
    assert isinstance(data["bidPrice"], JsonFloatLexeme)
    assert parse_money(data["bidPrice"]) == "302472435111.978000"
    assert parse_money(lot["lotPrice"]) == "121101790.978000"
    assert parse_money(lot["pricePlan"]) == "533.333000"


def test_canonical_json_and_semantic_hash_remain_unchanged():
    regular = json.loads(RAW)
    preserved = loads_bronze_payload(RAW)
    assert canonical_json(preserved) == canonical_json(regular)
    assert digest(preserved) == digest(regular)
    assert json.loads(canonical_json(preserved)) == regular


def test_original_numeric_token_not_float_roundtrip():
    original = loads_bronze_payload('{"value":0.1234567890123456789}')
    assert original["value"].json_lexeme == "0.1234567890123456789"
    with pytest.raises(ValueError):
        parse_money(original["value"])


def test_plain_float_still_rejected():
    with pytest.raises(ValueError):
        parse_money(302472435111.978)


def test_exact_decimal_and_int_still_supported():
    assert parse_money(Decimal("533.333")) == "533.333000"
    assert parse_money(462150000) == "462150000.000000"
