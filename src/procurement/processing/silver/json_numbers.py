"""Lossless access to original JSON decimal-number tokens without changing JSON hashing.

JsonFloatLexeme behaves like a regular float when serialized by the existing
json.dumps-based canonical JSON and digest helpers, but stores the exact numeric
literal for parse_money(). This avoids converting every Decimal into a JSON
string, which would alter revision hashes/root_json.
"""

import json
from typing import Any


class JsonFloatLexeme(float):
    """A JSON float with its original numeric token preserved for Decimal parsing."""

    def __new__(cls, lexeme: str) -> "JsonFloatLexeme":
        value = super().__new__(cls, lexeme)
        value.json_lexeme = lexeme
        return value


def loads_bronze_payload(raw: str | bytes | bytearray) -> Any:
    """Decode Bronze JSON while preserving the exact lexeme of fractional numbers."""
    return json.loads(raw, parse_float=JsonFloatLexeme)
