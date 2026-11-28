"""Shared SQL identifier safety utilities across lakehouse layers."""

import re


def identifier(value: str) -> str:
    """Validate and return a safe unquoted SQL identifier."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value
