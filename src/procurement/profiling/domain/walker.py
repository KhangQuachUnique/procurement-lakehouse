"""Domain logic for recursive traversal of arbitrary nested JSON data structures."""

from __future__ import annotations

import re
from collections.abc import Iterator
from enum import Enum
from typing import Any


class ValueState(str, Enum):
    MISSING = "missing"
    NULL = "null"
    EMPTY = "empty"
    POPULATED = "populated"


SENTINEL_VALUES = {
    "n/a", "na", "null", "none", "unknown", "undefined", "tbd", "test", "xxx",
    "chưa có", "không có", "không", "-", "--", "0", "-1", "999999", "9999-12-31"
}


def classify_value_state(value: Any) -> ValueState:
    """Classify value into NULL, EMPTY, or POPULATED."""
    if value is None:
        return ValueState.NULL
    if isinstance(value, str):
        if not value.strip():
            return ValueState.EMPTY
        return ValueState.POPULATED
    if isinstance(value, (list, dict, set, tuple)):
        if len(value) == 0:
            return ValueState.EMPTY
        return ValueState.POPULATED
    # 0, 0.0, False are POPULATED per data-cleaning standard
    return ValueState.POPULATED


def detect_type_name(value: Any) -> str:
    """Detect standardized semantic type name of a value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        s = value.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s):
            return "datetime"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return "date"
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


class NestedJsonWalker:
    """Recursively walks nested JSON data structures, emitting normalized paths and values."""

    @classmethod
    def walk(
        cls,
        data: Any,
        prefix: str = "$",
    ) -> Iterator[tuple[str, Any, ValueState, str]]:
        """Recursively yield (path, value, state, type_name) for all nodes in the tree."""
        state = classify_value_state(data)
        type_name = detect_type_name(data)
        
        yield prefix, data, state, type_name

        if isinstance(data, dict):
            for k, v in data.items():
                child_path = f"{prefix}.{k}" if prefix != "$" else f"$.{k}"
                yield from cls.walk(v, prefix=child_path)

        elif isinstance(data, list):
            array_item_prefix = f"{prefix}[]"
            for item in data:
                yield from cls.walk(item, prefix=array_item_prefix)

    @classmethod
    def flatten_record(cls, record: dict[str, Any]) -> dict[str, list[tuple[Any, ValueState, str]]]:
        """Flatten a complete record into a map of path -> list of observed values."""
        flattened: dict[str, list[tuple[Any, ValueState, str]]] = {}
        for path, val, state, type_name in cls.walk(record):
            if path not in flattened:
                flattened[path] = []
            flattened[path].append((val, state, type_name))
        return flattened
