"""Standard normalization and type parsing utilities for Silver transformations.

Rules enforced:
- Money must be parsed from exact strings / JSON decimals to Decimal(38, 6), never float.
- Currency must not be silently inferred if missing.
- Identifiers / codes must preserve leading zeros and whitespace stripped.
- Dates / times must handle ISO formats and Vietnam local timezone consistently.
- Booleans must not treat truthy strings like "0" as True.
"""

from contextvars import ContextVar
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any
from zoneinfo import ZoneInfo

from procurement.silver.domain.json_numbers import JsonFloatLexeme

VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
MAX_MONEY = Decimal(10) ** 32
MONEY_QUANTIZE = Decimal("0.000001")


def lookup_path(value: Any, path: str) -> Any:
    """Traverse nested dictionary using dot notation (e.g. 'projectDTO.id')."""
    if not path:
        return value
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


SENTINEL_VALUES = {
    "n/a",
    "na",
    "null",
    "none",
    "unknown",
    "undefined",
    "-",
    "--",
    "chưa có",
    "không có",
    "không",
}
PROJECT_REFERENCE_PLACEHOLDERS = frozenset({"N/A", "NA", "NULL", "NONE"})


def clean_project_reference(value: Any) -> str | None:
    """Clean a Project identifier/number, preserving zeroes and ID format."""
    result = clean_text(value)
    if result is None or result.upper() in PROJECT_REFERENCE_PLACEHOLDERS:
        return None
    return result


def clean_text(value: Any, *, filter_sentinels: bool = True) -> str | None:
    """Normalize text: strip whitespace, convert empty/sentinels to None, keep leading zeros."""
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    val_str = str(value).strip()
    if not val_str:
        return None
    if filter_sentinels and val_str.lower() in SENTINEL_VALUES:
        return None
    return val_str


def parse_money(value: Any) -> str | None:
    """Parse monetary amount to exact Decimal(38, 6) string.

    Raises ValueError if float is passed or scale/precision limits are violated.
    """
    if value is None or value == "":
        return None
    if isinstance(value, JsonFloatLexeme):
        value = value.json_lexeme
    elif isinstance(value, (bool, float)):
        raise ValueError(
            f"Money must be parsed from exact string or integer decimals, got {type(value).__name__}"
        )
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            result = Decimal(str(value).strip())
            if not result.is_finite() or abs(result) >= MAX_MONEY:
                raise ValueError("Money magnitude exceeds DECIMAL(38,6) capacity")
            quantized = result.quantize(MONEY_QUANTIZE)
            if quantized != result:
                raise ValueError(f"Money '{value}' exceeds maximum 6 fractional digits")
            return str(quantized)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid monetary format: {value}") from exc


import re

# Explicit, user-approved repairs; never infer other years from current time.
DATE_YEAR_REPAIRS = {
    "0222": "2022",
    "2222": "2022",
    "2202": "2022",
    "2223": "2023",
    "3023": "2023",
    "2206": "2026",
    "2044": "2024",
    "2035": "2025",
    "0202": "2022",
    "0201": "2021",
}

THREE_DIGIT_YEAR_REPAIRS = {
    "222": "2022",
    "202": "2022",
    "201": "2021",
    "203": "2023",
    "204": "2024",
    "205": "2025",
    "206": "2026",
}

DATE_REPAIR_EVENTS: ContextVar[list[tuple[str, str]] | None] = ContextVar(
    "date_repair_events", default=None
)


def repair_year_rule_based(year_str: str) -> str:
    """Algorithmic regex rule engine to repair 4-digit and 3-digit typo years."""
    if len(year_str) == 3:
        if m := re.match(r"^20([1-9])$", year_str):
            return f"202{m.group(1)}"
        if year_str == "222":
            return "2022"
        return THREE_DIGIT_YEAR_REPAIRS.get(year_str, year_str)

    if len(year_str) != 4:
        return year_str

    # 1. Fast lookup from verified catalog dictionary
    if year_str in DATE_YEAR_REPAIRS:
        return DATE_YEAR_REPAIRS[year_str]

    # 2. Adjacent key 3 instead of 2 (Numpad / top row): 302X / 30XX -> 202X / 20XX
    if m := re.match(r"^30([0-9]{2})$", year_str):
        return f"20{m.group(1)}"

    # 3. Double-tap repeating digit typo: 2066 -> 2026, 2044 -> 2024, 2055 -> 2025, 2033 -> 2023
    if m := re.match(r"^20([3-9])\1$", year_str):
        return f"202{m.group(1)}"

    # 4. Adjacent key 3 at tens digit: 2035 -> 2025
    if m := re.match(r"^203([4-9])$", year_str):
        return f"202{m.group(1)}"

    # 5. Leading zero typo on Numpad: 022X -> 202X, 020X -> 202X
    if m := re.match(r"^022([0-9])$", year_str):
        return f"202{m.group(1)}"
    if m := re.match(r"^020([1-9])$", year_str):
        return f"202{m.group(1)}"

    # 6. Double 2 at start: 222X -> 202X, 220X -> 202X
    if m := re.match(r"^222([0-9])$", year_str):
        return f"202{m.group(1)}"
    if m := re.match(r"^220([0-9])$", year_str):
        return f"202{m.group(1)}"

    return year_str


def repair_date_year(value: str, anchor_date: Any = None) -> str:
    """Repair four-digit and three-digit typo years at the start of a date string."""
    text_val = value.strip()
    year_part = ""
    rest = ""
    if len(text_val) >= 9 and text_val[3] == "-":
        year_part = text_val[:3]
        rest = text_val[3:]
    elif len(text_val) >= 10 and text_val[4] == "-":
        year_part = text_val[:4]
        rest = text_val[4:]
    else:
        return value

    repaired_year = repair_year_rule_based(year_part)

    # Cross-field anchor validation (using publicDate or source_date if present)
    if anchor_date is not None:
        anchor_str = str(anchor_date).strip()
        if len(anchor_str) >= 4 and anchor_str[:4].isdigit():
            anchor_year = int(anchor_str[:4])
            if repaired_year.isdigit():
                ry_int = int(repaired_year)
                # If parsed year leaps into unreasonable future (> anchor_year + 1), snap to anchor year
                if ry_int > anchor_year + 1:
                    repaired_year = str(anchor_year)
                # Historical projects (<= anchor_year, e.g. 1977, 1984, 2000) are safely preserved!

    return repaired_year + rest


def record_date_repair(original: str, repaired: str) -> None:
    events = DATE_REPAIR_EVENTS.get()
    if events is not None and original != repaired:
        events.append((original, repaired))


def parse_iso_datetime(
    value: Any, default_tz: ZoneInfo = VIETNAM_TZ, anchor_date: Any = None
) -> datetime | None:
    """Parse ISO datetime string, attaching default timezone if naive."""
    text_val = clean_text(value)
    if not text_val:
        return None
    try:
        repaired = repair_date_year(text_val, anchor_date=anchor_date)
        normalized = repaired.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tz)
        record_date_repair(text_val, repaired)
        return dt
    except ValueError as exc:
        raise ValueError(f"Invalid ISO datetime: {value}") from exc


def parse_date(value: Any, anchor_date: Any = None) -> str | None:
    """Parse date and return YYYY-MM-DD string."""
    text_val = clean_text(value)
    if not text_val:
        return None
    if "T" in text_val:
        text_val = text_val.split("T", 1)[0]
    elif " " in text_val:
        text_val = text_val.split(" ", 1)[0]
    try:
        repaired = repair_date_year(text_val, anchor_date=anchor_date)
        result = date.fromisoformat(repaired).isoformat()
        record_date_repair(text_val, repaired)
        return result
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}") from exc


def parse_boolean(value: Any) -> bool | None:
    """Safely parse boolean values without naive truthy checks."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        raise ValueError(f"Invalid numeric boolean value: {value}")
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in ("true", "t", "1", "yes", "y"):
            return True
        if cleaned in ("false", "f", "0", "no", "n"):
            return False
        if not cleaned:
            return None
        raise ValueError(f"Invalid string boolean value: {value}")
    raise ValueError(f"Cannot parse boolean from type: {type(value).__name__}")


def clean_pipe_code(value: Any) -> str | None:
    """Normalize pipe-enclosed codes (e.g. '|115|' -> '115', '||' -> None)."""
    text_val = clean_text(value)
    if not text_val:
        return None
    cleaned = text_val.strip("|").strip()
    return cleaned if cleaned else None


def sanitize_procurement_date(value: Any, min_year: int = 2010, max_year: int = 2030) -> str | None:
    """Parse date and enforce procurement timeline bounds (2010-2030).

    Returns None if date cannot be parsed or lies outside the valid bounds,
    preventing catastrophic dimensional expansion in Gold (dim_date).
    """
    d_str = parse_date(value)
    if not d_str:
        return None
    try:
        y = int(d_str.split("-")[0])
        if min_year <= y <= max_year:
            return d_str
        return None
    except Exception:
        return None


def clean_currency(value: Any) -> str | None:
    """Normalize currency to uppercase ISO-4217 standard."""
    text_val = clean_text(value)
    if not text_val:
        return None
    upper = text_val.upper().strip()
    if upper in ("VND", "VNĐ", "ĐỒNG", "DONG"):
        return "VND"
    if upper in ("USD", "DOLLAR"):
        return "USD"
    if upper in ("EUR", "EURO"):
        return "EUR"
    return upper


__all__ = [
    "VIETNAM_TZ",
    "clean_currency",
    "clean_pipe_code",
    "clean_project_reference",
    "clean_text",
    "lookup_path",
    "parse_boolean",
    "parse_date",
    "parse_iso_datetime",
    "parse_money",
    "sanitize_procurement_date",
]
