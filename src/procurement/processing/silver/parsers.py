"""Standard normalization and type parsing utilities for Silver transformations.

Rules enforced:
- Money must be parsed from exact strings / JSON decimals to Decimal(38, 6), never float.
- Currency must not be silently inferred if missing.
- Identifiers / codes must preserve leading zeros and whitespace stripped.
- Dates / times must handle ISO formats and Vietnam local timezone consistently.
- Booleans must not treat truthy strings like "0" as True.
"""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any
from zoneinfo import ZoneInfo

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


def clean_text(value: Any) -> str | None:
    """Normalize text: strip whitespace, convert empty to None, keep leading zeros."""
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    val_str = str(value).strip()
    return val_str if val_str else None


def parse_money(value: Any) -> str | None:
    """Parse monetary amount to exact Decimal(38, 6) string.

    Raises ValueError if float is passed or scale/precision limits are violated.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)):
        raise ValueError(  # noqa: TRY004
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


def parse_iso_datetime(value: Any, default_tz: ZoneInfo = VIETNAM_TZ) -> datetime | None:
    """Parse ISO datetime string, attaching default timezone if naive."""
    text_val = clean_text(value)
    if not text_val:
        return None
    try:
        # Normalize trailing Z if present
        normalized = text_val.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tz)
        return dt
    except ValueError as exc:
        raise ValueError(f"Invalid ISO datetime: {value}") from exc


def parse_date(value: Any) -> str | None:
    """Parse date and return YYYY-MM-DD string."""
    text_val = clean_text(value)
    if not text_val:
        return None
    # Truncate time if full ISO string was passed
    if "T" in text_val:
        text_val = text_val.split("T", 1)[0]
    elif " " in text_val:
        text_val = text_val.split(" ", 1)[0]
    try:
        return date.fromisoformat(text_val).isoformat()
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
