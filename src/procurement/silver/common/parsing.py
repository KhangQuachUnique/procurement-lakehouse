"""Native PySpark Column parsers for Vietnamese procurement data (zero Python UDFs)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Sentinel strings representing missing data in raw muasamcong JSON
_MISSING_SENTINELS = ("", "n/a", "na", "-", "--", "none", "null")

# Explicit, evidence-backed year typo repairs (never silently alter valid future years like 2027)
_KNOWN_YEAR_TYPOS: dict[str, str] = {
    "0201": "2021",
    "0202": "2022",
    "0222": "2022",
    "2202": "2022",
    "2222": "2022",
    "2223": "2023",
    "3023": "2023",
    "2206": "2026",
}


def clean_text(col: Column) -> Column:
    """Trim whitespace and convert empty / sentinel strings ('N/A', '-', 'null', ...) to NULL."""
    trimmed = F.trim(col.cast("string"))
    return F.when(
        trimmed.isNull() | F.lower(trimmed).isin(*_MISSING_SENTINELS),
        F.lit(None).cast("string"),
    ).otherwise(trimmed)


def clean_pipe_code(col: Column) -> Column:
    """Strip surrounding pipes from muasamcong codes (e.g. '|115|' -> '115', '||' -> NULL)."""
    cleaned = clean_text(col)
    stripped = F.trim(F.regexp_replace(cleaned, r"^\|+|\|+$", ""))
    return F.when(stripped.isNull() | (stripped == ""), F.lit(None).cast("string")).otherwise(stripped)


def parse_decimal_38_6(col: Column) -> Column:
    """Parse exact monetary string to DECIMAL(38, 6) via Java BigDecimal (no float/double).

    Supports both plain decimals and scientific notation (e.g. '1.048348256174E11')
    emitted when raw JSON numbers are read into StringType.
    Returns NULL for unparseable strings (callers check invalid_money flag).
    """
    cleaned = clean_text(col)
    normalized = F.regexp_replace(cleaned, ",", "")
    return F.when(
        normalized.isNotNull() & normalized.rlike(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"),
        normalized.cast(DecimalType(38, 6)),
    ).otherwise(F.lit(None).cast(DecimalType(38, 6)))


def parse_vn_date(col: Column) -> Column:
    """Parse ISO date / datetime strings into DATE with explicit known-typo year repair only."""
    cleaned = clean_text(col)
    # Extract YYYY-MM-DD (or 3-digit typo YYY-MM-DD) prefix before T or space
    date_part = F.regexp_extract(cleaned, r"^(\d{3,4}-\d{2}-\d{2})", 1)
    year_str = F.split(date_part, "-").getItem(0)
    rest_str = F.substring(date_part, F.length(year_str) + 1, 6)

    # Apply explicit known-typo year mapping (e.g. 2202 -> 2022, 3023 -> 2023, 222 -> 2022)
    repaired_year = year_str
    for wrong, right in _KNOWN_YEAR_TYPOS.items():
        repaired_year = F.when(year_str == F.lit(wrong), F.lit(right)).otherwise(repaired_year)
    repaired_year = (
        F.when(year_str == "222", F.lit("2022"))
        .when(year_str == "202", F.lit("2022"))
        .when(year_str == "201", F.lit("2021"))
        .otherwise(repaired_year)
    )

    repaired_iso = F.when(date_part != "", F.concat(repaired_year, rest_str)).otherwise(F.lit(None))
    return F.try_to_timestamp(repaired_iso, F.lit("yyyy-MM-dd")).cast("date")


def parse_vn_timestamp(col: Column) -> Column:
    """Parse ISO datetime string into TIMESTAMP (handling Z / +07:00 / naive timestamps)."""
    cleaned = clean_text(col)
    # Apply explicit known-typo year repair on leading YYYY-MM-DD
    date_part = F.regexp_extract(cleaned, r"^(\d{3,4}-\d{2}-\d{2})", 1)
    year_str = F.split(date_part, "-").getItem(0)
    suffix = F.substring(cleaned, F.length(year_str) + 1, 64)

    repaired_year = year_str
    for wrong, right in _KNOWN_YEAR_TYPOS.items():
        repaired_year = F.when(year_str == F.lit(wrong), F.lit(right)).otherwise(repaired_year)

    repaired = F.when(date_part != "", F.concat(repaired_year, suffix)).otherwise(cleaned)
    return F.try_to_timestamp(repaired)


def parse_boolean(col: Column) -> Column:
    """Safely parse boolean strings ('true'/'false'/'1'/'0'/'yes'/'no') to BOOLEAN."""
    cleaned = F.lower(clean_text(col))
    return (
        F.when(cleaned.isin("true", "t", "1", "yes", "y"), F.lit(True))
        .when(cleaned.isin("false", "f", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def parse_nonnegative_int(col: Column) -> Column:
    """Parse exact non-negative integer strings to INT (rejects floats/booleans/negatives)."""
    cleaned = clean_text(col)
    return F.when(
        cleaned.isNotNull() & cleaned.rlike(r"^\d+$"),
        cleaned.cast("int"),
    ).otherwise(F.lit(None).cast("int"))
