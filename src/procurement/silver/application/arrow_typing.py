"""Direct PyArrow schema validation and vectorized type coercion for Silver tables.

Uses a 3-tier execution strategy against the centralized SQLAlchemy -> PyArrow schema:
  1. Direct C++ fast-path (`pa.Table.from_pylist(rows, schema=pa_schema)`).
  2. Vectorized C++ column casting (`pyarrow.compute.cast`) when column types are uniform.
  3. Per-column Python fallback only for columns with mixed types or special Decimal/ISO edge cases.
"""

from collections.abc import Callable
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from procurement.silver.domain.schema import metadata, to_pyarrow_schema
from procurement.storage.iceberg import identifier

_PYARROW_SCHEMAS: dict[str, pa.Schema] = {}
_EMPTY_TYPED_TABLES: dict[str, pa.Table] = {}


def get_pyarrow_schema(table_name: str) -> pa.Schema:
    """Return the cached PyArrow schema for a Silver table."""
    clean_name = identifier(table_name)
    cached = _PYARROW_SCHEMAS.get(clean_name)
    if cached is not None:
        return cached

    sa_table = metadata.tables.get(f"silver.{clean_name}")
    if sa_table is None:
        sa_table = metadata.tables.get(clean_name)
    if sa_table is None:
        raise KeyError(f"Unknown Silver table schema: {clean_name}")

    pa_schema = to_pyarrow_schema(sa_table)
    _PYARROW_SCHEMAS[clean_name] = pa_schema
    return pa_schema


def _coerce_string(val: Any) -> str | None:
    if val is None:
        return None
    return val if isinstance(val, str) else str(val)


def _coerce_int(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        raise TypeError(f"Expected integer, got bool ({val!r})")
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        stripped = val.strip()
        return int(stripped) if stripped else None
    return int(val)


def _coerce_bool(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return bool(val)
    if isinstance(val, str):
        normalized = val.strip().lower()
        if normalized in {"true", "1", "t", "yes", "y"}:
            return True
        if normalized in {"false", "0", "f", "no", "n"}:
            return False
        if not normalized:
            return None
    raise ValueError(f"Cannot coerce {val!r} to bool")


def _coerce_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        stripped = val.strip()
        if not stripped:
            return None
        return date.fromisoformat(stripped[:10])
    raise TypeError(f"Cannot coerce {type(val).__name__} ({val!r}) to date")


def _coerce_timestamp(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo is not None else val.replace(tzinfo=timezone.utc)
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
    if isinstance(val, str):
        stripped = val.strip()
        if not stripped:
            return None
        dt = datetime.fromisoformat(stripped)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    raise TypeError(f"Cannot coerce {type(val).__name__} ({val!r}) to timestamp")


def _make_decimal_coercer(scale: int) -> Callable[[Any], Decimal | None]:
    quant = Decimal(10) ** -scale
    min_exp = -scale

    def _coerce_decimal(val: Any) -> Decimal | None:
        if val is None:
            return None
        if isinstance(val, bool):
            raise TypeError(f"Expected decimal, got bool ({val!r})")
        try:
            dec = (
                val
                if isinstance(val, Decimal)
                else Decimal(val.strip() if isinstance(val, str) else str(val))
            )
        except InvalidOperation as exc:
            raise ValueError(f"Invalid decimal value {val!r}") from exc
        if not dec.is_finite():
            raise ValueError(f"Non-finite decimal value {val!r}")
        exp = dec.as_tuple().exponent
        if isinstance(exp, int) and exp < min_exp:
            return dec.quantize(quant)
        return dec

    return _coerce_decimal


def _coercer_for_type(dtype: pa.DataType) -> Callable[[Any], Any]:
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return _coerce_string
    if pa.types.is_int64(dtype) or pa.types.is_integer(dtype):
        return _coerce_int
    if pa.types.is_boolean(dtype):
        return _coerce_bool
    if pa.types.is_date(dtype):
        return _coerce_date
    if pa.types.is_timestamp(dtype):
        return _coerce_timestamp
    if pa.types.is_decimal(dtype):
        return _make_decimal_coercer(dtype.scale)
    return lambda val: val


def _coerce_column_python(
    rows: list[dict[str, Any]],
    clean_name: str,
    field: pa.Field,
) -> pa.Array:
    """Coerce and validate a single column in Python when C++ vectorized cast cannot."""
    col_name = field.name
    coercer = _coercer_for_type(field.type)
    nullable = field.nullable
    col_values: list[Any] = []

    for idx, row in enumerate(rows):
        raw_val = row.get(col_name)
        try:
            coerced = coercer(raw_val)
        except (ValueError, TypeError, InvalidOperation) as exc:
            raise ValueError(
                f"Type coercion failed for {clean_name}.{col_name} at row {idx}: {exc}"
            ) from exc
        if not nullable and coerced is None:
            raise ValueError(
                f"NOT NULL constraint failed for {clean_name}.{col_name} at row {idx}"
            )
        col_values.append(coerced)

    try:
        return pa.array(col_values, type=field.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        raise ValueError(
            f"PyArrow array conversion failed for {clean_name}.{col_name} ({field.type}): {exc}"
        ) from exc


def _cast_column_vectorized(
    raw_col: pa.ChunkedArray,
    rows: list[dict[str, Any]],
    clean_name: str,
    field: pa.Field,
) -> pa.Array | pa.ChunkedArray:
    """Cast a uniform Arrow column via pyarrow.compute, falling back to Python only on failure."""
    if raw_col.type == field.type:
        if not field.nullable and raw_col.null_count > 0:
            raise ValueError(f"NOT NULL constraint failed for {clean_name}.{field.name}")
        return raw_col

    try:
        # String -> boolean in Python accepts "yes"/"no"/"y"/"n"; pc.cast only accepts true/false/1/0.
        casted = pc.cast(raw_col, field.type, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        return _coerce_column_python(rows, clean_name, field)

    if not field.nullable and casted.null_count > 0:
        raise ValueError(f"NOT NULL constraint failed for {clean_name}.{field.name}")
    return casted


def typed_arrow_rows_pyarrow(name: str, rows: list[dict[str, Any]]) -> pa.Table:
    """Validate and cast Python dict rows directly into a typed PyArrow Table."""
    clean_name = identifier(name)
    pa_schema = get_pyarrow_schema(clean_name)

    if not rows:
        cached = _EMPTY_TYPED_TABLES.get(clean_name)
        if cached is not None:
            return cached
        empty = pa_schema.empty_table()
        _EMPTY_TYPED_TABLES[clean_name] = empty
        return empty

    # Tier 1: Direct C++ fast-path when Python types already match pa_schema
    try:
        tbl = pa.Table.from_pylist(rows, schema=pa_schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, TypeError, ValueError):
        tbl = None

    if tbl is not None:
        for field in pa_schema:
            if not field.nullable and tbl[field.name].null_count > 0:
                raise ValueError(f"NOT NULL constraint failed for {clean_name}.{field.name}")
        return tbl

    # Tier 2: Build raw Arrow columns in C++, then cast column-wise via pyarrow.compute
    raw_tbl: pa.Table | None = None
    try:
        raw_tbl = pa.Table.from_pylist(rows)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, TypeError, ValueError):
        raw_tbl = None

    if raw_tbl is not None:
        try:
            n_rows = len(rows)
            raw_names = set(raw_tbl.column_names)
            arrays: list[pa.Array | pa.ChunkedArray] = []
            for field in pa_schema:
                if field.name not in raw_names:
                    if not field.nullable:
                        raise ValueError(
                            f"NOT NULL constraint failed for {clean_name}.{field.name} at row 0"
                        )
                    arrays.append(pa.nulls(n_rows, type=field.type))
                else:
                    arrays.append(
                        _cast_column_vectorized(raw_tbl[field.name], rows, clean_name, field)
                    )
            return pa.Table.from_arrays(arrays, schema=pa_schema)
        finally:
            del raw_tbl

    # Tier 3: Mixed-type column fallback
    arrays = [_coerce_column_python(rows, clean_name, field) for field in pa_schema]
    return pa.Table.from_arrays(arrays, schema=pa_schema)


__all__ = ["get_pyarrow_schema", "typed_arrow_rows_pyarrow"]
