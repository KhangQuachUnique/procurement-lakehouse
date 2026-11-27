"""Deterministic native Spark SHA-256 identity hashing."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def sha256_key(*cols: Column | str) -> Column:
    """Compute a deterministic SHA-256 hex digest from ordered key components.

    Uses ASCII Unit Separator (0x1F) with explicit NULL sentinel so ['a', None] != ['a', ''].
    """
    parts = [
        F.coalesce(
            (c if isinstance(c, Column) else F.lit(c)).cast("string"),
            F.lit("\x00NULL\x00"),
        )
        for c in cols
    ]
    return F.sha2(F.concat_ws("\x1f", *parts), 256)
