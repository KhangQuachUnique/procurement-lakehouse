"""Bounded Parquet rewriting and exact multiset verification for daily compaction (legacy shim)."""

from procurement.compaction.executor import rewrite_table
from procurement.compaction.verification import (
    CHUNK_BYTES,
    copy_file,
    digest_file,
    verify_multiset,
)
from procurement.compaction.verification import (
    verify_schema as _schema,
)

BATCH_ROWS = 1024

__all__ = [
    "BATCH_ROWS",
    "CHUNK_BYTES",
    "_schema",
    "copy_file",
    "digest_file",
    "rewrite_table",
    "verify_multiset",
]
