"""Consistent reader for fixed file selections in Bronze."""

from collections.abc import Iterable, Iterator
from typing import Any

import pyarrow.parquet as pq

from procurement.bronze.verification import verify_record_content_hash


def iter_bronze_records(
    fs: Any,
    files: Iterable[tuple[str, str]],
    *,
    verify_hash: bool = False,
    batch_size: int = 1024,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Stream records from a fixed list of (table_name, object_key) pairs."""
    for table, object_key in files:
        with fs.open(object_key, "rb") as f:
            parquet = pq.ParquetFile(f)
            for batch in parquet.iter_batches(batch_size=batch_size):
                for record in batch.to_pylist():
                    if verify_hash:
                        verify_record_content_hash(record)
                    yield table, record
