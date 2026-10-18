"""Driven adapter implementing BronzeRecordStreamPort via S3/filesystem parquet streaming."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pyarrow.parquet as pq

from procurement.profiling.ports.outbound.bronze_metadata_port import (
    ActiveCommitFileDescriptor,
)
from procurement.profiling.ports.outbound.record_stream_port import (
    BronzeRecordStreamPort,
)
from procurement.storage.object_store import create_s3_filesystem


class S3ParquetRecordStreamAdapter(BronzeRecordStreamPort):
    """Streams decoded Bronze records from Parquet files stored in S3/Object Storage."""

    def __init__(self, fs: Any | None = None) -> None:
        self.fs = fs or create_s3_filesystem()

    def stream_active_records(
        self,
        files: list[ActiveCommitFileDescriptor],
        *,
        batch_size: int = 1024,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> Iterator[tuple[ActiveCommitFileDescriptor, dict[str, Any]]]:
        yielded = 0
        counter = 0

        for f_meta in files:
            full_path = f"{f_meta.bucket}/{f_meta.object_key}"
            try:
                with self.fs.open(full_path, "rb") as f:
                    parquet = pq.ParquetFile(f)
                    for batch in parquet.iter_batches(batch_size=batch_size):
                        for r in batch.to_pylist():
                            counter += 1
                            if sample_step > 1 and (counter % sample_step != 0):
                                continue

                            payload = r.get("payload")
                            if isinstance(payload, str):
                                try:
                                    r["payload"] = json.loads(payload)
                                except Exception:
                                    pass

                            yield f_meta, r
                            yielded += 1
                            if max_records is not None and yielded >= max_records:
                                return
            except Exception:
                continue
