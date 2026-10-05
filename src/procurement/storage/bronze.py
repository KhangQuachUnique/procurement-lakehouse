import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from datetime import date
from typing import Any, Protocol

import dlt
from dlt.destinations import filesystem

from procurement.common.cancellation import INTERRUPTIONS
from procurement.common.settings import settings
from procurement.models.bronze import BronzeRecord


class BronzeWriter(Protocol):
    def write_page(self, tables: Mapping[str, list[BronzeRecord]]) -> int: ...


class BronzeWriteError(RuntimeError):
    def __init__(self, cause: Exception, persisted_records: int) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.persisted_records = persisted_records


class DltBronzeWriter:
    """One attempt owns one pipeline. Counts include only confirmed table loads."""

    def __init__(self, pipeline_factory: Callable[[], Any]) -> None:
        self._pipeline_factory = pipeline_factory
        self._pipeline: Any = None

    def write_page(self, tables: Mapping[str, list[BronzeRecord]]) -> int:
        persisted = 0
        try:
            for table, records in tables.items():
                if not records:
                    continue
                if self._pipeline is None:
                    self._pipeline = self._pipeline_factory()
                info = self._pipeline.run(create_bronze_resource(records, name=table))
                if info is None:
                    raise RuntimeError("DLT returned no load receipt for a non-empty resource")
                info.raise_on_failed_jobs()
                persisted += len(records)
        except INTERRUPTIONS:
            raise
        except Exception as exc:
            raise BronzeWriteError(exc, persisted) from exc
        return persisted


class BufferedBronzeWriter:
    """Bounded page batches; receipts only describe confirmed loads, never queued rows."""

    def __init__(self, writer, *, max_bytes=None, max_records=None):
        self.writer = writer
        self.max_bytes = max_bytes or settings.BRONZE_BATCH_BYTES
        self.max_records = max_records or settings.BRONZE_BATCH_RECORDS
        self.pending = []
        self.size = self.count = self.sequence = self.persisted_records = 0
        self.receipts = {}
        self.confirmed = defaultdict(int)

    def write_page(self, tables):
        before = self.persisted_records
        size = sum(len(json.dumps(serialize_record(r), separators=(",", ":")).encode("utf-8"))
                   for rows in tables.values() for r in rows)
        count = sum(map(len, tables.values()))
        try:
            if self.pending and (self.size + size > self.max_bytes or
                                 self.count + count > self.max_records):
                self.flush()
            number = self.sequence
            self.sequence += 1
            self.pending.append((number, tables))
            self.size += size
            self.count += count
            if self.size >= self.max_bytes or self.count >= self.max_records:
                self.flush()
        except BronzeWriteError as exc:
            raise BronzeWriteError(exc.cause, self.persisted_records - before) from exc
        return self.persisted_records - before

    def flush(self):
        before = self.persisted_records
        grouped = defaultdict(list)
        for _, tables in self.pending:
            for table, rows in tables.items():
                grouped[table].extend(rows)
        try:
            for table, rows in grouped.items():
                count = self.writer.write_page({table: rows})
                self.persisted_records += count
                if count != len(rows):
                    raise RuntimeError("Batch writer returned an incomplete receipt")
                for number, tables in self.pending:
                    self.confirmed[number] += len(tables.get(table, []))
        except INTERRUPTIONS:
            raise
        except Exception as exc:
            cause = exc.cause if isinstance(exc, BronzeWriteError) else exc
            # Do not retry this buffer: an unsuccessful load may have reached storage.
            self.pending.clear()
            self.size = self.count = 0
            raise BronzeWriteError(cause, self.persisted_records - before) from exc
        for number, _ in self.pending:
            self.receipts[number] = self.confirmed[number]
        self.pending.clear()
        self.size = self.count = 0
        return self.persisted_records - before

    def take_receipts(self):
        receipts, self.receipts = self.receipts, {}
        return receipts

    def abort(self):
        self.pending.clear()
        self.size = self.count = 0


def create_bronze_destination(*, source_partition_date: date, run_id: str):
    return filesystem(
        bucket_url=f"s3://{settings.OBJECT_STORAGE_BUCKET}/bronze",
        credentials={
            "aws_access_key_id": settings.OBJECT_STORAGE_ACCESS_KEY,
            "aws_secret_access_key": settings.OBJECT_STORAGE_SECRET_KEY,
            "endpoint_url": settings.OBJECT_STORAGE_ENDPOINT,
            "region_name": "us-east-1",
        },
        layout=("{table_name}/source_date={source_date}/run_id={run_id}/{load_id}.{file_id}.{ext}"),
        extra_placeholders={
            "source_date": source_partition_date.isoformat(),
            "run_id": run_id,
        },
    )


def serialize_record(record):
    row = record.model_dump(mode="json")
    # Preserve private-use Unicode markers through DLT normalization.
    row["payload"] = json.dumps(row["payload"], ensure_ascii=True, separators=(",", ":"))
    return row


def create_bronze_resource(records: Iterable[BronzeRecord], *, name: str):
    """Create one append-only Bronze table resource for a page/chunk."""

    def serialized():
        for record in records:
            yield serialize_record(record)

    return dlt.resource(
        serialized(),
        name=name,
        table_name=name,
        write_disposition="append",
        file_format="parquet",
        max_table_nesting=0,
        columns={
            "payload": {"data_type": "text", "nullable": False},
            "source_version": {
                "data_type": "text",
                "nullable": True,
            },
        },
    )
