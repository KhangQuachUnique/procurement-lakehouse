"""Bronze writer protocols and bounded buffering."""

import json
from collections import defaultdict
from collections.abc import Mapping
from typing import Protocol

from procurement.bronze.models import BronzeRecord
from procurement.common.cancellation import INTERRUPTIONS
from procurement.common.settings import settings


class BronzeWriter(Protocol):
    """Protocol for persisting batches of Bronze records by table."""

    def write_page(self, tables: Mapping[str, list[BronzeRecord]]) -> int: ...


class BronzeWriteError(RuntimeError):
    """Raised when writing Bronze records fails, preserving persisted count."""

    def __init__(self, cause: Exception, persisted_records: int) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.persisted_records = persisted_records


def serialize_record(record: BronzeRecord) -> dict:
    row = record.model_dump(mode="json")
    row["payload"] = json.dumps(row["payload"], ensure_ascii=True, separators=(",", ":"))
    return row


class BufferedBronzeWriter:
    """Bounded page batches; receipts only describe confirmed loads, never queued rows."""

    def __init__(
        self,
        writer: BronzeWriter,
        *,
        max_bytes: int | None = None,
        max_records: int | None = None,
    ):
        self.writer = writer
        self.max_bytes = max_bytes if max_bytes is not None else settings.BRONZE_BATCH_BYTES
        self.max_records = max_records if max_records is not None else settings.BRONZE_BATCH_RECORDS
        self.pending: list[tuple[int, Mapping[str, list[BronzeRecord]]]] = []
        self.size = 0
        self.count = 0
        self.sequence = 0
        self.persisted_records = 0
        self.receipts: dict[int, int] = {}
        self.confirmed: dict[int, int] = defaultdict(int)

    def write_page(self, tables: Mapping[str, list[BronzeRecord]]) -> int:
        before = self.persisted_records
        size = sum(
            len(json.dumps(serialize_record(r), separators=(",", ":")).encode("utf-8"))
            for rows in tables.values()
            for r in rows
        )
        count = sum(len(v) for v in tables.values())
        try:
            if self.pending and (
                self.size + size > self.max_bytes or self.count + count > self.max_records
            ):
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

    def flush(self) -> int:
        before = self.persisted_records
        grouped: dict[str, list[BronzeRecord]] = defaultdict(list)
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

    def take_receipts(self) -> dict[int, int]:
        receipts, self.receipts = self.receipts, {}
        return receipts

    def abort(self) -> None:
        self.pending.clear()
        self.size = self.count = 0
