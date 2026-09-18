"""Read and verify one fixed selection of successful attempts, never raw failed data."""

import json
from dataclasses import dataclass
from datetime import date

import pyarrow.parquet as pq

from procurement.common.catalog import ResourceDefinition
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash


@dataclass(frozen=True)
class CommittedDay:
    source_date: date
    run_id: str
    expected_records: int
    files: tuple[tuple[str, str], ...]


def select_committed_days(
    fs, definition: ResourceDefinition, start: date, end: date, *, dataset: str = "muasamcong"
) -> tuple[CommittedDay, ...]:
    selected = []
    for day in read_coverage(fs, definition.identity, start, end):
        if day.effective is None:
            raise ValueError(f"No committed attempt: {definition.identity.resource}/{day.source_date}")
        attempt = day.effective
        files = []
        for table in definition.tables:
            prefix = (f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{dataset}/{table}/"
                      f"source_date={day.source_date}/run_id={attempt.run_id}")
            files.extend((table, key) for key in sorted(fs.glob(f"{prefix}/*.parquet")))
        selected.append(CommittedDay(day.source_date, attempt.run_id, attempt.bronze_records, tuple(files)))
    return tuple(selected)


def iter_committed_records(fs, selection: tuple[CommittedDay, ...], *, verify_hash: bool = False):
    """Selection is frozen for the whole read, including resources with multiple tables."""
    for day in selection:
        count = 0
        for table, key in day.files:
            with fs.open(key, "rb") as file:
                parquet = pq.ParquetFile(file)
                for batch in parquet.iter_batches(batch_size=1024):
                    for record in batch.to_pylist():
                        if (record["run_id"] != day.run_id
                                or str(record["source_date"])[:10] != day.source_date.isoformat()):
                            raise ValueError(f"Bronze lineage mismatch in {key}")
                        if verify_hash:
                            payload = record["payload"]
                            if isinstance(payload, str):
                                payload = json.loads(payload)
                            if calculate_content_hash(payload) != record["content_hash"]:
                                raise ValueError(f"Bronze content hash mismatch in {key}")
                        count += 1
                        yield table, record
        if count != day.expected_records:
            raise ValueError(
                f"Bronze count mismatch for {day.source_date}/{day.run_id}: "
                f"expected={day.expected_records}, actual={count}"
            )


def verify_committed(fs, selection: tuple[CommittedDay, ...]) -> dict[str, int]:
    records = sum(1 for _ in iter_committed_records(fs, selection, verify_hash=True))
    return {"days": len(selection), "files": sum(len(day.files) for day in selection),
            "records": records}
