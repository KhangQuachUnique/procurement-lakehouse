"""Real PostgreSQL commit -> local Bronze Parquet -> Silver round-trip, no source requests."""

import copy
import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from procurement.ingestion.contracts import MaterializeDayRequest
from procurement.ingestion.service import IngestionService
from procurement.ingestion.sources.muasamcong.contract_performance.resource import (
    CONTRACT_PATH,
    LINKED_PATH,
    LOOKUP_PATH,
    SEARCH_PATH,
    create_contract_performance_spec,
)
from procurement.metadata.models import PartitionIdentity
from procurement.metadata.service import PostgresMetadataService
from procurement.silver.service import run_resource

pytestmark = pytest.mark.integration


class PublicFixtures:
    def __init__(self, *, malformed=False, cap=False):
        path = Path(__file__).resolve().parents[2] / "fixtures/contract_performance/2025.json"
        self.data = json.loads(path.read_text())
        self.day = self.data["search"][0]["publicDate"][:10]
        self.items = [r for r in self.data["search"] if r["publicDate"][:10] == self.day]
        self.malformed, self.cap = malformed, cap
        self.calls = 0

    def post(self, path, body):
        self.calls += 1
        if path == SEARCH_PATH:
            rows = self.items if body["publicDate"]["greaterThan"].startswith(self.day) else []
            size, page = body["pageSize"], body["pageNumber"]
            return {
                "page": {
                    "content": rows[page * size : (page + 1) * size],
                    "totalElements": 10000 if self.cap else len(rows),
                    "totalPages": (len(rows) + size - 1) // size,
                    "currentPage": page + 1,
                    "pageSize": size,
                }
            }
        if path == LINKED_PATH:
            if self.malformed:
                return {"resultList": []}
            return copy.deepcopy(
                next(
                    r
                    for r in self.data["linked"]
                    if r["resultList"][0]["contractCode"] == body["contractCode"]
                )
            )
        if path == LOOKUP_PATH:
            return copy.deepcopy(
                next(
                    r
                    for r in self.data["lookups"]
                    if r["page"]["content"][0]["contractCode"] == body[0]["query"][0]["keyWord"]
                )
            )
        if path == CONTRACT_PATH:
            return copy.deepcopy(
                next(r for r in self.data["contracts"] if r["contract"]["id"] == body["id"])
            )
        raise AssertionError(path)


def service(engine, client, tmp_path):
    metadata = PostgresMetadataService(engine)
    ingestion = IngestionService(
        metadata=metadata,
        spec_factory=lambda _: create_contract_performance_spec(client),
        bucket=str(tmp_path / "bucket"),
        access_key="",
        secret_key="",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    return ingestion, metadata


def test_commit_reuse_refresh_and_silver_round_trip(database, tmp_path, monkeypatch):
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    engine, _ = database
    client = PublicFixtures()
    ingestion, metadata = service(engine, client, tmp_path)
    underlying_fs = ingestion.fs

    class TableScopedStorage:
        def glob(self, pattern):
            if "/muasamcong/*/" in pattern:
                raise PermissionError("Unrelated tables cannot be enumerated")
            return underlying_fs.glob(pattern)

        def __getattr__(self, name):
            return getattr(underlying_fs, name)

    ingestion.fs = TableScopedStorage()
    day = date.fromisoformat(client.day)
    first = ingestion.materialize_day(MaterializeDayRequest(day, resource="contract_performance"))
    assert first.record_count == 2 * len(client.items)
    assert first.metrics["search_items"] == len(client.items)
    assert first.metrics["record_counts"]["contract_performance"] == len(client.items)
    calls = client.calls
    reused = ingestion.materialize_day(MaterializeDayRequest(day, resource="contract_performance"))
    assert reused.reused and client.calls == calls
    assert reused.commit_id == first.commit_id
    refreshed = ingestion.materialize_day(
        MaterializeDayRequest(day, resource="contract_performance", refresh=True)
    )
    assert refreshed.commit_id != first.commit_id
    assert refreshed.record_count == first.record_count
    legacy_lineage = f"{ingestion.bucket}/silver/observation/source_date={day}/data.parquet"
    with ingestion.fs.open(legacy_lineage, "wb") as stream:
        stream.write(b"legacy lineage must remain unchanged")
    report = run_resource(
        ingestion.fs, ingestion.bucket, "contract_performance", day, day, db_engine=engine
    )
    assert report["status"] == "completed"
    if day != date(2025, 1, 1):
        with ingestion.fs.open(legacy_lineage, "rb") as stream:
            assert stream.read() == b"legacy lineage must remain unchanged"
        assert (
            "_governance/contract_performance/observation"
            in report["days"][0]["files"]["observation"]
        )
    assert report["days"][0]["quality"]["accepted"] == first.record_count
    assert report["days"][0]["input_run_id"] == str(refreshed.attempt_id)

    client.malformed = True
    with pytest.raises(ValueError, match="exactly"):
        ingestion.materialize_day(
            MaterializeDayRequest(day, resource="contract_performance", refresh=True)
        )
    current = metadata.get_snapshot([PartitionIdentity("muasamcong", "contract_performance", day)])
    assert current[0].commit.id == refreshed.commit_id
    file = current[0].files[0]
    key = f"{file.bucket}/{file.object_key}"
    with ingestion.fs.open(key, "rb") as stream:
        original = pq.read_table(stream)
    # Valid Parquet, identical rows and lineage, but different physical bytes.
    with ingestion.fs.open(key, "wb") as stream:
        pq.write_table(original, stream, compression="gzip")
    rejected = run_resource(
        ingestion.fs, ingestion.bucket, "contract_performance", day, day, db_engine=engine
    )
    assert rejected["status"] == "failed"
    assert "SHA256 mismatch" in rejected["days"][0]["error"]
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("""select status from bronze_meta.attempts
            where partition_id=:pid order by started_at desc limit 1"""),
                {"pid": current[0].partition.id},
            ).scalar()
            == "failed"
        )


def test_empty_day_and_search_cap(database, tmp_path):
    engine, _ = database
    client = PublicFixtures()
    ingestion, _ = service(engine, client, tmp_path)
    day = date(2025, 1, 1)
    result = ingestion.materialize_day(MaterializeDayRequest(day, resource="contract_performance"))
    assert result.record_count == result.file_count == 0
    report = run_resource(
        ingestion.fs, ingestion.bucket, "contract_performance", day, day, db_engine=engine
    )
    assert report["status"] == "completed"
    assert report["days"][0]["quality"]["accepted"] == 0
    client.cap = True
    with pytest.raises(RuntimeError, match="website limit"):
        ingestion.materialize_day(
            MaterializeDayRequest(date(2025, 1, 2), resource="contract_performance")
        )
