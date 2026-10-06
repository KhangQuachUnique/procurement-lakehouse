"""Integration test for IngestionService materialize_day use case."""

import os
from datetime import date
from typing import Any
from uuid import uuid4

import pytest
import s3fs

from procurement.ingestion.contracts import MaterializeDayRequest
from procurement.ingestion.service import IngestionService
from procurement.ingestion.sources.muasamcong.project.resource import create_project_spec
from procurement.metadata.service import PostgresMetadataService

pytestmark = pytest.mark.integration


class FakeMockClient:
    """Mock HTTP client returning 1 project search item and detail payload."""

    def post(self, path: str, body: Any) -> dict[str, Any]:
        if "smart/search" in path:
            return {
                "page": {
                    "content": [{"id": "fixture-project-001"}],
                    "totalElements": 1,
                    "totalPages": 1,
                    "number": 0,
                    "size": 50,
                }
            }
        # project detail path
        return {
            "id": "fixture-project-001",
            "projectDTO": {"version": "01", "name": "Du an test sandbox"},
        }


def test_materialize_day_project_and_reuse(database, tmp_path, monkeypatch):
    engine, _ = database
    metadata_service = PostgresMetadataService(engine)

    s3_endpoint = os.environ.get("TEST_S3_ENDPOINT", "http://127.0.0.1:28333")
    bucket = "procurement-refactor-test"
    access_key = "sandbox-access"
    secret_key = "sandbox-secret-only"

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": s3_endpoint},
    )

    client = FakeMockClient()
    spec = create_project_spec(client)

    service = IngestionService(
        metadata=metadata_service,
        spec_factory=lambda name: spec,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=s3_endpoint,
        fs=fs,
        pipelines_dir=str(tmp_path / "pipelines"),
    )

    day = date(2025, 1, 1)

    # 1. First run: should crawl, write parquet to sandbox S3, and commit to PostgreSQL
    req1 = MaterializeDayRequest(
        source_date=day,
        resource="project",
        refresh=False,
        request_id=uuid4(),
    )
    res1 = service.materialize_day(req1)
    assert not res1.reused
    assert res1.status == "success"
    assert res1.record_count == 1
    assert res1.file_count >= 1
    assert res1.commit_id is not None
    assert res1.attempt_id is not None

    # 2. Second run: same day, no refresh -> MUST REUSE immediately without crawling
    req2 = MaterializeDayRequest(
        source_date=day,
        resource="project",
        refresh=False,
        request_id=uuid4(),
    )
    res2 = service.materialize_day(req2)
    assert res2.reused
    assert res2.status == "success"
    assert res2.commit_id == res1.commit_id
    assert res2.record_count == 1

    # 3. Third run: with refresh=True -> MUST create new attempt and commit
    req3 = MaterializeDayRequest(
        source_date=day,
        resource="project",
        refresh=True,
        request_id=uuid4(),
    )
    res3 = service.materialize_day(req3)
    assert not res3.reused
    assert res3.status == "success"
    assert res3.commit_id != res1.commit_id
    assert res3.record_count == 1
