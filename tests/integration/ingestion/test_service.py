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
from procurement.metadata.models import PartitionIdentity
from procurement.metadata.service import PostgresMetadataService

pytestmark = pytest.mark.integration


class FakeMockClient:
    """Mock HTTP client returning 1 project search item and detail payload."""

    def __init__(self, empty: bool = False, fail_on_detail: bool = False):
        self.empty = empty
        self.fail_on_detail = fail_on_detail

    def post(self, path: str, body: Any) -> dict[str, Any]:
        if "smart/search" in path:
            if self.empty:
                return {
                    "page": {
                        "content": [],
                        "totalElements": 0,
                        "totalPages": 0,
                        "number": 0,
                        "size": 50,
                    }
                }
            return {
                "page": {
                    "content": [{"id": "fixture-project-001"}],
                    "totalElements": 1,
                    "totalPages": 1,
                    "number": 0,
                    "size": 50,
                }
            }
        if self.fail_on_detail:
            raise RuntimeError("Upstream network connection dropped during detail fetch")
        # project detail path
        return {
            "id": "fixture-project-001",
            "projectDTO": {"version": "01", "name": "Du an test sandbox"},
        }


def make_test_service(metadata_service, client, tmp_path):
    s3_endpoint = os.environ.get("TEST_S3_ENDPOINT", "http://127.0.0.1:28333")
    bucket = "procurement-refactor-test"
    access_key = "sandbox-access"
    secret_key = "sandbox-secret-only"

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": s3_endpoint},
    )

    spec = create_project_spec(client)

    return IngestionService(
        metadata=metadata_service,
        spec_factory=lambda name: spec,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=s3_endpoint,
        fs=fs,
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def test_materialize_day_project_and_reuse(database, tmp_path):
    engine, _ = database
    metadata_service = PostgresMetadataService(engine)
    client = FakeMockClient()
    service = make_test_service(metadata_service, client, tmp_path)

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


def test_materialize_empty_day_commit(database, tmp_path):
    engine, _ = database
    metadata_service = PostgresMetadataService(engine)
    client = FakeMockClient(empty=True)
    service = make_test_service(metadata_service, client, tmp_path)

    day = date(2025, 1, 2)

    req = MaterializeDayRequest(
        source_date=day,
        resource="project",
        refresh=False,
        request_id=uuid4(),
    )
    res = service.materialize_day(req)
    assert not res.reused
    assert res.status == "success"
    assert res.record_count == 0
    assert res.file_count == 0
    assert res.commit_id is not None

    # Reuse empty day commit
    reuse_res = service.materialize_day(req)
    assert reuse_res.reused
    assert reuse_res.commit_id == res.commit_id
    assert reuse_res.record_count == 0


def test_partial_failure_does_not_publish_incomplete_commit(database, tmp_path):
    engine, _ = database
    metadata_service = PostgresMetadataService(engine)
    failing_client = FakeMockClient(fail_on_detail=True)
    service = make_test_service(metadata_service, failing_client, tmp_path)

    day = date(2025, 1, 3)
    req_id = uuid4()
    req = MaterializeDayRequest(
        source_date=day,
        resource="project",
        refresh=False,
        request_id=req_id,
    )

    with pytest.raises(RuntimeError, match="Upstream network connection dropped"):
        service.materialize_day(req)

    # Verify partition has NO current commit
    snapshots = metadata_service.get_snapshot(
        [PartitionIdentity(source="muasamcong", resource="project", source_date=day)]
    )
    assert len(snapshots) == 0

    # Verify attempt is marked 'failed'
    resolved = metadata_service.resolve_request(req_id)
    assert resolved is not None
    assert resolved.attempt is not None
    assert resolved.attempt.status == "failed"
    assert "Upstream network connection dropped" in (resolved.attempt.failure_reason or "")
