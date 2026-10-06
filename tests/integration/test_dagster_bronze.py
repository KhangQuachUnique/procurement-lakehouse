"""Real runner, Parquet and metadata through Dagster; HTTP source remains a fixture."""

import json
from datetime import date
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa
from dagster import materialize

from procurement import bootstrap
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.metadata.postgres.schema import attempts, commits, partitions
from procurement.orchestration.bronze import bronze_assets

pytestmark = pytest.mark.integration


def test_dagster_real_commit_retry_and_reuse(database, store, monkeypatch, tmp_path):
    engine, _ = database
    fs, _bucket = store
    fail_detail = False
    calls = []

    def source(request):
        calls.append(request.url.path)
        if isinstance(json.loads(request.content), list):
            return httpx.Response(
                200,
                json={
                    "page": {
                        "content": [{"id": "p"}],
                        "totalElements": 1,
                        "totalPages": 1,
                        "number": 0,
                        "size": 50,
                    }
                },
            )
        if fail_detail:
            return httpx.Response(404)
        return httpx.Response(200, json={"id": "p", "projectDTO": {"version": "01"}})

    monkeypatch.setenv("APP_DATABASE_URL", engine.url.render_as_string(hide_password=False))
    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "fixture-token")
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    monkeypatch.setattr(
        bootstrap,
        "MuasamcongClient",
        lambda **kw: MuasamcongClient(
            **kw, transport=httpx.MockTransport(source), sleep=lambda _: None
        ),
    )

    project = next(a for a in bronze_assets if a.key.to_user_string() == "bronze_project")

    def execute(refresh=False):
        return materialize(
            [project],
            partition_key="2025-01-01",
            resources={"object_storage": fs},
            run_config={"ops": {"bronze_project": {"config": {"refresh": refresh}}}},
            raise_on_error=False,
        )

    day = date(2025, 1, 1)

    def get_part():
        with engine.connect() as conn:
            return (
                conn.execute(
                    sa.select(partitions).where(
                        partitions.c.source == "muasamcong",
                        partitions.c.resource == "project",
                        partitions.c.source_date == day,
                    )
                )
                .mappings()
                .one_or_none()
            )

    # 1. First execution -> success
    result1 = execute()
    assert result1.success

    initial_part = get_part()
    assert initial_part is not None
    assert initial_part["current_commit_id"] is not None

    with engine.connect() as conn:
        commit_row = (
            conn.execute(
                sa.select(commits).where(commits.c.id == initial_part["current_commit_id"])
            )
            .mappings()
            .one()
        )
        assert commit_row["record_count"] == 1
        assert commit_row["file_count"] >= 1

    # 2. Second execution without refresh -> reused (no new API calls)
    original_calls = len(calls)
    result2 = execute()
    assert result2.success
    assert len(calls) == original_calls

    # 3. Third execution with refresh=True and source failure -> fail, retains initial commit
    fail_detail = True
    result3 = execute(refresh=True)
    assert not result3.success

    part_after_fail = get_part()
    assert part_after_fail["current_commit_id"] == initial_part["current_commit_id"]

    # 4. Fourth execution with refresh=True after fix -> success with new commit
    fail_detail = False
    result4 = execute(refresh=True)
    assert result4.success

    part_after_refresh = get_part()
    assert part_after_refresh["current_commit_id"] != initial_part["current_commit_id"]

    # 5. Check attempts in PostgreSQL
    with engine.connect() as conn:
        part_row = (
            conn.execute(
                sa.select(partitions.c.id).where(
                    partitions.c.source == "muasamcong",
                    partitions.c.resource == "project",
                    partitions.c.source_date == day,
                )
            )
            .mappings()
            .one()
        )
        attempt_rows = (
            conn.execute(
                sa.select(attempts.c.status)
                .where(attempts.c.partition_id == part_row["id"])
                .order_by(attempts.c.started_at)
            )
            .scalars()
            .all()
        )
        assert attempt_rows == ["success", "failed", "success"]

    # Auditable evidence: pipeline artifacts created
    assert list(Path(settings.DLT_PIPELINES_DIR).iterdir())
