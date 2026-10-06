from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.metadata.postgres.schema import (
    attempt_pages,
    attempts,
    commits,
    errors,
    partition_leases,
    partitions,
)
from procurement.models.control import DayStatus, RunStatus
from procurement.ops.models import ResourceHealth
from procurement.ops.repositories.postgres import (
    PostgresOpsControlRepository,
    PostgresOpsErrorRepository,
)
from procurement.ops.service import OpsService

pytestmark = pytest.mark.integration


def test_postgres_ops_control_repository(database):
    engine, _ = database

    part_id = uuid4()
    att_id = uuid4()
    owner_id = uuid4()
    commit_id = uuid4()
    now = datetime.now(UTC)
    expires = now + timedelta(minutes=5)
    source_date = date(2024, 2, 1)
    identity = ResourceIdentity(source="muasamcong", resource="project")

    with engine.begin() as conn:
        conn.execute(
            partitions.insert().values(
                id=part_id,
                source=identity.source,
                resource=identity.resource,
                source_date=source_date,
                current_commit_id=None,
                created_at=now,
            )
        )
        conn.execute(
            attempts.insert().values(
                id=att_id,
                partition_id=part_id,
                kind="ingestion",
                lease_generation=1,
                request_id=uuid4(),
                owner_id=owner_id,
                dagster_run_id="dagster-run-123",
                status="success",
                started_at=now,
                completed_at=now,
            )
        )
        conn.execute(
            partition_leases.insert().values(
                partition_id=part_id,
                generation=1,
                attempt_id=att_id,
                owner_id=owner_id,
                heartbeat_at=now,
                expires_at=expires,
            )
        )
        conn.execute(
            commits.insert().values(
                id=commit_id,
                partition_id=part_id,
                attempt_id=att_id,
                parent_commit_id=None,
                data_version=uuid4(),
                record_count=15,
                file_count=1,
                verification={"ok": True},
                committed_at=now,
            )
        )
        conn.execute(
            partitions.update()
            .where(partitions.c.id == part_id)
            .values(current_commit_id=commit_id)
        )
        conn.execute(
            attempt_pages.insert().values(
                attempt_id=att_id,
                page_number=1,
                page_size=50,
                status="success",
                search_items=15,
                bronze_records=15,
                error_count=0,
                started_at=now,
                completed_at=now,
            )
        )

    repo = PostgresOpsControlRepository(engine)

    # 1. list_runs
    runs = repo.list_runs(identity)
    assert len(runs) == 1
    run = runs[0]
    assert run.run_id == "dagster-run-123"
    assert run.resource == "project"
    assert run.status == RunStatus.SUCCESS
    assert run.total_dates == 1
    assert run.success_dates == 1
    assert run.failed_dates == 0

    # 2. get_run
    found_run = repo.get_run(identity, "dagster-run-123")
    assert found_run is not None
    assert found_run.run_id == "dagster-run-123"

    # 3. list_attempts
    day_manifests = repo.list_attempts(identity)
    assert len(day_manifests) == 1
    day = day_manifests[0]
    assert day.source_date == source_date
    assert day.status == DayStatus.SUCCESS
    assert day.completed_pages == 1
    assert day.search_items == 15
    assert day.bronze_records == 15

    # 4. get_attempt
    found_day = repo.get_attempt(identity, run_id="dagster-run-123", source_date=source_date)
    assert found_day is not None
    assert found_day.source_date == source_date

    # 5. list_pages
    pages = repo.list_pages(identity, run_id="dagster-run-123", source_date=source_date)
    assert len(pages) == 1
    assert pages[0].page_number == 1
    assert pages[0].bronze_records == 15


def test_postgres_ops_error_repository(database):
    engine, _ = database

    part_id = uuid4()
    att_id = uuid4()
    err_id = uuid4()
    now = datetime.now(UTC)
    source_date = date(2024, 2, 2)
    identity = ResourceIdentity(source="muasamcong", resource="project")

    with engine.begin() as conn:
        conn.execute(
            partitions.insert().values(
                id=part_id,
                source=identity.source,
                resource=identity.resource,
                source_date=source_date,
                current_commit_id=None,
                created_at=now,
            )
        )
        conn.execute(
            partition_leases.insert().values(
                partition_id=part_id,
                generation=0,
                attempt_id=None,
                owner_id=None,
                heartbeat_at=None,
                expires_at=None,
            )
        )
        conn.execute(
            attempts.insert().values(
                id=att_id,
                partition_id=part_id,
                kind="ingestion",
                lease_generation=1,
                request_id=uuid4(),
                owner_id=uuid4(),
                dagster_run_id="dagster-err-run",
                status="failed",
                failure_reason="Fetch error",
                started_at=now,
                completed_at=now,
            )
        )
        conn.execute(
            errors.insert().values(
                id=err_id,
                attempt_id=att_id,
                page_number=1,
                source_record_id="REC-999",
                stage="fetch",
                error_type="HTTPError",
                message="502 Bad Gateway",
                http_status=502,
                details={"url": "https://api.test"},
                occurred_at=now,
            )
        )

    err_repo = PostgresOpsErrorRepository(engine)
    errs = err_repo.list(identity)
    assert len(errs) == 1
    err = errs[0]
    assert err.error_id == str(err_id)
    assert err.stage == "fetch"
    assert err.error_type == "HTTPError"
    assert err.message == "502 Bad Gateway"
    assert err.http_status == 502
    assert err.source_id == "REC-999"


def test_ops_service_integration_with_postgres_repos(database):
    engine, _ = database
    ctrl_repo = PostgresOpsControlRepository(engine)
    err_repo = PostgresOpsErrorRepository(engine)

    service = OpsService(ctrl_repo, err_repo)
    summary = service.get_resource_summary("project")
    assert summary.health in {
        ResourceHealth.HEALTHY,
        ResourceHealth.DEGRADED,
        ResourceHealth.FAILED,
    }
