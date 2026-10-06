"""PostgreSQL implementation of attempt lifecycles, pages, and errors."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection

from procurement.metadata.models import (
    AttemptDescriptor,
    ErrorDescriptor,
    PageRecordDescriptor,
)
from procurement.metadata.postgres.schema import (
    attempt_pages,
    attempts,
    errors,
)


def get_attempt_by_id(conn: Connection, attempt_id: UUID) -> AttemptDescriptor | None:
    stmt = select(attempts).where(attempts.c.id == attempt_id)
    row = conn.execute(stmt).mappings().one_or_none()
    if not row:
        return None
    return AttemptDescriptor(
        id=row["id"],
        partition_id=row["partition_id"],
        request_id=row["request_id"],
        kind=row["kind"],
        status=row["status"],
        owner_id=row["owner_id"],
        lease_generation=row["lease_generation"],
        base_commit_id=row["base_commit_id"],
        dagster_run_id=row["dagster_run_id"],
        config=row["config"],
        failure_reason=row["failure_reason"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


def get_attempt_by_request_id(conn: Connection, request_id: UUID) -> AttemptDescriptor | None:
    stmt = select(attempts).where(attempts.c.request_id == request_id)
    row = conn.execute(stmt).mappings().one_or_none()
    if not row:
        return None
    return AttemptDescriptor(
        id=row["id"],
        partition_id=row["partition_id"],
        request_id=row["request_id"],
        kind=row["kind"],
        status=row["status"],
        owner_id=row["owner_id"],
        lease_generation=row["lease_generation"],
        base_commit_id=row["base_commit_id"],
        dagster_run_id=row["dagster_run_id"],
        config=row["config"],
        failure_reason=row["failure_reason"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


def create_attempt(
    conn: Connection,
    *,
    attempt_id: UUID,
    partition_id: UUID,
    request_id: UUID,
    kind: str,
    owner_id: UUID,
    lease_generation: int,
    base_commit_id: UUID | None,
    dagster_run_id: str | None,
    config: dict,
    db_now: datetime,
) -> AttemptDescriptor:
    values = {
        "id": attempt_id,
        "partition_id": partition_id,
        "request_id": request_id,
        "kind": kind,
        "status": "running",
        "owner_id": owner_id,
        "lease_generation": lease_generation,
        "base_commit_id": base_commit_id,
        "dagster_run_id": dagster_run_id,
        "config": config,
        "started_at": db_now,
    }
    conn.execute(attempts.insert().values(**values))
    return AttemptDescriptor(
        id=attempt_id,
        partition_id=partition_id,
        request_id=request_id,
        kind=kind,
        status="running",
        owner_id=owner_id,
        lease_generation=lease_generation,
        base_commit_id=base_commit_id,
        dagster_run_id=dagster_run_id,
        config=config,
        failure_reason=None,
        started_at=db_now,
        completed_at=None,
    )


def record_page_progress(conn: Connection, page: PageRecordDescriptor) -> None:
    """Upsert attempt page execution progress."""
    now_expr = func.now()
    started = page.started_at if page.started_at is not None else now_expr
    completed = page.completed_at
    if completed is None and page.status != "running":
        completed = now_expr

    stmt = pg_insert(attempt_pages).values(
        attempt_id=page.attempt_id,
        page_number=page.page_number,
        page_size=page.page_size,
        status=page.status,
        search_items=page.search_items,
        bronze_records=page.bronze_records,
        error_count=page.error_count,
        started_at=started,
        completed_at=completed,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["attempt_id", "page_number"],
        set_={
            "status": stmt.excluded.status,
            "search_items": stmt.excluded.search_items,
            "bronze_records": stmt.excluded.bronze_records,
            "error_count": stmt.excluded.error_count,
            "completed_at": stmt.excluded.completed_at,
        },
    )
    conn.execute(stmt)


def record_error_event(conn: Connection, error: ErrorDescriptor) -> None:
    """Insert error log record."""
    conn.execute(
        errors.insert().values(
            id=error.id,
            attempt_id=error.attempt_id,
            page_number=error.page_number,
            source_record_id=error.source_record_id,
            stage=error.stage,
            error_type=error.error_type,
            message=error.message,
            http_status=error.http_status,
            details=error.details,
            occurred_at=error.occurred_at,
        )
    )
