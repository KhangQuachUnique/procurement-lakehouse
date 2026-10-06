"""PostgreSQL read repositories for Ops monitoring."""

from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from procurement.common.resources import ResourceIdentity
from procurement.metadata.postgres.schema import (
    attempt_pages,
    attempts,
    commits,
    errors,
    partition_leases,
    partitions,
)
from procurement.models.control import (
    DayManifest,
    DayStatus,
    PageManifest,
    PageStatus,
    RunManifest,
    RunStatus,
)
from procurement.models.errors import ErrorRecord


class PostgresOpsControlRepository:
    """Read repository for operational overview directly from PostgreSQL."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def list_runs(
        self,
        identity: ResourceIdentity,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = 100,
        status: RunStatus | None = None,
    ) -> list[RunManifest]:
        """Aggregate attempts by dagster_run_id or attempt_id to reconstruct runs."""
        with self._engine.begin() as conn:
            # Query attempts joined with partitions for this resource
            run_col = func.coalesce(
                attempts.c.dagster_run_id, func.cast(attempts.c.id, sa.Text)
            ).label("run_id")
            stmt = (
                select(
                    run_col,
                    partitions.c.source,
                    partitions.c.resource,
                    func.min(partitions.c.source_date).label("start_date"),
                    func.max(partitions.c.source_date).label("end_date"),
                    func.count(func.distinct(partitions.c.source_date)).label("total_dates"),
                    func.count(func.distinct(partitions.c.source_date))
                    .filter(attempts.c.status == "success")
                    .label("success_dates"),
                    func.count(func.distinct(partitions.c.source_date))
                    .filter(attempts.c.status.in_(["failed", "canceled", "abandoned"]))
                    .label("failed_dates"),
                    func.min(attempts.c.started_at).label("started_at"),
                    func.max(attempts.c.completed_at).label("completed_at"),
                )
                .select_from(attempts.join(partitions, attempts.c.partition_id == partitions.c.id))
                .where(
                    partitions.c.source == identity.source,
                    partitions.c.resource == identity.resource,
                )
                .group_by(run_col, partitions.c.source, partitions.c.resource)
                .order_by(func.min(attempts.c.started_at).desc())
            )

            if start_date is not None:
                stmt = stmt.having(func.max(partitions.c.source_date) >= start_date)
            if end_date is not None:
                stmt = stmt.having(func.min(partitions.c.source_date) <= end_date)

            rows = conn.execute(stmt).mappings().all()

            manifests: list[RunManifest] = []
            for row in rows:
                succ = row["success_dates"] or 0
                fail = row["failed_dates"] or 0
                tot = row["total_dates"] or 1
                if fail == 0 and succ == tot:
                    run_status = RunStatus.SUCCESS
                elif succ == 0 and fail > 0:
                    run_status = RunStatus.FAILED
                elif succ > 0 and fail > 0:
                    run_status = RunStatus.PARTIAL_FAILED
                else:
                    run_status = RunStatus.RUNNING

                if status is not None and run_status != status:
                    continue

                manifests.append(
                    RunManifest(
                        schema_version=1,
                        run_id=row["run_id"],
                        source=row["source"],
                        resource=row["resource"],
                        start_date=row["start_date"],
                        end_date=row["end_date"],
                        status=run_status,
                        total_dates=tot,
                        success_dates=succ,
                        failed_dates=fail,
                        started_at=row["started_at"],
                        completed_at=row["completed_at"],
                    )
                )
                if limit is not None and len(manifests) >= limit:
                    break

            return manifests

    def get_run(self, identity: ResourceIdentity, run_id: str) -> RunManifest | None:
        runs = self.list_runs(identity, limit=None)
        for r in runs:
            if r.run_id == run_id:
                return r
        return None

    def list_attempts(
        self,
        identity: ResourceIdentity,
        *,
        run_id: str | None = None,
        source_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = 1000,
    ) -> list[DayManifest]:
        """List day attempts for the given resource identity."""
        with self._engine.begin() as conn:
            run_col = func.coalesce(
                attempts.c.dagster_run_id, func.cast(attempts.c.id, sa.Text)
            ).label("run_id")
            stmt = (
                select(
                    run_col,
                    partitions.c.source,
                    partitions.c.resource,
                    partitions.c.source_date,
                    attempts.c.id.label("attempt_id"),
                    attempts.c.status,
                    attempts.c.started_at,
                    attempts.c.completed_at,
                    commits.c.record_count.label("bronze_records"),
                )
                .select_from(
                    attempts.join(partitions, attempts.c.partition_id == partitions.c.id).outerjoin(
                        commits, commits.c.attempt_id == attempts.c.id
                    )
                )
                .where(
                    partitions.c.source == identity.source,
                    partitions.c.resource == identity.resource,
                )
                .order_by(attempts.c.started_at.desc())
            )

            if source_date is not None:
                stmt = stmt.where(partitions.c.source_date == source_date)
            if start_date is not None:
                stmt = stmt.where(partitions.c.source_date >= start_date)
            if end_date is not None:
                stmt = stmt.where(partitions.c.source_date <= end_date)

            rows = conn.execute(stmt).mappings().all()

            results: list[DayManifest] = []
            for row in rows:
                r_id = row["run_id"]
                if run_id is not None and r_id != run_id and str(row["attempt_id"]) != run_id:
                    continue

                att_status = row["status"]
                if att_status == "success":
                    day_status = DayStatus.SUCCESS
                elif att_status == "running":
                    day_status = DayStatus.RUNNING
                else:
                    day_status = DayStatus.FAILED

                # Get page stats and error count for this attempt
                att_id = row["attempt_id"]
                page_stats = (
                    conn.execute(
                        select(
                            func.count(attempt_pages.c.page_number).label("completed_pages"),
                            func.coalesce(func.sum(attempt_pages.c.search_items), 0).label(
                                "search_items"
                            ),
                        ).where(attempt_pages.c.attempt_id == att_id)
                    )
                    .mappings()
                    .one()
                )

                err_count = (
                    conn.execute(
                        select(func.count(errors.c.id)).where(errors.c.attempt_id == att_id)
                    ).scalar()
                    or 0
                )

                results.append(
                    DayManifest(
                        schema_version=1,
                        run_id=r_id,
                        source=row["source"],
                        resource=row["resource"],
                        source_date=row["source_date"],
                        status=day_status,
                        expected_pages=None,
                        completed_pages=page_stats["completed_pages"] or 0,
                        search_items=int(page_stats["search_items"]),
                        bronze_records=row["bronze_records"] or 0,
                        error_count=err_count,
                        started_at=row["started_at"],
                        completed_at=row["completed_at"],
                    )
                )
                if limit is not None and len(results) >= limit:
                    break

            return results

    def get_attempt(
        self,
        identity: ResourceIdentity,
        *,
        run_id: str,
        source_date: date,
    ) -> DayManifest | None:
        items = self.list_attempts(identity, run_id=run_id, source_date=source_date, limit=1)
        return items[0] if items else None

    def list_pages(
        self,
        identity: ResourceIdentity,
        *,
        run_id: str,
        source_date: date,
    ) -> list[PageManifest]:
        """List page manifests for a specific attempt."""
        with self._engine.begin() as conn:
            # Find the attempt_id
            att_stmt = (
                select(attempts.c.id)
                .select_from(attempts.join(partitions, attempts.c.partition_id == partitions.c.id))
                .where(
                    partitions.c.source == identity.source,
                    partitions.c.resource == identity.resource,
                    partitions.c.source_date == source_date,
                    sa.or_(
                        attempts.c.dagster_run_id == run_id,
                        func.cast(attempts.c.id, sa.Text) == run_id,
                    ),
                )
            )
            att_id = conn.execute(att_stmt).scalar()
            if not att_id:
                return []

            pages_stmt = (
                select(attempt_pages)
                .where(attempt_pages.c.attempt_id == att_id)
                .order_by(attempt_pages.c.page_number)
            )
            rows = conn.execute(pages_stmt).mappings().all()

            return [
                PageManifest(
                    schema_version=1,
                    run_id=run_id,
                    source_date=source_date,
                    page_number=r["page_number"],
                    page_size=r["page_size"],
                    status=PageStatus.SUCCESS
                    if r["status"] == "success"
                    else PageStatus(r["status"]),
                    search_items=r["search_items"],
                    bronze_records=r["bronze_records"],
                    error_count=r["error_count"],
                    started_at=r["started_at"],
                    completed_at=r["completed_at"],
                )
                for r in rows
            ]

    def get_execution(self, identity: ResourceIdentity, run_id: str) -> dict[str, Any] | None:
        """Inspect active lease or last status to report execution liveness."""
        with self._engine.begin() as conn:
            db_now: datetime = conn.execute(select(func.now())).scalar()
            stmt = (
                select(
                    partition_leases.c.heartbeat_at,
                    partition_leases.c.expires_at,
                    attempts.c.status,
                )
                .select_from(
                    partition_leases.join(
                        partitions, partition_leases.c.partition_id == partitions.c.id
                    ).join(attempts, partition_leases.c.attempt_id == attempts.c.id)
                )
                .where(
                    partitions.c.source == identity.source,
                    partitions.c.resource == identity.resource,
                    sa.or_(
                        attempts.c.dagster_run_id == run_id,
                        func.cast(attempts.c.id, sa.Text) == run_id,
                    ),
                )
            )
            row = conn.execute(stmt).mappings().one_or_none()
            if not row:
                return None

            expires_at = row["expires_at"]
            is_alive = expires_at is not None and expires_at > db_now
            state = "running" if (is_alive and row["status"] == "running") else "interrupted"

            return {
                "state": state,
                "heartbeat_at": row["heartbeat_at"].isoformat() if row["heartbeat_at"] else None,
                "expires_at": expires_at.isoformat() if expires_at else None,
            }


class PostgresOpsErrorRepository:
    """Read repository for errors directly from PostgreSQL."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def list(
        self,
        identity: ResourceIdentity,
        *,
        limit: int = 200,
        source_date: date | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        error_type: str | None = None,
    ) -> list[ErrorRecord]:
        """Query recorded errors from PostgreSQL."""
        with self._engine.begin() as conn:
            run_col = func.coalesce(
                attempts.c.dagster_run_id, func.cast(attempts.c.id, sa.Text)
            ).label("run_id")
            stmt = (
                select(
                    errors.c.id,
                    run_col,
                    partitions.c.source,
                    partitions.c.resource,
                    partitions.c.source_date,
                    errors.c.page_number,
                    errors.c.stage,
                    errors.c.source_record_id,
                    errors.c.error_type,
                    errors.c.message,
                    errors.c.http_status,
                    errors.c.occurred_at,
                )
                .select_from(
                    errors.join(attempts, errors.c.attempt_id == attempts.c.id).join(
                        partitions, attempts.c.partition_id == partitions.c.id
                    )
                )
                .where(
                    partitions.c.source == identity.source,
                    partitions.c.resource == identity.resource,
                )
                .order_by(errors.c.occurred_at.desc())
                .limit(limit)
            )

            if source_date is not None:
                stmt = stmt.where(partitions.c.source_date == source_date)
            if run_id is not None:
                stmt = stmt.where(
                    sa.or_(
                        attempts.c.dagster_run_id == run_id,
                        func.cast(attempts.c.id, sa.Text) == run_id,
                    )
                )
            if stage is not None:
                stmt = stmt.where(errors.c.stage == stage)
            if error_type is not None:
                stmt = stmt.where(errors.c.error_type == error_type)

            rows = conn.execute(stmt).mappings().all()

            return [
                ErrorRecord(
                    schema_version=2,
                    error_id=str(r["id"]),
                    run_id=r["run_id"],
                    source=r["source"],
                    resource=r["resource"],
                    source_date=r["source_date"],
                    page_number=r["page_number"],
                    stage=r["stage"],
                    source_id=r["source_record_id"],
                    error_type=r["error_type"],
                    message=r["message"],
                    http_status=r["http_status"],
                    occurred_at=r["occurred_at"],
                )
                for r in rows
            ]
