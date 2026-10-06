"""PostgreSQL implementation of the MetadataService protocol."""

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from procurement.metadata.contracts import MetadataService
from procurement.metadata.errors import (
    AttemptNotFoundError,
    InvalidAttemptStateError,
    LeaseLostError,
)
from procurement.metadata.models import (
    BeginOrReuseResult,
    CommitFileDescriptor,
    CommitRecord,
    ErrorDescriptor,
    LeaseInfo,
    PageRecordDescriptor,
    PartitionIdentity,
    PartitionRecord,
    SnapshotView,
)
from procurement.metadata.postgres.attempts import (
    create_attempt,
    get_attempt_by_id,
    get_attempt_by_request_id,
    record_error_event,
    record_page_progress,
)
from procurement.metadata.postgres.commits import (
    get_commit_by_id,
    get_partition_by_identity,
    get_snapshot_for_partitions,
)
from procurement.metadata.postgres.commits import (
    publish_commit as pg_publish_commit,
)
from procurement.metadata.postgres.leases import (
    apply_claimed_lease,
    get_lease_info,
    lock_and_claim_lease,
    release_lease,
)
from procurement.metadata.postgres.leases import (
    renew_lease as pg_renew_lease,
)
from procurement.metadata.postgres.schema import (
    attempts,
    partition_leases,
    partitions,
)


class PostgresMetadataService(MetadataService):
    """Production and sandbox PostgreSQL implementation of metadata contracts."""

    def __init__(self, engine: Engine):
        self._engine = engine

    def begin_or_reuse(
        self,
        identity: PartitionIdentity,
        *,
        refresh: bool,
        request_id: UUID,
        owner_id: UUID,
        kind: str = "ingestion",
        lease_duration: timedelta = timedelta(minutes=5),
        dagster_run_id: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> BeginOrReuseResult:
        cfg = config or {}
        with self._engine.begin() as conn:
            # 1. Idempotency check for request_id
            existing_attempt = get_attempt_by_request_id(conn, request_id)
            if existing_attempt is not None:
                part = get_partition_by_identity(conn, identity)
                commit = (
                    get_commit_by_id(conn, existing_attempt.base_commit_id)
                    if existing_attempt.base_commit_id
                    else None
                )
                if existing_attempt.status == "success":
                    commit = (
                        get_commit_by_id(conn, part.current_commit_id)
                        if part and part.current_commit_id
                        else commit
                    )
                return BeginOrReuseResult(
                    reused=(existing_attempt.status == "success"),
                    partition=part,  # type: ignore[arg-type]
                    commit=commit,
                    attempt=existing_attempt,
                    refresh_in_progress=False,
                )

            db_now: datetime = conn.execute(select(func.now())).scalar_one()

            # 2. Look up or create partition
            part_stmt = (
                select(partitions)
                .where(
                    partitions.c.source == identity.source,
                    partitions.c.resource == identity.resource,
                    partitions.c.source_date == identity.source_date,
                )
                .with_for_update()
            )
            part_row = conn.execute(part_stmt).mappings().one_or_none()
            if part_row is None:
                part_id = uuid4()
                conn.execute(
                    partitions.insert().values(
                        id=part_id,
                        source=identity.source,
                        resource=identity.resource,
                        source_date=identity.source_date,
                        current_commit_id=None,
                        created_at=db_now,
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
                part_record = PartitionRecord(
                    id=part_id,
                    identity=identity,
                    current_commit_id=None,
                    created_at=db_now,
                )
            else:
                part_record = PartitionRecord(
                    id=part_row["id"],
                    identity=identity,
                    current_commit_id=part_row["current_commit_id"],
                    created_at=part_row["created_at"],
                )

            # 3. Check for reuse if not refresh and commit exists
            if not refresh and part_record.current_commit_id is not None:
                lease_info = get_lease_info(conn, part_record.id)
                refresh_in_progress = bool(
                    lease_info
                    and lease_info.is_held
                    and lease_info.expires_at
                    and lease_info.expires_at > db_now
                )
                existing_commit = get_commit_by_id(conn, part_record.current_commit_id)
                return BeginOrReuseResult(
                    reused=True,
                    partition=part_record,
                    commit=existing_commit,
                    lease=lease_info,
                    refresh_in_progress=refresh_in_progress,
                )

            # 4. Claim lease and create new attempt
            new_attempt_id = uuid4()
            generation, lease_info = lock_and_claim_lease(
                conn,
                part_record.id,
                attempt_id=new_attempt_id,
                owner_id=owner_id,
                lease_duration=lease_duration,
                db_now=db_now,
            )
            att_desc = create_attempt(
                conn,
                attempt_id=new_attempt_id,
                partition_id=part_record.id,
                request_id=request_id,
                kind=kind,
                owner_id=owner_id,
                lease_generation=generation,
                base_commit_id=part_record.current_commit_id,
                dagster_run_id=dagster_run_id,
                config=cfg,
                db_now=db_now,
            )
            apply_claimed_lease(conn, lease_info)

            return BeginOrReuseResult(
                reused=False,
                partition=part_record,
                attempt=att_desc,
                lease=lease_info,
                refresh_in_progress=False,
            )

    def renew_lease(
        self,
        attempt_id: UUID,
        *,
        owner_id: UUID,
        generation: int,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> LeaseInfo:
        with self._engine.begin() as conn:
            att = get_attempt_by_id(conn, attempt_id)
            if att is None:
                raise AttemptNotFoundError(f"Attempt {attempt_id} not found")
            db_now: datetime = conn.execute(select(func.now())).scalar_one()
            return pg_renew_lease(
                conn,
                att.partition_id,
                attempt_id=attempt_id,
                owner_id=owner_id,
                generation=generation,
                lease_duration=lease_duration,
                db_now=db_now,
            )

    def record_page(self, page: PageRecordDescriptor) -> None:
        with self._engine.begin() as conn:
            record_page_progress(conn, page)

    def record_error(self, error: ErrorDescriptor) -> None:
        with self._engine.begin() as conn:
            record_error_event(conn, error)

    def publish_commit(
        self,
        attempt_id: UUID,
        *,
        owner_id: UUID,
        generation: int,
        expected_base_commit_id: UUID | None,
        record_count: int,
        files: list[CommitFileDescriptor],
        verification: dict[str, Any],
    ) -> CommitRecord:
        with self._engine.begin() as conn:
            db_now: datetime = conn.execute(select(func.now())).scalar_one()
            return pg_publish_commit(
                conn,
                attempt_id=attempt_id,
                owner_id=owner_id,
                generation=generation,
                expected_base_commit_id=expected_base_commit_id,
                record_count=record_count,
                files=files,
                verification=verification,
                db_now=db_now,
            )

    def fail_attempt(
        self,
        attempt_id: UUID,
        *,
        owner_id: UUID,
        generation: int,
        reason: str,
        canceled: bool = False,
    ) -> None:
        with self._engine.begin() as conn:
            att_stmt = select(attempts).where(attempts.c.id == attempt_id).with_for_update()
            att_row = conn.execute(att_stmt).mappings().one_or_none()
            if not att_row:
                raise AttemptNotFoundError(f"Attempt {attempt_id} not found")
            if att_row["status"] == "success":
                raise InvalidAttemptStateError("Cannot mark a successful attempt as failed")
            if att_row["status"] != "running":
                return

            partition_id = att_row["partition_id"]
            lease_stmt = (
                select(partition_leases)
                .where(partition_leases.c.partition_id == partition_id)
                .with_for_update()
            )
            lease_row = conn.execute(lease_stmt).mappings().one_or_none()
            if lease_row and lease_row["attempt_id"] == attempt_id:
                if lease_row["owner_id"] != owner_id or lease_row["generation"] != generation:
                    raise LeaseLostError(
                        f"Lease generation or owner mismatch when attempting to fail attempt {attempt_id}"
                    )
                release_lease(conn, partition_id)

            db_now: datetime = conn.execute(select(func.now())).scalar_one()
            status_val = "canceled" if canceled else "failed"
            conn.execute(
                attempts.update()
                .where(attempts.c.id == attempt_id)
                .values(
                    status=status_val,
                    failure_reason=reason,
                    completed_at=db_now,
                )
            )

    def resolve_request(self, request_id: UUID) -> BeginOrReuseResult | None:
        with self._engine.begin() as conn:
            att = get_attempt_by_request_id(conn, request_id)
            if not att:
                return None
            part_stmt = select(partitions).where(partitions.c.id == att.partition_id)
            part_row = conn.execute(part_stmt).mappings().one()
            part = PartitionRecord(
                id=part_row["id"],
                identity=PartitionIdentity(
                    source=part_row["source"],
                    resource=part_row["resource"],
                    source_date=part_row["source_date"],
                ),
                current_commit_id=part_row["current_commit_id"],
                created_at=part_row["created_at"],
            )
            commit = None
            if part.current_commit_id:
                commit = get_commit_by_id(conn, part.current_commit_id)
            return BeginOrReuseResult(
                reused=(att.status == "success"),
                partition=part,
                commit=commit,
                attempt=att,
            )

    def get_snapshot(self, partitions: list[PartitionIdentity]) -> list[SnapshotView]:
        with self._engine.begin() as conn:
            return get_snapshot_for_partitions(conn, partitions)
