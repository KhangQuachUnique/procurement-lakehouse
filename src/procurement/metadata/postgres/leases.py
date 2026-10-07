"""PostgreSQL implementation of partition lease locking and renewals."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.engine import Connection

from procurement.metadata.errors import (
    LeaseConflictError,
    LeaseLostError,
    StaleGenerationError,
)
from procurement.metadata.models import LeaseInfo
from procurement.metadata.postgres.schema import partition_leases


def get_lease_info(conn: Connection, partition_id: UUID) -> LeaseInfo | None:
    stmt = select(partition_leases).where(partition_leases.c.partition_id == partition_id)
    row = conn.execute(stmt).mappings().one_or_none()
    if not row:
        return None
    return LeaseInfo(
        partition_id=row["partition_id"],
        attempt_id=row["attempt_id"],
        owner_id=row["owner_id"],
        generation=row["generation"],
        heartbeat_at=row["heartbeat_at"],
        expires_at=row["expires_at"],
    )


def lock_and_claim_lease(
    conn: Connection,
    partition_id: UUID,
    *,
    attempt_id: UUID,
    owner_id: UUID,
    lease_duration: timedelta,
    db_now: datetime,
) -> tuple[int, LeaseInfo]:
    """Lock partition lease row and claim for attempt; raises LeaseConflictError if held by another active worker.

    Returns (allocated_generation, LeaseInfo). Note that caller MUST insert the attempt row
    with allocated_generation before this function updates partition_leases, due to foreign key constraints.
    """
    stmt = (
        select(partition_leases)
        .where(partition_leases.c.partition_id == partition_id)
        .with_for_update()
    )
    row = conn.execute(stmt).mappings().one_or_none()
    if row is None:
        conn.execute(
            partition_leases.insert().values(
                partition_id=partition_id,
                generation=0,
                attempt_id=None,
                owner_id=None,
                heartbeat_at=None,
                expires_at=None,
            )
        )
        current_generation = 0
    else:
        current_generation = row["generation"]
        if (
            row["attempt_id"] is not None
            and row["expires_at"] is not None
            and row["expires_at"] > db_now
            and (row["owner_id"] != owner_id or row["attempt_id"] != attempt_id)
        ):
            raise LeaseConflictError(
                f"Partition {partition_id} is currently locked by owner {row['owner_id']} "
                f"until {row['expires_at']}"
            )

    new_generation = current_generation + 1
    return new_generation, LeaseInfo(
        partition_id=partition_id,
        attempt_id=attempt_id,
        owner_id=owner_id,
        generation=new_generation,
        heartbeat_at=db_now,
        expires_at=db_now + lease_duration,
    )


def apply_claimed_lease(
    conn: Connection,
    lease: LeaseInfo,
) -> None:
    """Update partition_leases with claimed attempt after attempt row exists."""
    conn.execute(
        partition_leases.update()
        .where(partition_leases.c.partition_id == lease.partition_id)
        .values(
            attempt_id=lease.attempt_id,
            owner_id=lease.owner_id,
            generation=lease.generation,
            heartbeat_at=lease.heartbeat_at,
            expires_at=lease.expires_at,
        )
    )


def renew_lease(
    conn: Connection,
    partition_id: UUID,
    *,
    attempt_id: UUID,
    owner_id: UUID,
    generation: int,
    lease_duration: timedelta,
    db_now: datetime,
) -> LeaseInfo:
    """Renew lease heartbeat and expiry; ensures lease generation and ownership match."""
    stmt = (
        select(partition_leases)
        .where(partition_leases.c.partition_id == partition_id)
        .with_for_update()
    )
    row = conn.execute(stmt).mappings().one_or_none()
    if not row or row["attempt_id"] != attempt_id:
        raise LeaseLostError(
            f"Attempt {attempt_id} does not currently hold lease on partition {partition_id}"
        )
    if row["owner_id"] != owner_id:
        raise LeaseLostError(
            f"Owner {owner_id} does not match current lease owner {row['owner_id']}"
        )
    if row["generation"] != generation:
        raise StaleGenerationError(
            f"Stale lease generation: expected {generation}, active generation is {row['generation']}"
        )
    if row["expires_at"] is not None and row["expires_at"] <= db_now:
        raise LeaseLostError(
            f"Lease already expired at {row['expires_at']} (current time {db_now})"
        )

    new_expiry = db_now + lease_duration
    conn.execute(
        partition_leases.update()
        .where(partition_leases.c.partition_id == partition_id)
        .values(
            heartbeat_at=db_now,
            expires_at=new_expiry,
        )
    )
    return LeaseInfo(
        partition_id=partition_id,
        attempt_id=attempt_id,
        owner_id=owner_id,
        generation=generation,
        heartbeat_at=db_now,
        expires_at=new_expiry,
    )


def release_lease(conn: Connection, partition_id: UUID) -> None:
    """Clear lease holder fields while preserving generation counter."""
    conn.execute(
        partition_leases.update()
        .where(partition_leases.c.partition_id == partition_id)
        .values(
            attempt_id=None,
            owner_id=None,
            heartbeat_at=None,
            expires_at=None,
        )
    )
