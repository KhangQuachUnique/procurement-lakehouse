"""PostgreSQL implementation of atomic commits, commit files, and snapshot views."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select, tuple_
from sqlalchemy.engine import Connection

from procurement.metadata.errors import (
    AttemptNotFoundError,
    BaseCommitMismatchError,
    InvalidAttemptStateError,
    LeaseLostError,
    StaleGenerationError,
)
from procurement.metadata.models import (
    CommitFileDescriptor,
    CommitRecord,
    PartitionIdentity,
    PartitionRecord,
    SnapshotView,
)
from procurement.metadata.postgres.leases import release_lease
from procurement.metadata.postgres.schema import (
    attempts,
    commit_files,
    commits,
    partition_leases,
    partitions,
)


def get_partition_by_identity(
    conn: Connection, identity: PartitionIdentity
) -> PartitionRecord | None:
    stmt = select(partitions).where(
        partitions.c.source == identity.source,
        partitions.c.resource == identity.resource,
        partitions.c.source_date == identity.source_date,
    )
    row = conn.execute(stmt).mappings().one_or_none()
    if not row:
        return None
    return PartitionRecord(
        id=row["id"],
        identity=identity,
        current_commit_id=row["current_commit_id"],
        created_at=row["created_at"],
    )


def get_commit_by_id(conn: Connection, commit_id: UUID) -> CommitRecord | None:
    stmt = select(commits).where(commits.c.id == commit_id)
    row = conn.execute(stmt).mappings().one_or_none()
    if not row:
        return None

    files_stmt = (
        select(commit_files)
        .where(commit_files.c.commit_id == commit_id)
        .order_by(commit_files.c.file_number)
    )
    file_rows = conn.execute(files_stmt).mappings().all()
    files = tuple(
        CommitFileDescriptor(
            commit_id=f["commit_id"],
            file_number=f["file_number"],
            table_name=f["table_name"],
            bucket=f["bucket"],
            object_key=f["object_key"],
            row_count=f["row_count"],
            size_bytes=f["size_bytes"],
            sha256=f["sha256"],
            schema_version=f["schema_version"],
        )
        for f in file_rows
    )

    return CommitRecord(
        id=row["id"],
        partition_id=row["partition_id"],
        attempt_id=row["attempt_id"],
        parent_commit_id=row["parent_commit_id"],
        data_version=row["data_version"],
        record_count=row["record_count"],
        file_count=row["file_count"],
        verification=row["verification"],
        committed_at=row["committed_at"],
        files=files,
    )


def publish_commit(
    conn: Connection,
    *,
    attempt_id: UUID,
    owner_id: UUID,
    generation: int,
    expected_base_commit_id: UUID | None,
    record_count: int,
    files: list[CommitFileDescriptor],
    verification: dict,
    db_now: datetime,
) -> CommitRecord:
    """Publish attempt files atomically under active lease and base commit validation."""
    # 1. Lock and check attempt
    att_stmt = select(attempts).where(attempts.c.id == attempt_id).with_for_update()
    att_row = conn.execute(att_stmt).mappings().one_or_none()
    if not att_row:
        raise AttemptNotFoundError(f"Attempt {attempt_id} not found")
    if att_row["status"] != "running":
        raise InvalidAttemptStateError(
            f"Attempt {attempt_id} status is '{att_row['status']}', expected 'running'"
        )
    partition_id = att_row["partition_id"]

    # 2. Lock and check lease
    lease_stmt = (
        select(partition_leases)
        .where(partition_leases.c.partition_id == partition_id)
        .with_for_update()
    )
    lease_row = conn.execute(lease_stmt).mappings().one_or_none()
    if not lease_row or lease_row["attempt_id"] != attempt_id or lease_row["owner_id"] != owner_id:
        raise LeaseLostError(
            f"Lease on partition {partition_id} is not held by attempt {attempt_id}"
        )
    if lease_row["generation"] != generation:
        raise StaleGenerationError(
            f"Stale generation: expected {generation}, active is {lease_row['generation']}"
        )
    if lease_row["expires_at"] is not None and lease_row["expires_at"] <= db_now:
        raise LeaseLostError(f"Lease expired at {lease_row['expires_at']} (db_now {db_now})")

    # 3. Lock and check partition base commit
    part_stmt = select(partitions).where(partitions.c.id == partition_id).with_for_update()
    part_row = conn.execute(part_stmt).mappings().one()
    if part_row["current_commit_id"] != expected_base_commit_id:
        raise BaseCommitMismatchError(
            f"Base commit conflict on partition {partition_id}: "
            f"expected {expected_base_commit_id}, but found {part_row['current_commit_id']}"
        )

    # 4. Insert commit record
    new_commit_id = uuid4()
    data_version = uuid4()
    file_count = len(files)
    conn.execute(
        commits.insert().values(
            id=new_commit_id,
            partition_id=partition_id,
            attempt_id=attempt_id,
            parent_commit_id=expected_base_commit_id,
            data_version=data_version,
            record_count=record_count,
            file_count=file_count,
            verification=verification,
            committed_at=db_now,
        )
    )

    # 5. Insert commit files
    if files:
        conn.execute(
            commit_files.insert(),
            [
                {
                    "commit_id": new_commit_id,
                    "file_number": f.file_number,
                    "table_name": f.table_name,
                    "bucket": f.bucket,
                    "object_key": f.object_key,
                    "row_count": f.row_count,
                    "size_bytes": f.size_bytes,
                    "sha256": f.sha256,
                    "schema_version": f.schema_version,
                }
                for f in files
            ],
        )

    # 6. Update attempt to success
    conn.execute(
        attempts.update()
        .where(attempts.c.id == attempt_id)
        .values(
            status="success",
            completed_at=db_now,
        )
    )

    # 7. Update partition current commit
    conn.execute(
        partitions.update()
        .where(partitions.c.id == partition_id)
        .values(current_commit_id=new_commit_id)
    )

    # 8. Release lease
    release_lease(conn, partition_id)

    files_tuple = tuple(
        CommitFileDescriptor(
            commit_id=new_commit_id,
            file_number=f.file_number,
            table_name=f.table_name,
            bucket=f.bucket,
            object_key=f.object_key,
            row_count=f.row_count,
            size_bytes=f.size_bytes,
            sha256=f.sha256,
            schema_version=f.schema_version,
        )
        for f in files
    )
    return CommitRecord(
        id=new_commit_id,
        partition_id=partition_id,
        attempt_id=attempt_id,
        parent_commit_id=expected_base_commit_id,
        data_version=data_version,
        record_count=record_count,
        file_count=file_count,
        verification=verification,
        committed_at=db_now,
        files=files_tuple,
    )


def get_snapshot_for_partitions(
    conn: Connection,
    partition_identities: list[PartitionIdentity],
) -> list[SnapshotView]:
    """Return consistent read snapshot for given partition identities."""
    if not partition_identities:
        return []

    if len(partition_identities) <= 5:
        views: list[SnapshotView] = []
        for identity in partition_identities:
            part = get_partition_by_identity(conn, identity)
            if not part or part.current_commit_id is None:
                continue
            commit = get_commit_by_id(conn, part.current_commit_id)
            if not commit:
                continue
            views.append(
                SnapshotView(
                    partition=part,
                    commit=commit,
                    files=commit.files,
                )
            )
        return views

    # Batch resolution for larger lists to avoid N+1 query overhead
    partitions_map: dict[tuple[str, str, Any], PartitionRecord] = {}
    chunk_size = 500
    for i in range(0, len(partition_identities), chunk_size):
        chunk = partition_identities[i : i + chunk_size]
        sources = {ident.source for ident in chunk}
        resources = {ident.resource for ident in chunk}
        if len(sources) == 1 and len(resources) == 1:
            src = next(iter(sources))
            res = next(iter(resources))
            dates = [ident.source_date for ident in chunk]
            stmt = select(partitions).where(
                partitions.c.source == src,
                partitions.c.resource == res,
                partitions.c.source_date.in_(dates),
            )
        else:
            stmt = select(partitions).where(
                tuple_(partitions.c.source, partitions.c.resource, partitions.c.source_date).in_(
                    [(ident.source, ident.resource, ident.source_date) for ident in chunk]
                )
            )
        for row in conn.execute(stmt).mappings():
            key = (row["source"], row["resource"], row["source_date"])
            partitions_map[key] = PartitionRecord(
                id=row["id"],
                identity=PartitionIdentity(
                    source=row["source"],
                    resource=row["resource"],
                    source_date=row["source_date"],
                ),
                current_commit_id=row["current_commit_id"],
                created_at=row["created_at"],
            )

    active_commit_ids = [
        p.current_commit_id
        for p in partitions_map.values()
        if p.current_commit_id is not None
    ]
    if not active_commit_ids:
        return []

    commits_list = list(set(active_commit_ids))
    commits_map: dict[UUID, CommitRecord] = {}
    files_map: dict[UUID, list[CommitFileDescriptor]] = {cid: [] for cid in commits_list}

    for i in range(0, len(commits_list), chunk_size):
        c_chunk = commits_list[i : i + chunk_size]
        f_stmt = (
            select(commit_files)
            .where(commit_files.c.commit_id.in_(c_chunk))
            .order_by(commit_files.c.commit_id, commit_files.c.file_number)
        )
        for f in conn.execute(f_stmt).mappings():
            files_map[f["commit_id"]].append(
                CommitFileDescriptor(
                    commit_id=f["commit_id"],
                    file_number=f["file_number"],
                    table_name=f["table_name"],
                    bucket=f["bucket"],
                    object_key=f["object_key"],
                    row_count=f["row_count"],
                    size_bytes=f["size_bytes"],
                    sha256=f["sha256"],
                    schema_version=f["schema_version"],
                )
            )

        c_stmt = select(commits).where(commits.c.id.in_(c_chunk))
        for row in conn.execute(c_stmt).mappings():
            cid = row["id"]
            commits_map[cid] = CommitRecord(
                id=cid,
                partition_id=row["partition_id"],
                attempt_id=row["attempt_id"],
                parent_commit_id=row["parent_commit_id"],
                data_version=row["data_version"],
                record_count=row["record_count"],
                file_count=row["file_count"],
                verification=row["verification"],
                committed_at=row["committed_at"],
                files=tuple(files_map.get(cid, ())),
            )

    views = []
    for ident in partition_identities:
        key = (ident.source, ident.resource, ident.source_date)
        part = partitions_map.get(key)
        if not part or part.current_commit_id is None:
            continue
        commit = commits_map.get(part.current_commit_id)
        if not commit:
            continue
        views.append(
            SnapshotView(
                partition=part,
                commit=commit,
                files=commit.files,
            )
        )
    return views
