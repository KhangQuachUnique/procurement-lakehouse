"""Integration tests for PostgresMetadataService lifecycle, concurrency, and fencing."""

from datetime import date, timedelta
from uuid import uuid4

import pytest

from procurement.metadata import (
    CommitFileDescriptor,
    LeaseConflictError,
    LeaseLostError,
    PageRecordDescriptor,
    PartitionIdentity,
    PostgresMetadataService,
    StaleGenerationError,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def service(database):
    engine, _ = database
    return PostgresMetadataService(engine)


def make_file(commit_id, num=0):
    return CommitFileDescriptor(
        commit_id=commit_id,
        file_number=num,
        table_name="project",
        bucket="test-bucket",
        object_key=f"raw/project/{commit_id}/part-{num}.parquet",
        row_count=100,
        size_bytes=4096,
        sha256="a" * 64,
        schema_version=1,
    )


def test_full_materialize_and_reuse_flow(service):
    identity = PartitionIdentity(
        source="muasamcong", resource="project", source_date=date(2025, 1, 1)
    )
    req_id = uuid4()
    owner_id = uuid4()

    # 1. Begin new partition ingestion
    begin_res = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=req_id,
        owner_id=owner_id,
    )
    assert not begin_res.reused
    assert begin_res.attempt is not None
    assert begin_res.attempt.status == "running"
    assert begin_res.lease is not None
    assert begin_res.lease.generation == 1

    attempt_id = begin_res.attempt.id
    gen = begin_res.lease.generation

    # 2. Record page
    service.record_page(
        PageRecordDescriptor(
            attempt_id=attempt_id,
            page_number=1,
            page_size=50,
            status="success",
            search_items=50,
            bronze_records=50,
        )
    )

    # 3. Publish commit
    files = [make_file(attempt_id, 0)]
    commit = service.publish_commit(
        attempt_id,
        owner_id=owner_id,
        generation=gen,
        expected_base_commit_id=None,
        record_count=50,
        files=files,
        verification={"checksum": "ok"},
    )
    assert commit.record_count == 50
    assert len(commit.files) == 1

    # 4. Next invocation without refresh must reuse
    reuse_res = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=uuid4(),
    )
    assert reuse_res.reused
    assert reuse_res.commit is not None
    assert reuse_res.commit.id == commit.id
    assert not reuse_res.refresh_in_progress


def test_worker_lease_conflict(service):
    identity = PartitionIdentity(
        source="muasamcong", resource="project", source_date=date(2025, 1, 2)
    )
    worker1 = uuid4()
    worker2 = uuid4()

    res1 = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=worker1,
        lease_duration=timedelta(minutes=10),
    )
    assert not res1.reused

    # Worker 2 attempts refresh while worker 1 holds active lease
    with pytest.raises(LeaseConflictError):
        service.begin_or_reuse(
            identity,
            refresh=True,
            request_id=uuid4(),
            owner_id=worker2,
        )


def test_lease_renewal_and_stale_generation(service):
    identity = PartitionIdentity(
        source="muasamcong", resource="project", source_date=date(2025, 1, 3)
    )
    owner = uuid4()

    res = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=owner,
    )
    att_id = res.attempt.id
    gen = res.lease.generation

    # Successful renewal
    renewed = service.renew_lease(att_id, owner_id=owner, generation=gen)
    assert renewed.generation == gen

    # Wrong generation fails
    with pytest.raises(StaleGenerationError):
        service.renew_lease(att_id, owner_id=owner, generation=gen + 99)

    # Wrong owner fails
    with pytest.raises(LeaseLostError):
        service.renew_lease(att_id, owner_id=uuid4(), generation=gen)


def test_stale_worker_commit_rejected_after_lease_loss(service):
    identity = PartitionIdentity(
        source="muasamcong", resource="project", source_date=date(2025, 1, 4)
    )
    worker1 = uuid4()
    worker2 = uuid4()

    # Worker 1 starts with very short lease (1 second)
    res1 = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=worker1,
        lease_duration=timedelta(seconds=1),
    )
    w1_attempt_id = res1.attempt.id
    w1_gen = res1.lease.generation

    # Wait for worker 1 lease to expire in DB
    import time

    time.sleep(1.2)

    # Worker 2 claims the partition; generation increases
    res2 = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=worker2,
    )
    assert res2.lease.generation == w1_gen + 1

    # Worker 1 wakes up and tries to commit with old generation
    with pytest.raises(LeaseLostError):
        service.publish_commit(
            w1_attempt_id,
            owner_id=worker1,
            generation=w1_gen,
            expected_base_commit_id=None,
            record_count=10,
            files=[],
            verification={},
        )


def test_idempotent_retry_of_same_request_id(service):
    identity = PartitionIdentity(
        source="muasamcong", resource="project", source_date=date(2025, 1, 5)
    )
    req_id = uuid4()
    owner = uuid4()

    res1 = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=req_id,
        owner_id=owner,
    )
    att_id = res1.attempt.id

    # Commit attempt
    files = [make_file(att_id, 0)]
    service.publish_commit(
        att_id,
        owner_id=owner,
        generation=res1.lease.generation,
        expected_base_commit_id=None,
        record_count=20,
        files=files,
        verification={},
    )

    # Re-call with same request_id
    res_retry = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=req_id,
        owner_id=owner,
    )
    assert res_retry.reused
    assert res_retry.attempt.id == att_id


def test_fail_attempt_releases_lease(service):
    identity = PartitionIdentity(
        source="muasamcong", resource="project", source_date=date(2025, 1, 6)
    )
    worker1 = uuid4()
    worker2 = uuid4()

    res1 = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=worker1,
    )
    att1 = res1.attempt.id
    gen1 = res1.lease.generation

    # Worker 1 fails
    service.fail_attempt(
        att1,
        owner_id=worker1,
        generation=gen1,
        reason="Network timeout during crawl",
    )

    # Attempt cannot be failed twice or failed after success
    # Worker 2 can now claim without conflict
    res2 = service.begin_or_reuse(
        identity,
        refresh=False,
        request_id=uuid4(),
        owner_id=worker2,
    )
    assert not res2.reused
    assert res2.lease.generation == gen1 + 1


def test_snapshot_query(service):
    id1 = PartitionIdentity(source="muasamcong", resource="project", source_date=date(2025, 1, 7))
    id2 = PartitionIdentity(source="muasamcong", resource="project", source_date=date(2025, 1, 8))

    # Commit id1
    r1 = service.begin_or_reuse(id1, refresh=False, request_id=uuid4(), owner_id=uuid4())
    f1 = [make_file(r1.attempt.id, 0)]
    c1 = service.publish_commit(
        r1.attempt.id,
        owner_id=r1.attempt.owner_id,
        generation=r1.lease.generation,
        expected_base_commit_id=None,
        record_count=100,
        files=f1,
        verification={},
    )

    # Commit id2
    r2 = service.begin_or_reuse(id2, refresh=False, request_id=uuid4(), owner_id=uuid4())
    f2 = [make_file(r2.attempt.id, 0), make_file(r2.attempt.id, 1)]
    c2 = service.publish_commit(
        r2.attempt.id,
        owner_id=r2.attempt.owner_id,
        generation=r2.lease.generation,
        expected_base_commit_id=None,
        record_count=200,
        files=f2,
        verification={},
    )

    snapshots = service.get_snapshot([id1, id2])
    assert len(snapshots) == 2
    by_date = {s.partition.identity.source_date: s for s in snapshots}
    assert by_date[date(2025, 1, 7)].commit.id == c1.id
    assert len(by_date[date(2025, 1, 7)].files) == 1
    assert by_date[date(2025, 1, 8)].commit.id == c2.id
    assert len(by_date[date(2025, 1, 8)].files) == 2
