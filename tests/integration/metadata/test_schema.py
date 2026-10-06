"""Database-enforced invariants; lifecycle/concurrent-worker service tests follow later."""

from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import IntegrityError

from procurement.metadata.postgres.schema import (
    SCHEMA,
    attempt_pages,
    attempts,
    commit_files,
    commits,
    errors,
    partition_leases,
    partitions,
)

pytestmark = pytest.mark.integration


def partition(conn, day=date(2025, 1, 1)):
    id_ = uuid4()
    conn.execute(
        partitions.insert().values(
            id=id_,
            source="muasamcong",
            resource="project",
            source_date=day,
        )
    )
    return id_


def attempt(conn, partition_id, **overrides):
    values = dict(
        id=uuid4(),
        partition_id=partition_id,
        request_id=uuid4(),
        kind="ingestion",
        owner_id=uuid4(),
        lease_generation=1,
    )
    values.update(overrides)
    conn.execute(attempts.insert().values(**values))
    return values


def commit(conn, a, **overrides):
    values = dict(
        id=uuid4(),
        partition_id=a["partition_id"],
        attempt_id=a["id"],
        data_version=uuid4(),
        record_count=0,
        file_count=0,
        verification={},
    )
    values.update(overrides)
    conn.execute(commits.insert().values(**values))
    return values


def rejected(conn, statement):
    with pytest.raises(IntegrityError), conn.begin_nested():
        conn.execute(statement)


def test_migration_roundtrip_and_model_drift(database):
    engine, config = database
    with engine.connect() as conn:
        config.attributes["connection"] = conn
        command.check(config)
        conn.commit()
        command.downgrade(config, "base")
        assert not inspect(conn).has_schema(SCHEMA)
        conn.commit()
        command.upgrade(config, "head")
        command.upgrade(config, "head")
        assert set(inspect(conn).get_table_names(schema=SCHEMA)) == {
            "partitions",
            "attempts",
            "attempt_pages",
            "partition_leases",
            "commits",
            "commit_files",
            "errors",
        }
        assert (
            conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            == "0001_bronze_core"
        )
    config.attributes.pop("connection", None)


def test_partition_and_request_uniqueness(connection):
    p = partition(connection)
    rejected(
        connection,
        partitions.insert().values(
            id=uuid4(),
            source="muasamcong",
            resource="project",
            source_date=date(2025, 1, 1),
        ),
    )
    a = attempt(connection, p)
    rejected(connection, attempts.insert().values({**a, "id": uuid4()}))


@pytest.mark.parametrize(
    "values",
    [
        {"status": "unknown"},
        {"status": "success"},
        {"status": "failed"},
        {"lease_generation": 0},
        {"kind": "invalid"},
        {"config": []},
    ],
)
def test_invalid_attempt_states(connection, values):
    p = partition(connection)
    with pytest.raises(IntegrityError), connection.begin_nested():
        attempt(connection, p, **values)


def test_cross_partition_references_rejected(connection):
    p1, p2 = partition(connection), partition(connection, date(2025, 1, 2))
    a1, a2 = attempt(connection, p1), attempt(connection, p2)
    c1 = commit(connection, a1)
    rejected(
        connection,
        commits.insert().values(
            **{**c1, "id": uuid4(), "partition_id": p2},
        ),
    )
    rejected(
        connection,
        partitions.update().where(partitions.c.id == p2).values(current_commit_id=c1["id"]),
    )
    rejected(
        connection,
        attempts.update().where(attempts.c.id == a2["id"]).values(base_commit_id=c1["id"]),
    )
    with pytest.raises(IntegrityError), connection.begin_nested():
        commit(connection, a2, parent_commit_id=c1["id"])


def test_one_commit_per_attempt_and_empty_commit(connection):
    p = partition(connection)
    a = attempt(connection, p)
    c = commit(connection, a)
    connection.execute(
        partitions.update().where(partitions.c.id == p).values(current_commit_id=c["id"])
    )
    assert connection.execute(select(partitions.c.current_commit_id)).scalar_one() == c["id"]
    rejected(connection, commits.insert().values({**c, "id": uuid4()}))


def test_lease_holder_must_match_attempt_owner_generation_and_partition(connection):
    p = partition(connection)
    a = attempt(connection, p)
    now = datetime.now(UTC)
    values = dict(
        partition_id=p,
        attempt_id=a["id"],
        owner_id=a["owner_id"],
        generation=1,
        heartbeat_at=now,
        expires_at=now + timedelta(seconds=60),
    )
    for wrong in (
        {"owner_id": uuid4()},
        {"generation": 2},
        {"expires_at": now},
        {"attempt_id": None},
        {"owner_id": None},
    ):
        rejected(connection, partition_leases.insert().values({**values, **wrong}))
    connection.execute(partition_leases.insert().values(**values))
    rejected(connection, partition_leases.insert().values(**values))
    connection.execute(
        partition_leases.update().values(
            attempt_id=None,
            owner_id=None,
            heartbeat_at=None,
            expires_at=None,
        )
    )
    assert connection.execute(select(partition_leases.c.generation)).scalar_one() == 1


def test_files_have_checksum_bounds_and_unique_storage_identity(connection):
    p = partition(connection)
    c = commit(connection, attempt(connection, p), record_count=1, file_count=1)
    values = dict(
        commit_id=c["id"],
        file_number=0,
        table_name="project_detail",
        bucket="test",
        object_key="bronze/attempt/part.parquet",
        row_count=1,
        size_bytes=128,
        sha256="a" * 64,
        schema_version=1,
    )
    for wrong in ({"sha256": "invalid"}, {"row_count": -1}, {"size_bytes": 0}, {"bucket": ""}):
        rejected(connection, commit_files.insert().values({**values, **wrong}))
    connection.execute(commit_files.insert().values(**values))
    rejected(connection, commit_files.insert().values({**values, "file_number": 1}))


def test_page_and_error_idempotency_and_bounds(connection):
    a = attempt(connection, partition(connection))
    page = dict(attempt_id=a["id"], page_number=0, page_size=50, status="running")
    connection.execute(attempt_pages.insert().values(**page))
    rejected(connection, attempt_pages.insert().values(**page))
    rejected(connection, attempt_pages.insert().values({**page, "page_number": -1}))
    error = dict(
        id=uuid4(),
        attempt_id=a["id"],
        page_number=2,
        stage="fetch",
        error_type="HTTPError",
        message="fixture failure",
        http_status=500,
    )
    # Error context may refer to a page that could not be initialized.
    connection.execute(errors.insert().values(**error))
    rejected(connection, errors.insert().values(**error))
    rejected(connection, errors.insert().values({**error, "id": uuid4(), "http_status": 999}))


def test_transaction_rollback_does_not_publish_partial_metadata(connection):
    p = partition(connection)
    a = attempt(connection, p)
    with pytest.raises(RuntimeError), connection.begin_nested():
        c = commit(connection, a)
        connection.execute(
            partitions.update().where(partitions.c.id == p).values(current_commit_id=c["id"])
        )
        raise RuntimeError("simulated interruption before transaction commit")
    assert connection.execute(select(partitions.c.current_commit_id)).scalar_one() is None
    assert connection.execute(select(commits.c.id)).first() is None
