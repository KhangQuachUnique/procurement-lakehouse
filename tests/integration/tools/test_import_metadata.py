from datetime import UTC, date, datetime
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.metadata.postgres.schema import (
    attempts,
    commit_files,
    commits,
    partitions,
)
from procurement.models.control import DayManifest, DayStatus
from procurement.storage.control import write_day_manifest
from procurement.tools.import_metadata import import_resource_manifests

pytestmark = pytest.mark.integration


def test_legacy_manifest_write_guard(monkeypatch):
    import fsspec

    fs = fsspec.filesystem("memory")
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", "mem-bucket")
    identity = ResourceIdentity(source="muasamcong", resource="project")
    manifest = DayManifest(
        schema_version=1,
        run_id="legacy-run-1",
        source="muasamcong",
        resource="project",
        source_date=date(2024, 1, 1),
        status=DayStatus.SUCCESS,
        started_at=datetime.now(UTC),
    )

    # 1. Deprecation warning by default
    with pytest.deprecated_call():
        write_day_manifest(fs, identity, manifest)

    # 2. Blocked when PREVENT_LEGACY_MANIFEST_WRITES=1
    monkeypatch.setenv("PREVENT_LEGACY_MANIFEST_WRITES", "1")
    with pytest.raises(RuntimeError, match="Legacy S3 manifest writing is disabled"):
        write_day_manifest(fs, identity, manifest)


def test_import_historical_metadata_to_postgres(database, store):
    engine, _ = database
    fs, bucket = store

    identity = ResourceIdentity(source="muasamcong", resource="project")
    run_id = f"hist-run-{uuid4().hex[:8]}"
    source_date = date(2024, 3, 1)

    # 1. Create a dummy Parquet file on store
    table = pa.Table.from_pydict(
        {
            "id": ["1", "2", "3"],
            "name": ["A", "B", "C"],
        }
    )
    parquet_path = f"{bucket}/bronze/muasamcong/project_detail/source_date=2024-03-01/run_id={run_id}/part_0.parquet"
    fs.mkdirs(f"{bucket}/bronze/muasamcong/project_detail/source_date=2024-03-01/run_id={run_id}")
    with fs.open(parquet_path, "wb") as f:
        pq.write_table(table, f)

    # 2. Write legacy DayManifest to S3
    now = datetime.now(UTC)
    manifest = DayManifest(
        schema_version=1,
        run_id=run_id,
        source="muasamcong",
        resource="project",
        source_date=source_date,
        status=DayStatus.SUCCESS,
        bronze_records=3,
        completed_pages=1,
        started_at=now,
        completed_at=now,
    )
    write_day_manifest(fs, identity, manifest)

    # 3. Run import_resource_manifests
    stats = import_resource_manifests(
        engine,
        fs,
        "project",
        source="muasamcong",
        bucket=bucket,
    )

    assert stats["imported"] == 1
    assert stats["total_manifest_days"] == 1
    assert stats["skipped"] == 0

    # 4. Verify in PostgreSQL
    with engine.begin() as conn:
        part_row = (
            conn.execute(
                sa.select(partitions).where(
                    partitions.c.source == "muasamcong",
                    partitions.c.resource == "project",
                    partitions.c.source_date == source_date,
                )
            )
            .mappings()
            .one()
        )

        assert part_row["current_commit_id"] is not None

        commit_row = (
            conn.execute(sa.select(commits).where(commits.c.id == part_row["current_commit_id"]))
            .mappings()
            .one()
        )

        assert commit_row["record_count"] == 3
        assert commit_row["file_count"] == 1
        assert commit_row["verification"]["migrated_from"] == "s3_manifest"

        attempt_row = (
            conn.execute(sa.select(attempts).where(attempts.c.id == commit_row["attempt_id"]))
            .mappings()
            .one()
        )
        assert attempt_row["dagster_run_id"] == run_id

        file_rows = (
            conn.execute(
                sa.select(commit_files).where(commit_files.c.commit_id == commit_row["id"])
            )
            .mappings()
            .all()
        )

        assert len(file_rows) == 1
        assert file_rows[0]["table_name"] == "project_detail"
        assert file_rows[0]["row_count"] == 3
        assert file_rows[0]["size_bytes"] > 0
        assert len(file_rows[0]["sha256"]) == 64

    # 5. Idempotent re-run
    second_stats = import_resource_manifests(
        engine,
        fs,
        "project",
        source="muasamcong",
        bucket=bucket,
    )
    assert second_stats["imported"] == 0
    assert second_stats["skipped"] == 1
