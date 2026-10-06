from datetime import date
from unittest.mock import MagicMock
from uuid import uuid4

from procurement.metadata.models import (
    CommitFileDescriptor,
    CommitRecord,
    PartitionIdentity,
    PartitionRecord,
    SnapshotView,
)
from procurement.tools.bronze_explorer import BronzeTable, select_snapshot_files


def test_select_snapshot_files_retrieves_files_and_commits():
    mock_meta = MagicMock()

    part = PartitionRecord(
        id=uuid4(),
        identity=PartitionIdentity(source="muasamcong", resource="project", source_date=date(2024, 1, 1)),
        current_commit_id=uuid4(),
        created_at=MagicMock(),
    )
    commit = CommitRecord(
        id=part.current_commit_id,
        partition_id=part.id,
        attempt_id=uuid4(),
        parent_commit_id=None,
        data_version=uuid4(),
        record_count=10,
        file_count=1,
        verification={},
        committed_at=MagicMock(),
        files=(
            CommitFileDescriptor(
                commit_id=part.current_commit_id,
                file_number=1,
                table_name="project_detail",
                bucket="test-bucket",
                object_key="bronze/muasamcong/project_detail/part_1.parquet",
                row_count=10,
                size_bytes=1024,
                sha256="a" * 64,
                schema_version=1,
            ),
        ),
    )

    mock_meta.get_snapshot.return_value = [
        SnapshotView(partition=part, commit=commit, files=commit.files)
    ]

    tables = [BronzeTable(dataset="muasamcong", table="project_detail")]
    committed_days = {}

    selected = select_snapshot_files(
        mock_meta,
        tables=tables,
        start=date(2024, 1, 1),
        end=date(2024, 1, 1),
        committed_days=committed_days,
    )

    table_key = tables[0]
    assert len(selected[table_key]) == 1
    assert selected[table_key][0] == "s3://test-bucket/bronze/muasamcong/project_detail/part_1.parquet"
    assert "project" in committed_days
    assert "2024-01-01" in committed_days["project"]
    assert committed_days["project"]["2024-01-01"].record_count == 10
