"""Domain models and DTOs for Bronze metadata (non-ORM, immutable)."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class PartitionIdentity:
    source: str
    resource: str
    source_date: date


@dataclass(frozen=True)
class PartitionRecord:
    id: UUID
    identity: PartitionIdentity
    current_commit_id: UUID | None
    created_at: datetime


@dataclass(frozen=True)
class AttemptDescriptor:
    id: UUID
    partition_id: UUID
    request_id: UUID
    kind: str
    status: str
    owner_id: UUID
    lease_generation: int
    base_commit_id: UUID | None
    dagster_run_id: str | None
    config: dict[str, Any]
    failure_reason: str | None
    started_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True)
class LeaseInfo:
    partition_id: UUID
    attempt_id: UUID | None
    owner_id: UUID | None
    generation: int
    heartbeat_at: datetime | None
    expires_at: datetime | None

    @property
    def is_held(self) -> bool:
        return self.attempt_id is not None and self.owner_id is not None


@dataclass(frozen=True)
class CommitFileDescriptor:
    commit_id: UUID
    file_number: int
    table_name: str
    bucket: str
    object_key: str
    row_count: int
    size_bytes: int
    sha256: str
    schema_version: int


@dataclass(frozen=True)
class CommitRecord:
    id: UUID
    partition_id: UUID
    attempt_id: UUID
    parent_commit_id: UUID | None
    data_version: UUID
    record_count: int
    file_count: int
    verification: dict[str, Any]
    committed_at: datetime
    files: tuple[CommitFileDescriptor, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class BeginOrReuseResult:
    reused: bool
    partition: PartitionRecord
    commit: CommitRecord | None = None
    attempt: AttemptDescriptor | None = None
    lease: LeaseInfo | None = None
    refresh_in_progress: bool = False


@dataclass(frozen=True)
class PageRecordDescriptor:
    attempt_id: UUID
    page_number: int
    page_size: int
    status: str
    search_items: int = 0
    bronze_records: int = 0
    error_count: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class ErrorDescriptor:
    id: UUID
    attempt_id: UUID
    stage: str
    error_type: str
    message: str
    page_number: int | None = None
    source_record_id: str | None = None
    http_status: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime | None = None


@dataclass(frozen=True)
class SnapshotView:
    partition: PartitionRecord
    commit: CommitRecord
    files: tuple[CommitFileDescriptor, ...]
