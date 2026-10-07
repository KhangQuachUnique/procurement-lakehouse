"""Application metadata; independent of Dagster's instance database."""

from procurement.metadata.contracts import MetadataService
from procurement.metadata.errors import (
    AttemptNotFoundError,
    BaseCommitMismatchError,
    InvalidAttemptStateError,
    LeaseConflictError,
    LeaseError,
    LeaseLostError,
    MetadataError,
    PartitionNotFoundError,
    StaleGenerationError,
)
from procurement.metadata.models import (
    AttemptDescriptor,
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
from procurement.metadata.service import PostgresMetadataService

__all__ = [
    "AttemptDescriptor",
    "AttemptNotFoundError",
    "BaseCommitMismatchError",
    "BeginOrReuseResult",
    "CommitFileDescriptor",
    "CommitRecord",
    "ErrorDescriptor",
    "InvalidAttemptStateError",
    "LeaseConflictError",
    "LeaseError",
    "LeaseInfo",
    "LeaseLostError",
    "MetadataError",
    "MetadataService",
    "PageRecordDescriptor",
    "PartitionIdentity",
    "PartitionNotFoundError",
    "PartitionRecord",
    "PostgresMetadataService",
    "SnapshotView",
    "StaleGenerationError",
]
