"""Contract and interface protocols for metadata service operations."""

from datetime import timedelta
from typing import Any, Protocol
from uuid import UUID

from procurement.metadata.models import (
    BeginOrReuseResult,
    CommitFileDescriptor,
    CommitRecord,
    ErrorDescriptor,
    LeaseInfo,
    PageRecordDescriptor,
    PartitionIdentity,
    SnapshotView,
)


class MetadataService(Protocol):
    """Core metadata transaction, lease, attempt, and snapshot interface."""

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
        """Atomically inspect partition and either return existing commit or claim lease for a new attempt."""
        ...

    def renew_lease(
        self,
        attempt_id: UUID,
        *,
        owner_id: UUID,
        generation: int,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> LeaseInfo:
        """Renew active partition lease if holder identity and generation match."""
        ...

    def record_page(self, page: PageRecordDescriptor) -> None:
        """Record pagination progress for an attempt; idempotent on retry."""
        ...

    def record_error(self, error: ErrorDescriptor) -> None:
        """Record an error event associated with an attempt."""
        ...

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
        """Publish attempt files atomically: verify lease/base, write commit & files, mark success, update partition."""
        ...

    def fail_attempt(
        self,
        attempt_id: UUID,
        *,
        owner_id: UUID,
        generation: int,
        reason: str,
        canceled: bool = False,
    ) -> None:
        """Mark attempt as failed/canceled and release the lease; does not modify already succeeded attempts."""
        ...

    def resolve_request(self, request_id: UUID) -> BeginOrReuseResult | None:
        """Resolve state of a previously issued request_id without creating a new attempt."""
        ...

    def get_snapshot(self, partitions: list[PartitionIdentity]) -> list[SnapshotView]:
        """Fetch consistent read snapshot of current commits and file sets for given partitions."""
        ...
