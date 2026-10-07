"""Domain error types for procurement metadata."""


class MetadataError(Exception):
    """Base class for all metadata service errors."""


class PartitionNotFoundError(MetadataError):
    """Requested partition does not exist."""


class AttemptNotFoundError(MetadataError):
    """Requested attempt does not exist."""


class InvalidAttemptStateError(MetadataError):
    """Attempt is in an invalid state for the requested operation."""


class LeaseError(MetadataError):
    """Base class for lease-related failures."""


class LeaseConflictError(LeaseError):
    """Another active worker currently holds the partition lease."""


class LeaseLostError(LeaseError):
    """Lease was lost, expired, or revoked."""


class StaleGenerationError(LeaseLostError):
    """Lease generation in request does not match current partition generation."""


class BaseCommitMismatchError(MetadataError):
    """Base commit changed during execution; optimistic concurrency conflict."""
