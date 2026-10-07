"""Compaction recovery, interruption handling, and execution state checks."""


class UnconfirmedCompaction(RuntimeError):
    """Do not retry or mark FAILED until the existing publication has been inspected."""


class CompactionVerificationError(ValueError):
    """Raised when data verification fails during compaction."""
