"""Compatibility shim: Hashing moved to procurement.bronze.hashing."""

from datetime import UTC, datetime

from procurement.bronze.hashing import calculate_content_hash


def utc_now() -> datetime:
    return datetime.now(UTC)


__all__ = ["calculate_content_hash", "utc_now"]
