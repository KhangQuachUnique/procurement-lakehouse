"""Compatibility shim for transfer archive.

Delegates to `procurement.transfer.archive`.
"""
import sys

from procurement.transfer import archive
from procurement.transfer.archive import (
    CHUNK_SIZE,
    MAX_INDEX_BYTES,
    MAX_JSON_BYTES,
    ArchiveFilesystem,
    BundleDay,
    BundleIndex,
    BundleModel,
    ObjectDigest,
    ValidatedArchive,
    copy_digest,
    project_import_run,
    read_json_member,
    safe_key,
    selection_for,
    validate_day_metadata,
    year_dates,
)

sys.modules[__name__] = archive

__all__ = [
    "CHUNK_SIZE",
    "MAX_INDEX_BYTES",
    "MAX_JSON_BYTES",
    "ArchiveFilesystem",
    "BundleDay",
    "BundleIndex",
    "BundleModel",
    "ObjectDigest",
    "ValidatedArchive",
    "copy_digest",
    "project_import_run",
    "read_json_member",
    "safe_key",
    "selection_for",
    "validate_day_metadata",
    "year_dates",
]
