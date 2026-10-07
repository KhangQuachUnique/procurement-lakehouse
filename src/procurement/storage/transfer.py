"""Compatibility shim for storage transfer.

Delegates to `procurement.transfer`.
"""
from procurement.ingestion.coverage import read_coverage
from procurement.storage.control import commit_day_manifest
from procurement.transfer import (
    ImportRun,
    TransferError,
    copy_digest,
    export_bundle,
    import_bundle,
    inspect_bundle,
)
from procurement.transfer.export import _bucket, _key, _relative, _source_plan
from procurement.transfer.importer import _put_missing

__all__ = [
    "ImportRun",
    "TransferError",
    "_bucket",
    "_key",
    "_put_missing",
    "_relative",
    "_source_plan",
    "commit_day_manifest",
    "copy_digest",
    "export_bundle",
    "import_bundle",
    "inspect_bundle",
    "read_coverage",
]
