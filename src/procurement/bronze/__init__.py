"""Bronze layer: models, hashing, paths, writers, readers, and verification."""

from procurement.bronze.dlt_writer import (
    DltBronzeWriter,
    create_bronze_destination,
    create_bronze_resource,
)
from procurement.bronze.hashing import calculate_content_hash
from procurement.bronze.models import BronzeRecord
from procurement.bronze.paths import bronze_attempt_prefix, bronze_layout_template
from procurement.bronze.reader import iter_bronze_records
from procurement.bronze.verification import (
    calculate_file_sha256,
    verify_record_content_hash,
    verify_record_lineage,
)
from procurement.bronze.writer import (
    BronzeWriteError,
    BronzeWriter,
    BufferedBronzeWriter,
    serialize_record,
)

__all__ = [
    "BronzeRecord",
    "BronzeWriteError",
    "BronzeWriter",
    "BufferedBronzeWriter",
    "DltBronzeWriter",
    "bronze_attempt_prefix",
    "bronze_layout_template",
    "calculate_content_hash",
    "calculate_file_sha256",
    "create_bronze_destination",
    "create_bronze_resource",
    "iter_bronze_records",
    "serialize_record",
    "verify_record_content_hash",
    "verify_record_lineage",
]
