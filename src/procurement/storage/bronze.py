import sys

from dlt.destinations import filesystem

from procurement.bronze import dlt_writer as _dlt_writer
from procurement.bronze.dlt_writer import (
    DltBronzeWriter,
    create_bronze_resource,
)
from procurement.bronze.writer import (
    BronzeWriteError,
    BronzeWriter,
    BufferedBronzeWriter,
    serialize_record,
)


def create_bronze_destination(*args, **kwargs):
    current = sys.modules.get(__name__)
    fs_func = getattr(current, "filesystem", filesystem) if current else filesystem
    if "filesystem_factory" not in kwargs:
        kwargs["filesystem_factory"] = fs_func
    return _dlt_writer.create_bronze_destination(*args, **kwargs)


__all__ = [
    "BronzeWriteError",
    "BronzeWriter",
    "BufferedBronzeWriter",
    "DltBronzeWriter",
    "create_bronze_destination",
    "create_bronze_resource",
    "filesystem",
    "serialize_record",
]
