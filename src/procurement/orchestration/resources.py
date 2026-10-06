from dagster import resource

from procurement.storage.object_store import create_s3_filesystem


@resource
def object_storage(_context):
    """Use the same storage settings as the existing ingestion application."""
    return create_s3_filesystem()
