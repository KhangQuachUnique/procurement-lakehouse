import s3fs

from procurement.common.settings import settings


def create_s3_filesystem() -> s3fs.S3FileSystem:
    """Create an S3 filesystem connected to the object store."""

    return s3fs.S3FileSystem(
        # Manifests may be published by another worker or container between reads.
        use_listings_cache=False,
        key=settings.OBJECT_STORAGE_ACCESS_KEY,
        secret=settings.OBJECT_STORAGE_SECRET_KEY,
        client_kwargs={
            "endpoint_url": settings.OBJECT_STORAGE_ENDPOINT,
            "region_name": "us-east-1",
        },
    )
