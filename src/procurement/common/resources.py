from dataclasses import dataclass


@dataclass(frozen=True)
class ResourceIdentity:
    """Stable source/resource namespace shared by ingestion and storage."""

    source: str
    resource: str

