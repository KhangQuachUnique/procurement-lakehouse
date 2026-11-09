"""Session reader for one explicitly selected compact Gold manifest generation."""

from types import SimpleNamespace

from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
from procurement.storage.iceberg import identifier


def ensure_gold_views(db, catalog, *, target_namespace):
    target_namespace = identifier(target_namespace)
    if not target_namespace.startswith("gold_v2_"):
        raise ValueError("An explicit compact Gold v2 target is required")
    receipt = IcebergPublisher(db, catalog).ensure_views(
        SimpleNamespace(target_namespace=target_namespace), view_schema="gold"
    )
    return sorted(receipt["tables"])
