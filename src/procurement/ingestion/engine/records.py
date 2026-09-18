from datetime import date
from typing import Any

from procurement.ingestion.engine.metadata import calculate_content_hash, utc_now
from procurement.ingestion.engine.models import BronzeItem
from procurement.models.bronze import BronzeRecord


def build_bronze_item(
    *,
    table: str,
    source_id: str,
    source_version: str | None,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    """One canonical lineage envelope; identity/version semantics belong to the adapter."""
    return BronzeItem(
        table=table,
        record=BronzeRecord(
            source_id=source_id,
            source_version=source_version,
            run_id=run_id,
            source_date=source_date,
            ingested_at=utc_now(),
            content_hash=calculate_content_hash(payload),
            payload=payload,
        ),
    )
