"""Build a source envelope and dispatch through the registered transformer."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.registry import require_transformer
from procurement.silver.domain.transformers.base import canonical_json


def transform(
    namespace: str,
    partition: dict[str, Any],
    file: dict[str, Any],
    ordinal: int,
    record: dict[str, Any],
) -> dict[str, Any]:
    """Build RawRecordEnvelope and transform via registered transformer."""
    transformer = require_transformer(file["table"])
    raw = record["payload"]
    payload = json.loads(raw, parse_float=Decimal) if isinstance(raw, str) else raw
    observed_at = record["ingested_at"]
    if isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at)
    if observed_at.tzinfo is None:
        raise ValueError("Observation time must have a timezone")
    envelope = RawRecordEnvelope(
        namespace=namespace,
        table_name=file["table"],
        resource=partition["resource"],
        source_date=partition["source_date"],
        run_id=record["run_id"],
        row_ordinal=ordinal,
        file_key=file["key"],
        file_sha256=file["sha256"],
        source_id=record["source_id"],
        source_version=record.get("source_version"),
        content_hash=record["content_hash"],
        ingested_at=observed_at,
        payload=payload,
    )
    result = transformer.transform(envelope)
    result.observation["payload_json"] = raw if isinstance(raw, str) else canonical_json(raw)
    return result.to_dict()


__all__ = ["transform"]
