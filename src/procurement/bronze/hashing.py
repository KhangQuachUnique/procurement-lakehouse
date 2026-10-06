"""Hashing and canonical serialization rules for Bronze content."""

import hashlib
import json
from typing import Any


def calculate_content_hash(payload: Any) -> str:
    """Calculate deterministic SHA256 hex digest for JSON payload.

    Uses canonical JSON: sorted keys, no whitespace separators, UTF-8 encoded.
    """
    canonical_json = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
