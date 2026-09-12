import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


class IncompatibleCheckpointError(RuntimeError):
    """Raised when persisted control state does not match the current query/page."""


def _hash_json(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def calculate_query_fingerprint(query_definition: Mapping[str, Any]) -> str:
    return _hash_json(query_definition)


def calculate_page_fingerprint(search_items: Sequence[Mapping[str, Any]]) -> str:
    """Hash ordered search identities so a resumed page cannot silently drift.

    Most Mua Sam Cong search rows expose ``id``. For a malformed/unknown resource,
    hashing the complete item is safer than pretending the page identity is stable.
    """
    identities: list[Any] = []
    for item in search_items:
        if item.get("id") is not None:
            identities.append(item["id"])
        else:
            identities.append(dict(item))
    return _hash_json(identities)


def ensure_compatible(checkpoint: Mapping[str, Any], query_fingerprint: str) -> None:
    existing = checkpoint.get("query_fingerprint")
    if existing != query_fingerprint:
        raise IncompatibleCheckpointError(
            f"Checkpoint fingerprint {existing!r} does not match current query"
        )


def ensure_page_compatible(checkpoint: Mapping[str, Any], page_fingerprint: str) -> None:
    existing = checkpoint.get("search_page_fingerprint")
    if existing is not None and existing != page_fingerprint:
        raise IncompatibleCheckpointError(
            "Checkpoint search page no longer matches the source result ordering"
        )
