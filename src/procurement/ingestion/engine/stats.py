from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PageStats:
    """Resource-neutral counters persisted with each page checkpoint."""

    search_items: int = 0
    record_counts: Counter[str] = field(default_factory=Counter)
    error_counts: Counter[str] = field(default_factory=Counter)

    def record(self, kind: str) -> None:
        self.record_counts[kind] += 1

    def error(self, kind: str) -> None:
        self.error_counts[kind] += 1

    def merge(self, other: "PageStats") -> None:
        self.search_items += other.search_items
        self.record_counts.update(other.record_counts)
        self.error_counts.update(other.error_counts)

    def to_metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "record_counts": dict(self.record_counts),
            "error_counts": dict(self.error_counts),
            "total_records": self.total_records,
            "total_errors": self.total_errors,
        }
        # Preserve existing flat fields for current lake consumers.
        metadata.update({f"{key}_records": value for key, value in self.record_counts.items()})
        metadata.update({f"{key}_errors": value for key, value in self.error_counts.items()})
        return metadata

    @classmethod
    def from_metadata(cls, metadata: Mapping[str, Any]) -> "PageStats":
        stats = cls(search_items=int(metadata.get("search_items", 0)))
        record_counts = metadata.get("record_counts")
        error_counts = metadata.get("error_counts")
        if isinstance(record_counts, Mapping):
            stats.record_counts.update({str(k): int(v) for k, v in record_counts.items()})
        else:
            stats.record_counts.update(cls._legacy_counts(metadata, "records"))
        if isinstance(error_counts, Mapping):
            stats.error_counts.update({str(k): int(v) for k, v in error_counts.items()})
        else:
            stats.error_counts.update(cls._legacy_counts(metadata, "errors"))
        return stats

    @staticmethod
    def _legacy_counts(metadata: Mapping[str, Any], suffix: str) -> dict[str, int]:
        marker = f"_{suffix}"
        return {
            key.removesuffix(marker): int(value)
            for key, value in metadata.items()
            if key.endswith(marker) and key != f"total_{suffix}"
        }

    @property
    def total_records(self) -> int:
        return sum(self.record_counts.values())

    @property
    def total_errors(self) -> int:
        return sum(self.error_counts.values())
