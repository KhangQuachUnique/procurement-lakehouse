"""Domain models for profiling dataset schema, metrics, and drift."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class NumericStats:
    """Summary statistics for numeric values."""
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    std: float | None = None
    p1: float | None = None
    p5: float | None = None
    p25: float | None = None
    p50: float | None = None  # median
    p75: float | None = None
    p95: float | None = None
    p99: float | None = None
    zero_count: int = 0
    negative_count: int = 0


@dataclass
class TextStats:
    """Summary statistics for string/text values."""
    min_length: int = 0
    max_length: int = 0
    avg_length: float = 0.0
    empty_strings: int = 0
    whitespace_padded: int = 0
    case_summary: dict[str, int] = field(default_factory=dict)  # upper, lower, mixed, numeric


@dataclass
class FieldMetrics:
    """Evidence metrics for a single flattened field path."""
    path: str
    total_records: int = 0
    present_records: int = 0
    null_records: int = 0
    empty_records: int = 0
    populated_records: int = 0
    occurrences: int = 0
    populated_occurrences: int = 0
    
    # Ratios
    null_ratio: float = 0.0
    empty_ratio: float = 0.0
    missing_ratio: float = 0.0
    populated_ratio: float = 0.0
    completeness_tier: str = "Sparse"  # Full (>99%), Mostly full (95-99%), Gaps (80-95%), Sparse (<80%)
    
    # Types and values
    type_counts: dict[str, int] = field(default_factory=dict)
    distinct_count: int = 0
    cardinality_ratio: float = 0.0
    top_values: list[tuple[str, int]] = field(default_factory=list)
    bottom_values: list[tuple[str, int]] = field(default_factory=list)
    
    # Detailed stats
    numeric_stats: NumericStats | None = None
    text_stats: TextStats | None = None
    
    # Structural classification
    role: str = "Attribute"  # Key, Attribute, Measure, Timestamp, Free-text, Flag, Nested
    is_candidate_key: bool = False
    anomalies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TableProfile:
    """Complete profile for a dataset/table."""
    table_name: str
    resource: str
    total_active_records: int = 0
    total_active_files: int = 0
    profiled_records: int = 0
    date_range: tuple[str, str] = ("", "")
    fields: dict[str, FieldMetrics] = field(default_factory=dict)
    candidate_keys: list[str] = field(default_factory=list)
    candidate_composite_keys: list[list[str]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    conditional_patterns: list[dict[str, Any]] = field(default_factory=list)
    anomalies: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WindowDrift:
    """Drift metrics between two ingestion windows."""
    window_a: str
    window_b: str
    new_fields: list[str] = field(default_factory=list)
    dropped_fields: list[str] = field(default_factory=list)
    type_mutations: dict[str, dict[str, Any]] = field(default_factory=dict)
    null_ratio_shifts: dict[str, dict[str, float]] = field(default_factory=dict)
    cardinality_shifts: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
