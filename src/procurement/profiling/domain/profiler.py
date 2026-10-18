"""Domain engine for statistical calculations, candidate keys, and drift comparison."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import date
from typing import Any

from procurement.profiling.domain.models import (
    FieldMetrics,
    NumericStats,
    TableProfile,
    TextStats,
    WindowDrift,
)
from procurement.profiling.domain.walker import (
    SENTINEL_VALUES,
    NestedJsonWalker,
    ValueState,
)


class FieldAccumulator:
    """Online accumulator for statistics of a single field path."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.total_records = 0
        self.present_records = 0
        self.null_records = 0
        self.empty_records = 0
        self.populated_records = 0
        self.occurrences = 0
        self.populated_occurrences = 0

        self.type_counts: Counter[str] = Counter()
        self.value_counts: Counter[str] = Counter()
        
        # Numeric online stats (Welford's algorithm)
        self.numeric_count = 0
        self.numeric_min: float | None = None
        self.numeric_max: float | None = None
        self.numeric_m1 = 0.0
        self.numeric_m2 = 0.0
        self.numeric_zeros = 0
        self.numeric_negatives = 0
        self.numeric_samples: list[float] = []

        # String stats
        self.text_count = 0
        self.text_min_len: int | None = None
        self.text_max_len: int | None = None
        self.text_sum_len = 0
        self.text_empty = 0
        self.text_whitespace_padded = 0
        self.case_summary: Counter[str] = Counter()

        # Anomalies
        self.anomalies: set[str] = set()

    def update_record(self, observed_items: list[tuple[Any, ValueState, str]] | None) -> None:
        """Update stats with the observations from a single record."""
        self.total_records += 1

        if not observed_items:
            return

        self.present_records += 1
        record_has_populated = False
        record_has_null = False
        record_has_empty = False

        for val, state, type_name in observed_items:
            self.occurrences += 1
            self.type_counts[type_name] += 1

            if state == ValueState.POPULATED:
                self.populated_occurrences += 1
                record_has_populated = True
                self._observe_populated_value(val, type_name)
            elif state == ValueState.NULL:
                record_has_null = True
            elif state == ValueState.EMPTY:
                record_has_empty = True
                if isinstance(val, str):
                    self.text_empty += 1

        if record_has_populated:
            self.populated_records += 1
        elif record_has_empty:
            self.empty_records += 1
        elif record_has_null:
            self.null_records += 1

    def _observe_populated_value(self, val: Any, type_name: str) -> None:
        val_str = str(val)
        if len(val_str) <= 256:
            self.value_counts[val_str] += 1
            if val_str.lower().strip() in SENTINEL_VALUES:
                self.anomalies.add(f"sentinel_value_detected: '{val_str}'")

        # Numeric stats
        num_val: float | None = None
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            num_val = float(val)
        elif isinstance(val, str):
            try:
                parsed = float(val.strip().replace(",", ""))
                if math.isfinite(parsed):
                    num_val = parsed
            except ValueError:
                pass

        if num_val is not None:
            self.numeric_count += 1
            if self.numeric_min is None or num_val < self.numeric_min:
                self.numeric_min = num_val
            if self.numeric_max is None or num_val > self.numeric_max:
                self.numeric_max = num_val
            if num_val == 0.0:
                self.numeric_zeros += 1
            elif num_val < 0.0:
                self.numeric_negatives += 1

            delta = num_val - self.numeric_m1
            self.numeric_m1 += delta / self.numeric_count
            delta2 = num_val - self.numeric_m1
            self.numeric_m2 += delta * delta2

            if len(self.numeric_samples) < 2000:
                self.numeric_samples.append(num_val)

        # Text stats
        if isinstance(val, str):
            self.text_count += 1
            v_len = len(val)
            if self.text_min_len is None or v_len < self.text_min_len:
                self.text_min_len = v_len
            if self.text_max_len is None or v_len > self.text_max_len:
                self.text_max_len = v_len
            self.text_sum_len += v_len

            if val.startswith(" ") or val.endswith(" "):
                self.text_whitespace_padded += 1
                self.anomalies.add("whitespace_padding")

            s = val.strip()
            if s.isupper():
                self.case_summary["upper"] += 1
            elif s.islower():
                self.case_summary["lower"] += 1
            elif s.isnumeric():
                self.case_summary["numeric"] += 1
            else:
                self.case_summary["mixed"] += 1

    def finalize(self) -> FieldMetrics:
        """Produce final FieldMetrics for this path."""
        missing = self.total_records - self.present_records
        total = max(self.total_records, 1)

        null_ratio = self.null_records / total
        empty_ratio = self.empty_records / total
        missing_ratio = missing / total
        populated_ratio = self.populated_records / total

        if populated_ratio > 0.99:
            tier = "Full"
        elif populated_ratio >= 0.95:
            tier = "Mostly full"
        elif populated_ratio >= 0.80:
            tier = "Gaps present"
        else:
            tier = "Sparse"

        distinct_count = len(self.value_counts)
        cardinality_ratio = distinct_count / total if total > 0 else 0.0

        top_values = self.value_counts.most_common(10)
        bottom_values = sorted(self.value_counts.items(), key=lambda x: x[1])[:5]

        num_stats: NumericStats | None = None
        if self.numeric_count > 0:
            mean = self.numeric_m1
            std = math.sqrt(self.numeric_m2 / self.numeric_count) if self.numeric_count > 1 else 0.0
            p1 = p5 = p25 = p50 = p75 = p95 = p99 = None
            if self.numeric_samples:
                s = sorted(self.numeric_samples)
                n = len(s)
                p1 = s[int(0.01 * n)]
                p5 = s[int(0.05 * n)]
                p25 = s[int(0.25 * n)]
                p50 = s[int(0.50 * n)]
                p75 = s[min(int(0.75 * n), n - 1)]
                p95 = s[min(int(0.95 * n), n - 1)]
                p99 = s[min(int(0.99 * n), n - 1)]

            num_stats = NumericStats(
                min=self.numeric_min,
                max=self.numeric_max,
                mean=round(mean, 4),
                std=round(std, 4),
                p1=p1,
                p5=p5,
                p25=p25,
                p50=p50,
                p75=p75,
                p95=p95,
                p99=p99,
                zero_count=self.numeric_zeros,
                negative_count=self.numeric_negatives,
            )

        text_stats: TextStats | None = None
        if self.text_count > 0:
            avg_len = round(self.text_sum_len / self.text_count, 2)
            text_stats = TextStats(
                min_length=self.text_min_len or 0,
                max_length=self.text_max_len or 0,
                avg_length=avg_len,
                empty_strings=self.text_empty,
                whitespace_padded=self.text_whitespace_padded,
                case_summary=dict(self.case_summary),
            )

        role = "Attribute"
        clean_path = self.path.lower()
        if "id" in clean_path or "code" in clean_path or "no" in clean_path:
            role = "Key"
        elif "date" in clean_path or "time" in clean_path:
            role = "Timestamp"
        elif num_stats and (num_stats.mean != 0 or num_stats.max != 1):
            role = "Measure"
        elif set(self.type_counts.keys()) == {"bool"}:
            role = "Flag"
        elif any(k in clean_path for k in ("name", "desc", "content", "reason")):
            role = "Free-text"
        elif any(t in self.type_counts for t in ("object", "array")):
            role = "Nested"

        is_candidate_key = (
            distinct_count == total 
            and populated_ratio == 1.0 
            and self.null_records == 0 
            and self.empty_records == 0
        )

        return FieldMetrics(
            path=self.path,
            total_records=self.total_records,
            present_records=self.present_records,
            null_records=self.null_records,
            empty_records=self.empty_records,
            populated_records=self.populated_records,
            occurrences=self.occurrences,
            populated_occurrences=self.populated_occurrences,
            null_ratio=round(null_ratio, 4),
            empty_ratio=round(empty_ratio, 4),
            missing_ratio=round(missing_ratio, 4),
            populated_ratio=round(populated_ratio, 4),
            completeness_tier=tier,
            type_counts=dict(self.type_counts),
            distinct_count=distinct_count,
            cardinality_ratio=round(cardinality_ratio, 4),
            top_values=top_values,
            bottom_values=bottom_values,
            numeric_stats=num_stats,
            text_stats=text_stats,
            role=role,
            is_candidate_key=is_candidate_key,
            anomalies=sorted(self.anomalies),
        )


class TableProfilerEngine:
    """Core domain profiler aggregating field metrics, keys, and anomalies."""

    def __init__(self, table_name: str, resource: str) -> None:
        self.table_name = table_name
        self.resource = resource
        self.accumulators: dict[str, FieldAccumulator] = {}
        self.total_profiled = 0
        self.dates_seen: set[str] = set()

        self.composite_key_candidates = [
            ["$.source_id", "$.source_version"],
            ["$.id", "$.version"],
            ["$.notifyNo", "$.notifyVersion"],
            ["$.bidNo", "$.bidVersion"],
        ]
        self.composite_seen: dict[str, set[tuple[str, ...]]] = defaultdict(set)

    def feed_record(self, record: dict[str, Any]) -> None:
        """Feed a single decoded record into the engine."""
        self.total_profiled += 1

        if "source_date" in record and record["source_date"]:
            self.dates_seen.add(str(record["source_date"]))

        flattened = NestedJsonWalker.flatten_record(record)

        for path in flattened:
            if path not in self.accumulators:
                acc = FieldAccumulator(path)
                if self.total_profiled > 1:
                    for _ in range(self.total_profiled - 1):
                        acc.update_record(None)
                self.accumulators[path] = acc

        for path, acc in self.accumulators.items():
            items = flattened.get(path)
            acc.update_record(items)

        for comp in self.composite_key_candidates:
            key_name = "+".join(comp)
            vals = []
            valid = True
            for k in comp:
                items = flattened.get(k)
                if not items or items[0][1] != ValueState.POPULATED:
                    valid = False
                    break
                vals.append(str(items[0][0]))
            if valid:
                self.composite_seen[key_name].add(tuple(vals))

    def feed_records(self, records: Iterable[dict[str, Any]]) -> None:
        for r in records:
            self.feed_record(r)

    def finalize(
        self,
        *,
        total_active_records: int = 0,
        total_active_files: int = 0,
    ) -> TableProfile:
        """Finalize and build TableProfile."""
        fields: dict[str, FieldMetrics] = {}
        candidate_keys: list[str] = []

        for path, acc in self.accumulators.items():
            metrics = acc.finalize()
            fields[path] = metrics
            if metrics.is_candidate_key:
                candidate_keys.append(path)

        candidate_composites: list[list[str]] = []
        for comp in self.composite_key_candidates:
            key_name = "+".join(comp)
            seen_set = self.composite_seen.get(key_name)
            if seen_set and len(seen_set) == self.total_profiled and self.total_profiled > 0:
                candidate_composites.append(comp)

        min_d = min(self.dates_seen) if self.dates_seen else ""
        max_d = max(self.dates_seen) if self.dates_seen else ""

        anomalies_list: list[dict[str, Any]] = []
        for p, m in fields.items():
            if m.anomalies:
                anomalies_list.append({
                    "path": p,
                    "issues": m.anomalies,
                    "affected_records": m.total_records - m.populated_records if m.null_records > 0 else m.total_records,
                })

        return TableProfile(
            table_name=self.table_name,
            resource=self.resource,
            total_active_records=total_active_records or self.total_profiled,
            total_active_files=total_active_files,
            profiled_records=self.total_profiled,
            date_range=(min_d, max_d),
            fields=fields,
            candidate_keys=candidate_keys,
            candidate_composite_keys=candidate_composites,
            anomalies=anomalies_list,
        )

    @classmethod
    def compare_windows(cls, profile_a: TableProfile, profile_b: TableProfile) -> WindowDrift:
        """Compare two profiles to detect schema and metric drift."""
        w_a = f"{profile_a.date_range[0]}..{profile_a.date_range[1]}"
        w_b = f"{profile_b.date_range[0]}..{profile_b.date_range[1]}"

        fields_a = set(profile_a.fields.keys())
        fields_b = set(profile_b.fields.keys())

        new_fields = sorted(fields_b - fields_a)
        dropped_fields = sorted(fields_a - fields_b)
        common_fields = fields_a & fields_b

        type_mutations: dict[str, dict[str, Any]] = {}
        null_shifts: dict[str, dict[str, float]] = {}
        cardinality_shifts: dict[str, dict[str, Any]] = {}

        for p in common_fields:
            m_a = profile_a.fields[p]
            m_b = profile_b.fields[p]

            types_a = set(m_a.type_counts.keys())
            types_b = set(m_b.type_counts.keys())
            if types_a != types_b:
                type_mutations[p] = {"window_a": dict(m_a.type_counts), "window_b": dict(m_b.type_counts)}

            if abs(m_a.null_ratio - m_b.null_ratio) >= 0.10:
                null_shifts[p] = {"window_a_null_ratio": m_a.null_ratio, "window_b_null_ratio": m_b.null_ratio}

            if abs(m_a.cardinality_ratio - m_b.cardinality_ratio) >= 0.20:
                cardinality_shifts[p] = {
                    "window_a_distinct": m_a.distinct_count,
                    "window_b_distinct": m_b.distinct_count,
                    "window_a_ratio": m_a.cardinality_ratio,
                    "window_b_ratio": m_b.cardinality_ratio,
                }

        return WindowDrift(
            window_a=w_a,
            window_b=w_b,
            new_fields=new_fields,
            dropped_fields=dropped_fields,
            type_mutations=type_mutations,
            null_ratio_shifts=null_shifts,
            cardinality_shifts=cardinality_shifts,
        )
