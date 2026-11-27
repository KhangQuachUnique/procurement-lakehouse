"""Unified work planner for Daily Incremental and Monthly Backfill modes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

from procurement_spark.control.manifest import BronzeDayManifest
from procurement_spark.control.receipts import SilverReceipt


@dataclass(frozen=True)
class PlannedBatch:
    """One deterministic Spark processing unit (1 day for daily, 1 month for backfill)."""

    batch_id: str
    label: str  # e.g. "2025-01" or "2025-01-15"
    resource: str
    transform_version: str
    days: tuple[BronzeDayManifest, ...]

    @property
    def start_date(self) -> date:
        return self.days[0].source_date

    @property
    def end_date(self) -> date:
        return self.days[-1].source_date

    @property
    def s3a_uris(self) -> list[str]:
        return [f.s3a_uri for d in self.days for f in d.files]

    @property
    def uris_by_table(self) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for d in self.days:
            for f in d.files:
                grouped.setdefault(f.table_name, []).append(f.s3a_uri)
        return grouped

    @property
    def total_expected_records(self) -> int:
        return sum(d.expected_records for d in self.days)


def _make_batch_id(
    resource: str,
    label: str,
    transform_version: str,
    days: list[BronzeDayManifest],
) -> str:
    fingerprint = "|".join(
        f"{d.source_date.isoformat()}:{d.commit_id}" for d in days
    )
    raw = f"{resource}|{label}|{transform_version}|{fingerprint}"
    short_hash = hashlib.sha256(raw.encode()).hexdigest()[:12]
    safe_label = label.replace("-", "")
    return f"{resource}_{safe_label}_{short_hash}"


def plan_batches(
    *,
    resource: str,
    transform_version: str,
    manifests: dict[date, BronzeDayManifest],
    published_receipts: dict[date, SilverReceipt],
    start_date: date,
    end_date: date,
    mode: Literal["daily", "backfill"] = "backfill",
    force: bool = False,
) -> tuple[list[PlannedBatch], list[date]]:
    """Determine pending days and group them into deterministic processing batches.

    Returns:
        (batches, missing_bronze_dates)
    """
    missing_dates: list[date] = []
    pending_days: list[BronzeDayManifest] = []

    cursor = start_date
    while cursor <= end_date:
        manifest = manifests.get(cursor)
        if manifest is None:
            missing_dates.append(cursor)
            cursor += timedelta(days=1)
            continue

        existing = published_receipts.get(cursor)
        already_current = (
            not force
            and existing is not None
            and existing.status == "PUBLISHED"
            and existing.input_commit_id == manifest.commit_id
            and existing.transform_version == transform_version
        )
        if not already_current:
            pending_days.append(manifest)
        cursor += timedelta(days=1)

    if not pending_days:
        return [], missing_dates

    grouped: dict[str, list[BronzeDayManifest]] = {}
    for d in pending_days:
        key = d.source_date.strftime("%Y-%m") if mode == "backfill" else d.source_date.isoformat()
        grouped.setdefault(key, []).append(d)

    batches = [
        PlannedBatch(
            batch_id=_make_batch_id(resource, label, transform_version, group_days),
            label=label,
            resource=resource,
            transform_version=transform_version,
            days=tuple(group_days),
        )
        for label, group_days in sorted(grouped.items())
    ]
    return batches, missing_dates
