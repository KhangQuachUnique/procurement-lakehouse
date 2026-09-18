"""Complete manifest-based coverage used by execution planning and data readers."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from procurement.common.attempts import effective_attempt
from procurement.common.resources import ResourceIdentity
from procurement.models.control import DayManifest, DayStatus, RunStatus
from procurement.storage.control import list_day_manifests, list_run_manifests


@dataclass(frozen=True)
class CoverageDay:
    source_date: date
    effective: DayManifest | None
    latest: DayManifest | None
    active_run_ids: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.effective is not None:
            return "success"
        if self.active_run_ids:
            return "running"
        return "no_attempt" if self.latest is None else "failed"


def read_coverage(fs, identity: ResourceIdentity, start: date, end: date) -> list[CoverageDay]:
    if start > end:
        raise ValueError("start must be before or equal to end")
    by_date: dict[date, list[DayManifest]] = defaultdict(list)
    active: dict[date, list[str]] = defaultdict(list)
    runs = list_run_manifests(fs, identity, start_date=start, end_date=end)
    for run in runs:
        attempts = list_day_manifests(fs, identity, run_id=run.run_id)
        for day in attempts:
            if start <= day.source_date <= end:
                by_date[day.source_date].append(day)
        if run.status is RunStatus.RUNNING:
            # Include dates not reached yet; another live range job may still get to them.
            cursor = max(start, run.start_date)
            completed = {day.source_date for day in attempts if day.status is not DayStatus.RUNNING}
            while cursor <= min(end, run.end_date):
                if cursor not in completed:
                    active[cursor].append(run.run_id)
                cursor += timedelta(days=1)
    result = []
    cursor = start
    while cursor <= end:
        attempts = by_date[cursor]
        result.append(CoverageDay(
            cursor, effective_attempt(attempts),
            max(attempts, key=lambda item: item.started_at, default=None),
            tuple(active[cursor]),
        ))
        cursor += timedelta(days=1)
    return result
