"""Repair-local coverage guard: refresh new/active runs, reuse terminal run headers."""

from procurement.common.attempts import effective_attempt
from procurement.common.settings import settings
from procurement.ingestion.coverage import CoverageDay
from procurement.models.control import DayStatus, RunStatus
from procurement.storage.control import read_day_manifest, read_run_manifest


class CoverageGuard:
    """Only run headers are cached. Day markers and run-directory listings stay live.

    Completed run headers are immutable in the append-only execution model. New
    and RUNNING headers are read every check, including this repair's own run.
    This avoids rereading years of unrelated completed run headers per record-day.
    """

    def __init__(self, fs, identity):
        self.fs, self.identity = fs, identity
        self.headers = {}

    def read(self, day):
        prefix = (f"{settings.OBJECT_STORAGE_BUCKET}/_control/"
                  f"{self.identity.source}/{self.identity.resource}")
        entries = self.fs.ls(prefix, detail=False)
        run_ids = {str(key).rstrip("/").rsplit("/", 1)[-1].removeprefix("run_id=")
                   for key in entries if str(key).rstrip("/").rsplit("/", 1)[-1].startswith("run_id=")}
        for removed in set(self.headers) - run_ids:
            del self.headers[removed]
        attempts, active = [], []
        for run_id in sorted(run_ids):
            run = self.headers.get(run_id)
            if run is None or run.status is RunStatus.RUNNING:
                run = read_run_manifest(self.fs, self.identity, run_id)
                if run is None:
                    raise ValueError(f"Unconfirmed run header: {run_id}")
                self.headers[run_id] = run
            if not run.start_date <= day <= run.end_date:
                continue
            attempt = read_day_manifest(self.fs, self.identity, run_id, day)
            if attempt:
                attempts.append(attempt)
            if run.status is RunStatus.RUNNING and (attempt is None or attempt.status is DayStatus.RUNNING):
                active.append(run_id)
        return CoverageDay(day, effective_attempt(attempts),
                           max(attempts, key=lambda item: item.started_at, default=None), tuple(active))
