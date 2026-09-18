from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from procurement.common.attempts import effective_attempt
from procurement.common.catalog import DEFAULT_SOURCE, SUPPORTED_RESOURCES
from procurement.common.resources import ResourceIdentity
from procurement.models.control import DayManifest, DayStatus, PageManifest, RunManifest, RunStatus
from procurement.models.errors import ErrorRecord
from procurement.ops.models import (
    AttemptDetail,
    AttemptSummary,
    DateDetail,
    DateIngestionStatus,
    DateSummary,
    ErrorSummary,
    ExecutionState,
    OpsOverview,
    PageSummary,
    ResourceHealth,
    ResourceSummary,
    RunDetail,
    RunSummary,
)
from procurement.ops.repositories import ControlRepository, ErrorRepository

VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
MAX_DATE_WINDOW_DAYS = 366


class OpsService:
    def __init__(
        self, control: ControlRepository, errors: ErrorRepository, *,
        sync_status: dict | None = None, stale_after_seconds: float = 600,
    ) -> None:
        self._control = control
        self._errors = errors
        self.sync_status = sync_status
        self.stale_after_seconds = stale_after_seconds
        self._executions = {}

    @staticmethod
    def identity(resource: str, *, source: str = DEFAULT_SOURCE) -> ResourceIdentity:
        if source != DEFAULT_SOURCE or resource not in SUPPORTED_RESOURCES:
            raise ValueError(f"Unsupported ops identity: {source}/{resource}")
        return ResourceIdentity(source=source, resource=resource)

    @staticmethod
    def _duration_seconds(started_at: datetime, completed_at: datetime | None) -> float | None:
        if completed_at is None:
            return None
        return max(0.0, (completed_at - started_at).total_seconds())

    def _execution_state(self, manifest):
        if manifest.status.value != "running":
            return None
        key = (manifest.source, manifest.resource, manifest.run_id)
        if key not in self._executions:
            try:
                self._executions[key] = self._control.get_execution(
                    ResourceIdentity(manifest.source, manifest.resource), manifest.run_id,
                )
            except Exception:  # noqa: BLE001 -- unavailable heartbeat means unknown, never alive
                self._executions[key] = None
        execution = self._executions[key]
        if not execution:
            return ExecutionState.UNKNOWN
        if execution.get("state") in {"finished", "interrupted"}:
            return ExecutionState.INTERRUPTED
        try:
            heartbeat = datetime.fromisoformat(execution["heartbeat_at"])
            age = (datetime.now(UTC) - heartbeat).total_seconds()
            if age < -60 or execution.get("state") != "running":
                return ExecutionState.UNKNOWN
            return ExecutionState.STALE if age > self.stale_after_seconds else ExecutionState.RUNNING
        except (KeyError, ValueError, TypeError):
            return ExecutionState.UNKNOWN

    def _run_summary(self, manifest: RunManifest) -> RunSummary:
        return RunSummary(
            **manifest.model_dump(exclude={"schema_version"}),
            duration_seconds=self._duration_seconds(manifest.started_at, manifest.completed_at),
            execution_state=self._execution_state(manifest),
        )

    def _attempt_summary(self, manifest: DayManifest) -> AttemptSummary:
        return AttemptSummary(
            **manifest.model_dump(exclude={"schema_version"}),
            duration_seconds=self._duration_seconds(manifest.started_at, manifest.completed_at),
            execution_state=self._execution_state(manifest),
        )

    @classmethod
    def _page_summary(cls, manifest: PageManifest) -> PageSummary:
        return PageSummary(
            page_number=manifest.page_number,
            page_size=manifest.page_size,
            status=manifest.status,
            search_items=manifest.search_items,
            bronze_records=manifest.bronze_records,
            error_count=manifest.error_count,
            started_at=manifest.started_at,
            completed_at=manifest.completed_at,
            duration_seconds=cls._duration_seconds(manifest.started_at, manifest.completed_at),
        )

    @staticmethod
    def _error_summary(record: ErrorRecord) -> ErrorSummary:
        return ErrorSummary(**record.model_dump(exclude={"schema_version"}))

    @staticmethod
    def _effective_attempt(attempts: list[DayManifest]) -> DayManifest | None:
        return effective_attempt(attempts)

    @staticmethod
    def _latest_attempt(attempts: list[DayManifest]) -> DayManifest | None:
        if not attempts:
            return None
        return max(attempts, key=lambda item: item.started_at)

    def _date_status(self, attempts: list[DayManifest]) -> DateIngestionStatus:
        if self._effective_attempt(attempts) is not None:
            return DateIngestionStatus.SUCCESS
        latest = self._latest_attempt(attempts)
        if latest is None:
            return DateIngestionStatus.NO_ATTEMPT
        if latest.status is DayStatus.RUNNING:
            return DateIngestionStatus(self._execution_state(latest).value)
        return DateIngestionStatus.FAILED

    @staticmethod
    def _today_vn() -> date:
        return datetime.now(VIETNAM_TZ).date()

    @classmethod
    def _resolve_window(
        cls,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[date, date]:
        end = end_date or (cls._today_vn() - timedelta(days=1))
        start = start_date or (end - timedelta(days=29))
        if start > end:
            raise ValueError("start_date must be before or equal to end_date")
        if (end - start).days + 1 > MAX_DATE_WINDOW_DAYS:
            raise ValueError(f"Date window cannot exceed {MAX_DATE_WINDOW_DAYS} days")
        return start, end

    def list_runs(
        self,
        *,
        source: str = DEFAULT_SOURCE,
        resource: str | None = None,
        status: RunStatus | str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunSummary]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if not 0 <= offset <= 100000:
            raise ValueError("offset must be between 0 and 100000")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("start_date must be before or equal to end_date")

        if status is not None and not isinstance(status, RunStatus):
            try:
                status = RunStatus(status)
            except ValueError as exc:
                raise ValueError(f"Unsupported run status: {status}") from exc

        if resource is not None:
            resources = (resource,)
        else:
            if source != DEFAULT_SOURCE:
                raise ValueError(f"Unsupported ops source: {source}")
            resources = SUPPORTED_RESOURCES

        manifests: list[RunManifest] = []
        for item in resources:
            identity = self.identity(item, source=source)
            manifests.extend(
                self._control.list_runs(
                    identity,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit + offset,
                    status=status,
                )
            )

        if status is not None:
            manifests = [item for item in manifests if item.status is status]
        manifests.sort(key=lambda item: (item.started_at, item.run_id), reverse=True)
        return [self._run_summary(item) for item in manifests[offset:offset + limit]]

    def overview(self, *, source: str = DEFAULT_SOURCE) -> OpsOverview:
        return OpsOverview(
            generated_at=datetime.now(UTC),
            resources=self.list_resources(source=source),
        )

    def list_resources(self, *, source: str = DEFAULT_SOURCE) -> list[ResourceSummary]:
        return [
            self.get_resource_summary(resource, source=source) for resource in SUPPORTED_RESOURCES
        ]

    def get_resource_summary(
        self,
        resource: str,
        *,
        source: str = DEFAULT_SOURCE,
    ) -> ResourceSummary:
        identity = self.identity(resource, source=source)
        attempts = self._control.list_attempts(identity, limit=None)
        if not attempts:
            return ResourceSummary(
                source=source,
                resource=resource,
                health=ResourceHealth.NO_DATA,
            )

        by_date: dict[date, list[DayManifest]] = defaultdict(list)
        for attempt in attempts:
            by_date[attempt.source_date].append(attempt)

        latest_source_date = max(by_date)
        successful_dates = [
            source_date
            for source_date, items in by_date.items()
            if self._effective_attempt(items) is not None
        ]
        latest_success_date = max(successful_dates) if successful_dates else None
        unresolved_failed_dates = sum(
            1
            for items in by_date.values()
            if self._effective_attempt(items) is None
            and self._latest_attempt(items) is not None
            and self._latest_attempt(items).status is DayStatus.FAILED
        )
        freshness_days = (
            None
            if latest_success_date is None
            else max(0, (self._today_vn() - latest_success_date).days)
        )

        latest_status = self._date_status(by_date[latest_source_date])
        if latest_success_date is None or latest_status is DateIngestionStatus.FAILED:
            health = ResourceHealth.FAILED
        elif (
            unresolved_failed_dates > 0 or (freshness_days is not None and freshness_days > 1)
            or latest_status in {
                DateIngestionStatus.STALE, DateIngestionStatus.UNKNOWN,
                DateIngestionStatus.INTERRUPTED,
            }
        ):
            health = ResourceHealth.DEGRADED
        else:
            health = ResourceHealth.HEALTHY

        return ResourceSummary(
            source=source,
            resource=resource,
            health=health,
            latest_source_date=latest_source_date,
            latest_success_source_date=latest_success_date,
            freshness_days=freshness_days,
            unresolved_failed_dates=unresolved_failed_dates,
            total_attempts=len(attempts),
        )

    def _attempts_for_window(
        self,
        identity: ResourceIdentity,
        *,
        start_date: date,
        end_date: date,
    ) -> list[DayManifest]:
        return self._control.list_attempts(
            identity, start_date=start_date, end_date=end_date, limit=None,
        )

    def list_dates(
        self,
        resource: str,
        *,
        source: str = DEFAULT_SOURCE,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[DateSummary]:
        identity = self.identity(resource, source=source)
        start, end = self._resolve_window(start_date, end_date)
        attempts = self._attempts_for_window(identity, start_date=start, end_date=end)

        by_date: dict[date, list[DayManifest]] = defaultdict(list)
        for attempt in attempts:
            by_date[attempt.source_date].append(attempt)

        items: list[DateSummary] = []
        cursor = end
        while cursor >= start:
            date_attempts = by_date.get(cursor, [])
            effective = self._effective_attempt(date_attempts)
            latest = self._latest_attempt(date_attempts)
            status = self._date_status(date_attempts)
            projected = effective or latest

            items.append(
                DateSummary(
                    source=source,
                    resource=resource,
                    source_date=cursor,
                    status=status,
                    attempt_count=len(date_attempts),
                    effective_run_id=None if effective is None else effective.run_id,
                    latest_run_id=None if latest is None else latest.run_id,
                    latest_attempt_status=None if latest is None else latest.status,
                    bronze_records=0 if projected is None else projected.bronze_records,
                    error_count=0 if projected is None else projected.error_count,
                    last_attempt_at=None if latest is None else latest.started_at,
                )
            )
            cursor -= timedelta(days=1)
        return items

    def get_date(
        self,
        resource: str,
        source_date: date,
        *,
        source: str = DEFAULT_SOURCE,
    ) -> DateDetail:
        identity = self.identity(resource, source=source)
        attempts = self._attempts_for_window(
            identity,
            start_date=source_date,
            end_date=source_date,
        )
        effective = self._effective_attempt(attempts)
        return DateDetail(
            source=source,
            resource=resource,
            source_date=source_date,
            status=self._date_status(attempts),
            effective_run_id=None if effective is None else effective.run_id,
            attempts=[self._attempt_summary(item) for item in attempts],
        )

    def _find_run(
        self,
        run_id: str,
        *,
        source: str = DEFAULT_SOURCE,
    ) -> tuple[ResourceIdentity, RunManifest] | None:
        for resource in SUPPORTED_RESOURCES:
            identity = self.identity(resource, source=source)
            manifest = self._control.get_run(identity, run_id)
            if manifest is not None:
                return identity, manifest
        return None

    def get_run(self, run_id: str, *, source: str = DEFAULT_SOURCE) -> RunDetail | None:
        found = self._find_run(run_id, source=source)
        if found is None:
            return None
        identity, manifest = found
        attempts = self._control.list_attempts(identity, run_id=run_id, limit=None)
        return RunDetail(
            run=self._run_summary(manifest),
            attempts=[self._attempt_summary(item) for item in attempts],
        )

    def get_attempt(
        self,
        run_id: str,
        source_date: date,
        *,
        source: str = DEFAULT_SOURCE,
    ) -> AttemptDetail | None:
        found = self._find_run(run_id, source=source)
        if found is None:
            return None
        identity, _ = found
        attempt = self._control.get_attempt(
            identity,
            run_id=run_id,
            source_date=source_date,
        )
        if attempt is None:
            return None
        pages = self._control.list_pages(
            identity,
            run_id=run_id,
            source_date=source_date,
        )
        errors = self._errors.list(
            identity,
            run_id=run_id,
            source_date=source_date,
            limit=1000,
        )
        return AttemptDetail(
            attempt=self._attempt_summary(attempt),
            pages=[self._page_summary(item) for item in pages],
            errors=[self._error_summary(item) for item in errors],
        )

    def list_errors(
        self,
        *,
        source: str = DEFAULT_SOURCE,
        resource: str | None = None,
        source_date: date | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        error_type: str | None = None,
        limit: int = 200,
    ) -> list[ErrorSummary]:
        resources: tuple[str, ...]
        if resource is not None:
            resources = (resource,)
        elif run_id is not None:
            found = self._find_run(run_id, source=source)
            if found is None:
                return []
            resources = (found[0].resource,)
        else:
            if source != DEFAULT_SOURCE:
                raise ValueError(f"Unsupported ops source: {source}")
            resources = SUPPORTED_RESOURCES

        records: list[ErrorRecord] = []
        for item in resources:
            identity = self.identity(item, source=source)
            records.extend(
                self._errors.list(
                    identity,
                    source_date=source_date,
                    run_id=run_id,
                    stage=stage,
                    error_type=error_type,
                    limit=limit,
                )
            )
        records.sort(key=lambda item: item.occurred_at, reverse=True)
        return [self._error_summary(item) for item in records[:limit]]
