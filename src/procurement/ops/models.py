from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from procurement.models.control import DayStatus, PageStatus, RunStatus


class _OpsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ResourceHealth(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    NO_DATA = "no_data"


class DateIngestionStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    MISSING = "missing"


class RunSummary(_OpsModel):
    run_id: str
    source: str
    resource: str
    start_date: date
    end_date: date
    status: RunStatus
    total_dates: int
    success_dates: int
    failed_dates: int
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None


class AttemptSummary(_OpsModel):
    run_id: str
    source: str
    resource: str
    source_date: date
    status: DayStatus
    expected_pages: int | None = None
    completed_pages: int
    search_items: int
    bronze_records: int
    error_count: int
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None


class PageSummary(_OpsModel):
    page_number: int
    page_size: int
    status: PageStatus
    search_items: int
    bronze_records: int
    error_count: int
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None


class ErrorSummary(_OpsModel):
    error_id: str
    run_id: str
    source: str
    resource: str
    source_date: date
    page_number: int | None = None
    stage: str
    source_id: str | None = None
    error_type: str
    message: str
    http_status: int | None = None
    occurred_at: datetime


class ResourceSummary(_OpsModel):
    source: str
    resource: str
    health: ResourceHealth
    latest_source_date: date | None = None
    latest_success_source_date: date | None = None
    freshness_days: int | None = None
    unresolved_failed_dates: int = Field(default=0, ge=0)
    total_attempts: int = Field(default=0, ge=0)


class DateSummary(_OpsModel):
    source: str
    resource: str
    source_date: date
    status: DateIngestionStatus
    attempt_count: int = Field(ge=0)
    effective_run_id: str | None = None
    latest_run_id: str | None = None
    latest_attempt_status: DayStatus | None = None
    bronze_records: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    last_attempt_at: datetime | None = None


class DateDetail(_OpsModel):
    source: str
    resource: str
    source_date: date
    status: DateIngestionStatus
    effective_run_id: str | None = None
    attempts: list[AttemptSummary]


class AttemptDetail(_OpsModel):
    attempt: AttemptSummary
    pages: list[PageSummary]
    errors: list[ErrorSummary]


class RunDetail(_OpsModel):
    run: RunSummary
    attempts: list[AttemptSummary]


class OpsOverview(_OpsModel):
    generated_at: datetime
    resources: list[ResourceSummary]
