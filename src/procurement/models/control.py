from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"


class DayStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class PageStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class RunManifest(_StrictModel):
    schema_version: int = 1
    run_id: str
    source: str
    resource: str
    start_date: date
    end_date: date
    status: RunStatus
    total_dates: int = Field(ge=1)
    success_dates: int = Field(default=0, ge=0)
    failed_dates: int = Field(default=0, ge=0)
    started_at: datetime
    completed_at: datetime | None = None


class DayManifest(_StrictModel):
    schema_version: int = 1
    run_id: str
    source: str
    resource: str
    source_date: date
    status: DayStatus
    expected_pages: int | None = Field(default=None, ge=0)
    completed_pages: int = Field(default=0, ge=0)
    search_items: int = Field(default=0, ge=0)
    bronze_records: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    started_at: datetime
    completed_at: datetime | None = None


class PageManifest(_StrictModel):
    schema_version: int = 1
    run_id: str
    source_date: date
    page_number: int = Field(ge=0)
    page_size: int = Field(gt=0)
    status: PageStatus
    search_items: int = Field(default=0, ge=0)
    bronze_records: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    started_at: datetime
    completed_at: datetime | None = None
