"""Contracts, request DTOs, and result specifications for Ingestion Service."""

from dataclasses import dataclass, field
from datetime import date
from typing import Any
from uuid import UUID, uuid4


@dataclass(frozen=True)
class MaterializeDayRequest:
    source_date: date
    source: str = "muasamcong"
    resource: str = "project"
    refresh: bool = False
    request_id: UUID = field(default_factory=uuid4)
    owner_id: UUID = field(default_factory=uuid4)
    page_size: int = 50
    dagster_run_id: str | None = None


@dataclass(frozen=True)
class MaterializeDayResult:
    source: str
    resource: str
    source_date: date
    reused: bool
    status: str
    record_count: int = 0
    file_count: int = 0
    commit_id: UUID | None = None
    attempt_id: UUID | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
