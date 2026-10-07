from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    max_inflight: int = Field(default=1, ge=1, le=32)
    request_interval: float = Field(default=2, ge=0)
    duration_seconds: float = Field(default=60, gt=0)
    warmup_seconds: float = Field(default=5, ge=0)


class Sample(BaseModel):
    # Only request identity and validation context are persisted, never arbitrary input.
    model_config = ConfigDict(extra="ignore")
    id: str | None = None
    notifyId: str | None = None
    notifyNo: str = Field(min_length=1, max_length=100)
    notifyVersion: str | None = None
    publicDate: str | None = None
    publicDateKqmt: str | None = None
    bidRealityOpenDate: str | None = None
    bidOpenDate: str | None = None
    stepCode: str | None = None
    processApply: str | None = None
    bidForm: str | None = None
    bidMode: str | None = None
    isInternet: int | None = None

    @model_validator(mode="after")
    def identity(self):
        if not (self.id or self.notifyId):
            raise ValueError("Sample requires id or notifyId")
        if self.id and self.notifyId and self.id != self.notifyId:
            raise ValueError("Conflicting sample identities")
        return self


class BenchmarkConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    resource: Literal[
        "bid_opening", "notify_contractor", "khlcnt", "project", "contractor_result"
    ] = "bid_opening"
    mode: Literal["manual", "auto"] = "manual"
    stages: list[StageConfig] = Field(default_factory=lambda: [StageConfig()], min_length=1)
    start_date: date | None = None
    end_date: date | None = None
    samples: list[Sample] = Field(default_factory=list)
    sample_size: int = Field(default=20, ge=1, le=10000)
    max_requests: int = Field(default=1000, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=10)
    cooldown_seconds: float = Field(default=10, ge=0)
    max_run_seconds: float = Field(default=7200, ge=0)
    stop_on_rate_limit: bool = True
    max_errors_in_window: int = Field(default=5, ge=0, le=20)

    @model_validator(mode="after")
    def valid_plan(self):
        if not self.samples and not (self.start_date and self.end_date):
            raise ValueError("Provide samples or a start/end date range")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must not exceed end_date")
        if self.mode == "manual" and len(self.stages) != 1:
            raise ValueError("Manual mode accepts one initial stage")
        return self
