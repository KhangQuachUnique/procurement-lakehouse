from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class ErrorRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = 2
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
