from datetime import date, datetime

from pydantic import BaseModel, ConfigDict

from procurement.common.errors import ErrorCode, ErrorStage


class ErrorRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = 1
    error_id: str
    run_id: str
    source: str
    resource: str
    source_date: date
    page_number: int | None = None
    stage: ErrorStage
    code: ErrorCode
    source_id: str | None = None
    error_type: str
    message: str
    http_status: int | None = None
    occurred_at: datetime
