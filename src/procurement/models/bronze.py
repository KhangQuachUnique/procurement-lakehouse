from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class BronzeRecord(BaseModel):
    """Source observation persisted in Bronze.

    System-owned lineage is typed. The external source payload intentionally stays
    flexible so Bronze can preserve source changes without schema-blocking ingestion.
    """

    model_config = ConfigDict(extra="forbid")

    source_id: str
    source_version: str | None = None
    run_id: str
    source_date: date
    ingested_at: datetime
    content_hash: str
    payload: dict[str, Any]
