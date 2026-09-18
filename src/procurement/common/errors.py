import re
import uuid
from datetime import UTC, date, datetime

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.models.errors import ErrorRecord

_SECRET_QUERY_RE = re.compile(
    r"(?i)([?&](?:token|access_token|api_key|apikey|secret|password)=)([^&\s]+)"
)
_AUTH_RE = re.compile(r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?)([^\s,;]+)")


def sanitize_error_message(message: str) -> str:
    """Preserve the original error message while redacting common credentials."""

    sanitized = _SECRET_QUERY_RE.sub(r"\1[REDACTED]", message)
    return _AUTH_RE.sub(r"\1[REDACTED]", sanitized)


def extract_http_status(exc: Exception) -> int | None:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code
    return None


def build_error_record(
    *,
    identity: ResourceIdentity,
    run_id: str,
    stage: str,
    source_date: date,
    page_number: int | None,
    exc: Exception,
    source_id: str | None = None,
) -> ErrorRecord:
    return ErrorRecord(
        error_id=uuid.uuid4().hex,
        run_id=run_id,
        source=identity.source,
        resource=identity.resource,
        source_date=source_date,
        page_number=page_number,
        stage=stage,
        source_id=source_id,
        error_type=type(exc).__name__,
        message=sanitize_error_message(str(exc)),
        http_status=extract_http_status(exc),
        occurred_at=datetime.now(UTC),
    )
