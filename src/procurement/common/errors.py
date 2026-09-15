import re

import httpx

_SECRET_QUERY_RE = re.compile(
    r"(?i)([?&](?:token|access_token|api_key|apikey|secret|password)=)([^&\s]+)"
)
_AUTH_RE = re.compile(
    r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?)([^\s,;]+)"
)


def sanitize_error_message(message: str) -> str:
    """Preserve the original error message while redacting common credentials."""

    sanitized = _SECRET_QUERY_RE.sub(r"\1[REDACTED]", message)
    return _AUTH_RE.sub(r"\1[REDACTED]", sanitized)


def extract_http_status(exc: Exception) -> int | None:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code
    return None
