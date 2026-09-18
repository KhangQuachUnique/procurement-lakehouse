import ssl
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from random import uniform
from typing import Any, Self

import httpx

from procurement.common.settings import settings

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def create_muasamcong_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.set_ciphers("DEFAULT:@SECLEVEL=1")
    return context


class MuasamcongClient:
    """HTTP transport shared by all Mua Sam Cong resource adapters."""

    def __init__(
        self,
        *,
        token: str,
        max_attempts: int = 3,
        max_retry_delay: float = 30,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not token:
            raise ValueError("Muasamcong token is required")
        if max_attempts < 1 or max_retry_delay < 0:
            raise ValueError("max_attempts must be positive and max_retry_delay non-negative")
        self._token = token
        self._max_attempts = max_attempts
        self._max_retry_delay = max_retry_delay
        self._sleep = sleep
        self._auth_response: httpx.Response | None = None
        self._client = httpx.Client(
            base_url=settings.MUASAMCONG_BASE_URL,
            timeout=settings.MUASAMCONG_TIMEOUT_SECONDS,
            verify=create_muasamcong_ssl_context(),
            transport=transport,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "procurement-lakehouse/0.1",
            },
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def post(self, path: str, body: Any) -> dict[str, Any]:
        # This transport is for read-only source endpoints implemented as POST.
        # A rejected credential stays rejected for this client/run; avoid request storms.
        if self._auth_response is not None:
            self._auth_response.raise_for_status()
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._client.post(path, params={"token": self._token}, json=body)
                if response.status_code in {401, 403}:
                    self._auth_response = response
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < self._max_attempts:
                    delay = self._retry_delay(attempt, response.headers.get("Retry-After"))
                    # Do not retry earlier than requested when the server delay exceeds our budget.
                    if delay is not None:
                        self._sleep(delay)
                        continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise TypeError("Source response must be a JSON object")
                return payload
            except httpx.TransportError:
                if attempt >= self._max_attempts:
                    raise
                self._sleep(self._retry_delay(attempt) or 0)
        raise RuntimeError("Unexpected retry state")

    def _retry_delay(self, attempt: int, retry_after: str | None = None) -> float | None:
        server_delay = 0.0
        if retry_after:
            try:
                server_delay = max(0.0, float(retry_after))
            except ValueError:
                try:
                    requested = parsedate_to_datetime(retry_after)
                    if requested.tzinfo is None:
                        requested = requested.replace(tzinfo=UTC)
                    server_delay = max(0.0, (requested - datetime.now(UTC)).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    pass  # malformed Retry-After falls back to bounded jitter
        if server_delay > self._max_retry_delay:
            return None
        return max(server_delay, uniform(0, min(2 ** (attempt - 1), self._max_retry_delay)))
