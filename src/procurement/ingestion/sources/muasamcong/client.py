import ssl
import threading
import time
from typing import Any, Self

import httpx

from procurement.common.settings import settings

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def create_muasamcong_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.set_ciphers("DEFAULT:@SECLEVEL=1")
    return context


class _RequestPacer:
    """Space request starts across threads to avoid burst traffic."""

    def __init__(self, min_interval_seconds: float) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be non-negative")
        self._min_interval_seconds = min_interval_seconds
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0

    def wait(self) -> None:
        if self._min_interval_seconds == 0:
            return

        with self._lock:
            now = time.monotonic()
            scheduled_at = max(now, self._next_allowed_at)
            self._next_allowed_at = scheduled_at + self._min_interval_seconds

        delay = scheduled_at - now
        if delay > 0:
            time.sleep(delay)


class MuasamcongClient:
    """HTTP transport shared by all Mua Sam Cong resource adapters."""

    def __init__(
        self,
        *,
        token: str,
        max_attempts: int = 3,
        min_request_interval_seconds: float | None = None,
    ) -> None:
        if not token:
            raise ValueError("Muasamcong token is required")
        self._token = token
        self._max_attempts = max_attempts
        request_interval = (
            settings.MUASAMCONG_MIN_REQUEST_INTERVAL_SECONDS
            if min_request_interval_seconds is None
            else min_request_interval_seconds
        )
        self._request_pacer = _RequestPacer(request_interval)
        self._client = httpx.Client(
            base_url=settings.MUASAMCONG_BASE_URL,
            timeout=settings.MUASAMCONG_TIMEOUT_SECONDS,
            verify=create_muasamcong_ssl_context(),
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
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._request_pacer.wait()
                response = self._client.post(path, params={"token": self._token}, json=body)
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < self._max_attempts:
                    self._sleep_before_retry(attempt)
                    continue
                response.raise_for_status()
                return response.json()
            except httpx.TransportError:
                if attempt >= self._max_attempts:
                    raise
                self._sleep_before_retry(attempt)
        raise RuntimeError("Unexpected retry state")

    @staticmethod
    def _sleep_before_retry(attempt: int) -> None:
        time.sleep(2 ** (attempt - 1))
