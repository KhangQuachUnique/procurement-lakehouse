import ssl
import time
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

    def __init__(self, *, token: str, max_attempts: int = 3) -> None:
        if not token:
            raise ValueError("Muasamcong token is required")
        self._token = token
        self._max_attempts = max_attempts
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
