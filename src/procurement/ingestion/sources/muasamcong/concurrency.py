"""One flow's request budget, shared by all its resource clients and detail threads."""

from contextlib import contextmanager
from threading import BoundedSemaphore, Lock

import httpx

from procurement.common.cancellation import IngestionInterrupted


class RequestBudget:
    def __init__(self, max_inflight: int = 3):
        if max_inflight < 1:
            raise ValueError("max_inflight must be positive")
        self._slots = BoundedSemaphore(max_inflight)
        self._lock = Lock()
        self._auth_response: httpx.Response | None = None
        self._cancelled = False

    def reject_auth(self, response: httpx.Response) -> None:
        with self._lock:
            if self._auth_response is None:
                self._auth_response = response

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True

    def check_cancelled(self) -> None:
        with self._lock:
            if self._cancelled:
                raise IngestionInterrupted("Ingestion requests cancelled")

    @contextmanager
    def request(self):
        with self._slots:
            # Check after acquiring: queued requests must see auth failures/cancellation.
            with self._lock:
                if self._auth_response is not None:
                    self._auth_response.raise_for_status()
                if self._cancelled:
                    raise IngestionInterrupted("Ingestion requests cancelled")
            yield
