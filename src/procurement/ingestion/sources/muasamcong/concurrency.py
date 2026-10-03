"""One flow's request budget, shared by all its resource clients and detail threads."""

from contextlib import contextmanager
from math import isfinite
from threading import BoundedSemaphore, Lock
from time import monotonic, sleep

import httpx

from procurement.common.cancellation import IngestionInterrupted


class RequestBudget:
    def __init__(self, max_inflight: int = 3, *, min_interval=0.0, clock=monotonic, wait=sleep):
        if max_inflight < 1:
            raise ValueError("max_inflight must be positive")
        self._slots = BoundedSemaphore(max_inflight)
        self._lock = Lock()
        self._auth_response: httpx.Response | None = None
        self._cancelled = False
        if not isfinite(min_interval) or min_interval < 0:
            raise ValueError("min_interval must be finite and non-negative")
        self._interval = min_interval
        self._clock, self._wait = clock, wait
        self._pace_lock = Lock()
        self._next_start = 0.0
        self._cooldown_until = 0.0

    def defer(self, seconds):
        """Pause queued requests as well as the request being retried."""
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, self._clock() + seconds)

    def _pace(self):
        with self._pace_lock:
            while True:
                with self._lock:
                    if self._auth_response is not None:
                        self._auth_response.raise_for_status()
                    if self._cancelled:
                        raise IngestionInterrupted("Ingestion requests cancelled")
                    delay = max(self._next_start, self._cooldown_until) - self._clock()
                if delay <= 0:
                    self._next_start = self._clock() + self._interval
                    return
                self._wait(min(delay, .25))

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
            self._pace()
            # Check after acquiring: queued requests must see auth failures/cancellation.
            with self._lock:
                if self._auth_response is not None:
                    self._auth_response.raise_for_status()
                if self._cancelled:
                    raise IngestionInterrupted("Ingestion requests cancelled")
            yield
