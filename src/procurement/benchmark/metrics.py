import math
import threading
import time
from collections import Counter, deque

from procurement.benchmark.store import now


def percentile(values, fraction):
    return round(sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)], 2) if values else None


class Metrics:
    """One phase, including drain time; all attempt events are persisted by the runner."""

    def __init__(self, stage, phase, config):
        self.stage, self.phase, self.config = stage, phase, config
        self.started = time.monotonic()
        self.started_at = now()
        self.finished = None
        self.attempts = []
        self.records = Counter()
        self.errors = Counter()
        self.recent_errors = deque(maxlen=20)
        self.retry_wait_seconds = 0.0
        self.cooldowns = []
        self.lock = threading.Lock()

    def observe(self, event):
        with self.lock:
            if event["kind"] == "attempt":
                self.attempts.append(event)
                if event["error"]:
                    self.errors[event["error"]] += 1
                    self.recent_errors.append(event)
            elif event["kind"] == "retry_wait":
                delay = event["delay_seconds"]
                self.retry_wait_seconds += delay
                start = time.monotonic()
                self.cooldowns.append((start, start + delay))

    def record(self, outcome):
        with self.lock:
            self.records[outcome] += 1

    def finish(self):
        self.finished = time.monotonic()

    def snapshot(self):
        with self.lock:
            end = self.finished or time.monotonic()
            elapsed = max(end - self.started, .001)
            endpoints = {}
            for path in sorted({a["endpoint"] for a in self.attempts}):
                attempts = [a for a in self.attempts if a["endpoint"] == path]
                durations = [a["latency_ms"] for a in attempts]
                endpoints[path] = {"attempts": len(attempts),
                                   "errors": sum(bool(a["error"]) for a in attempts),
                                   "p50_ms": percentile(durations, .5),
                                   "p95_ms": percentile(durations, .95)}
            first = [a for a in self.attempts if a["attempt"] == 1]
            # Union of shared cooldown intervals, clipped to actual phase wall time.
            cooldown = 0.0
            previous_end = self.started
            for start, stop in sorted(self.cooldowns):
                stop = min(stop, end)
                cooldown += max(0, stop - max(start, previous_end))
                previous_end = max(previous_end, stop)
            successes = self.records["valid"]
            total = sum(self.records.values())
            return {
                "stage": self.stage, "phase": self.phase, "config": self.config,
                "started_at": self.started_at, "elapsed_seconds": round(elapsed, 3),
                "attempts": len(self.attempts), "retries": len(self.attempts) - len(first),
                "first_attempt_success_rate": (
                    round(sum(not a["error"] for a in first) / len(first), 4) if first else None),
                "records": dict(self.records), "valid_records": successes,
                "record_success_rate": round(successes / total, 4) if total else None,
                "records_per_minute": round(successes * 60 / elapsed, 2),
                "requests_per_second": round(len(self.attempts) / elapsed, 3),
                "p95_ms": percentile([a["latency_ms"] for a in self.attempts], .95),
                "errors": dict(self.errors), "recent_errors": list(self.recent_errors),
                "cooldown_seconds": round(cooldown, 3),
                "retry_wait_requested_seconds": round(self.retry_wait_seconds, 3),
                "endpoints": endpoints,
            }
