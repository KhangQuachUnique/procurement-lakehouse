"""Best-effort liveness sidecar. Never changes whether business data is committed."""

import logging
import threading
from datetime import UTC, datetime

from procurement.common.resources import ResourceIdentity
from procurement.common.settings import settings
from procurement.storage.io import read_json, write_json

logger = logging.getLogger(__name__)


def execution_key(identity: ResourceIdentity, run_id: str) -> str:
    return (f"{settings.OBJECT_STORAGE_BUCKET}/_ops/{identity.source}/{identity.resource}/"
            f"run_id={run_id}/execution.json")


def read_execution(fs, identity: ResourceIdentity, run_id: str):
    return read_json(fs, execution_key(identity, run_id))


class ExecutionHeartbeat:
    def __init__(self, fs, identity: ResourceIdentity, run_id: str, *, interval: float = 30):
        self.fs, self.identity, self.run_id = fs, identity, run_id
        self.interval = interval
        self.started_at = datetime.now(UTC).isoformat()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="ingestion-heartbeat")

    def _write(self, state: str) -> None:
        try:
            write_json(self.fs, execution_key(self.identity, self.run_id), {
                "schema_version": 1, "run_id": self.run_id,
                "source": self.identity.source, "resource": self.identity.resource,
                "state": state, "started_at": self.started_at,
                "heartbeat_at": datetime.now(UTC).isoformat(),
            })
        except Exception:  # sidecar failures must never fail a committed ingestion
            logger.warning("heartbeat_write_failed run_id=%s", self.run_id, exc_info=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._write("running")
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, *_):
        self._stop.set()
        self._thread.join(timeout=2)
        if self._thread.is_alive():
            # Do not race a stalled writer with a terminal sidecar write.
            logger.warning("heartbeat_stop_pending run_id=%s", self.run_id)
            return
        self._write("finished" if exc_type is None else "interrupted")
