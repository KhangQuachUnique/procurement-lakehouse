"""One background writer per local index; storage failures preserve the last snapshot."""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime

from procurement.common.catalog import DEFAULT_SOURCE, SUPPORTED_RESOURCES
from procurement.common.errors import sanitize_error_message
from procurement.common.file_lock import LockBusy, exclusive_file_lock
from procurement.models.control import DayManifest, RunManifest
from procurement.models.errors import ErrorRecord
from procurement.storage.errors import _normalize_error_payload

logger = logging.getLogger(__name__)


class IndexSynchronizer:
    def __init__(self, index, fs, *, bucket, interval=5, reconcile_interval=300, workers=8):
        self.index, self.fs = index, fs
        self.bucket = bucket.replace("\\", "/").rstrip("/")
        self.interval, self.reconcile_interval, self.workers = interval, reconcile_interval, workers
        self._stop = threading.Event()
        self._thread = None
        self._last_reconcile = 0.0

    def _identity(self, key):
        parts = key.removeprefix(self.bucket + "/").split("/")
        if len(parts) < 5:
            return None
        area, source, resource, run_part = parts[:4]
        if source != DEFAULT_SOURCE or resource not in SUPPORTED_RESOURCES:
            return None
        if not run_part.startswith("run_id=") or not run_part[7:]:
            return None
        tail = parts[4:]
        if area == "_control" and tail == ["run.json"]:
            kind = "run"
        elif area == "_control" and len(tail) == 2 and tail[-1] == "day.json":
            kind = "day"
        elif area == "_ops" and tail == ["execution.json"]:
            kind = "execution"
        elif area == "_errors" and len(tail) == 2 and tail[-1].endswith(".jsonl"):
            kind = "errors"
        else:
            return None  # page manifests are loaded only when opening attempt details
        if kind in {"day", "errors"}:
            if not tail[0].startswith("source_date="):
                return None
            date.fromisoformat(tail[0][12:])
        return kind, source, resource, run_part[7:]

    @staticmethod
    def _fingerprint(info):
        fields = {key: info[key] for key in ("ETag", "LastModified", "mtime", "size") if key in info}
        # Size alone cannot detect in-place updates with equal-length JSON.
        if not any(key in fields for key in ("ETag", "LastModified", "mtime")):
            return None
        return json.dumps(fields, sort_keys=True, default=str)

    def _download(self, item):
        if self._stop.is_set():
            raise RuntimeError("Index synchronization stopped")
        key, fingerprint, identity = item
        kind, source, resource, run_id = identity
        raw = self.fs.cat_file(key).decode("utf-8")
        if kind == "errors":
            payload = [ErrorRecord.model_validate(_normalize_error_payload(json.loads(line)))
                       .model_dump(mode="json") for line in raw.splitlines() if line.strip()]
            records = payload
        else:
            payload = json.loads(raw)
            if kind in {"run", "day"}:
                model = RunManifest if kind == "run" else DayManifest
                payload = model.model_validate(payload).model_dump(mode="json")
            else:
                if payload["state"] not in {"running", "finished", "interrupted"}:
                    raise ValueError("Invalid execution state")
                if datetime.fromisoformat(payload["heartbeat_at"]).tzinfo is None:
                    raise ValueError("Heartbeat requires a timezone")
            records = [payload]
        for record in records:
            if (record["source"], record["resource"], record["run_id"]) != (source, resource, run_id):
                raise ValueError("Manifest identity does not match its storage key")
            if kind in {"day", "errors"} and f"/source_date={record['source_date']}/" not in key:
                raise ValueError("Manifest date does not match its storage key")
        return key, fingerprint, kind, source, resource, run_id, payload

    def sync_once(self, *, force=False):
        started_at = datetime.now(UTC).isoformat()
        reconcile = force or time.monotonic() - self._last_reconcile >= self.reconcile_interval
        try:
            if not self.fs.exists(self.bucket):
                raise RuntimeError("Ops storage bucket is unavailable")
            with self.index.connect() as db:
                known = {row["key"]: (row["fingerprint"], row["status"])
                         for row in db.execute("SELECT key, fingerprint, status FROM objects")}
            listed = {}
            for area in ("_control", "_ops", "_errors"):
                for key, info in self.fs.find(f"{self.bucket}/{area}", detail=True).items():
                    identity = self._identity(key)
                    if identity is not None:
                        listed[key] = (self._fingerprint(info), identity)
            changed = [
                (key, fingerprint, identity) for key, (fingerprint, identity) in listed.items()
                if reconcile or fingerprint is None or key not in known
                or known[key][0] != fingerprint or known[key][1] == "running"
            ]
            # Confirm absence directly; a missing listing entry alone must not delete coverage.
            removed = [key for key in known.keys() - listed.keys() if not self.fs.exists(key)]
            with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="ops-download") as pool:
                objects = list(pool.map(self._download, changed))
            if self._stop.is_set():
                return
            self.index.apply(objects, removed, started_at=started_at)
            if reconcile:
                self._last_reconcile = time.monotonic()
            logger.info("ops_index_synced downloaded=%s removed=%s", len(objects), len(removed))
        except Exception as exc:
            self.index.record_failure(sanitize_error_message(str(exc)))
            raise

    def _run(self):
        while not self._stop.is_set():
            try:
                with exclusive_file_lock(self.index.path.with_suffix(".sync.lock")):
                    while not self._stop.is_set():
                        try:
                            self.sync_once()
                        except Exception:
                            logger.exception("ops_index_sync_failed")
                        self._stop.wait(self.interval)
            except LockBusy:
                self._stop.wait(self.interval)  # another app process owns synchronization

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="ops-index-sync", daemon=True)
            self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)


def main():
    import argparse

    from procurement.common.logging_config import configure_logging
    from procurement.common.settings import settings
    from procurement.ops.index import OpsIndex
    from procurement.storage.object_store import create_s3_filesystem

    parser = argparse.ArgumentParser(description="Synchronize the rebuildable Ops SQLite index")
    parser.add_argument("--once", action="store_true", help="Sync once and exit")
    parser.add_argument("--rebuild", action="store_true", help="Re-read all objects; requires --once")
    args = parser.parse_args()
    if args.rebuild and not args.once:
        parser.error("--rebuild requires --once")
    configure_logging()
    index = OpsIndex(settings.OPS_INDEX_PATH, namespace=(
        f"{settings.OBJECT_STORAGE_ENDPOINT}/{settings.OBJECT_STORAGE_BUCKET}"
    ))
    worker = IndexSynchronizer(
        index, create_s3_filesystem(), bucket=settings.OBJECT_STORAGE_BUCKET,
        interval=settings.OPS_SYNC_INTERVAL_SECONDS,
        reconcile_interval=settings.OPS_RECONCILE_INTERVAL_SECONDS, workers=settings.OPS_SYNC_WORKERS,
    )
    if args.once:
        try:
            with exclusive_file_lock(index.path.with_suffix(".sync.lock")):
                worker.sync_once(force=args.rebuild)
        except LockBusy:
            parser.exit(1, "Ops sync is active; stop that service before a manual rebuild.\n")
        print(json.dumps(index.get_status()))
    else:
        try:
            worker._run()
        except KeyboardInterrupt:
            worker.stop()


if __name__ == "__main__":
    main()
