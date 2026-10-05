import os
import subprocess
import sys
from datetime import UTC, datetime

from procurement.benchmark.store import ACTIVE, BenchmarkStore
from procurement.common.file_lock import LockBusy, exclusive_file_lock
from procurement.common.settings import settings


class BenchmarkService:
    def __init__(self, store=None):
        self.store = store or BenchmarkStore(settings.BENCHMARK_STATE_DIR, settings.BENCHMARK_EXPORT_DIR)

    def recover(self):
        """Only reclaim stale state after proving that no worker owns the OS lock."""
        for run in self.store.list():
            if run["status"] not in ACTIVE:
                continue
            age = (datetime.now(UTC) - datetime.fromisoformat(run["updated_at"])).total_seconds()
            if age < 120:
                continue
            try:
                with exclusive_file_lock(self.store.state_dir / "worker.lock"):
                    current = self.store.get(run["id"])
                    if current["updated_at"] == run["updated_at"] and current["status"] in ACTIVE:
                        self.store.update(run["id"], status="interrupted", summary={
                            **run["summary"], "reason": "worker_exited_without_final_status"})
            except LockBusy:
                pass

    def start(self, config):
        if not settings.MUASAMCONG_TOKEN:
            raise ValueError("MUASAMCONG_TOKEN is not configured on the server")
        with exclusive_file_lock(self.store.state_dir / "launch.lock"):
            self.recover()
            run_id = self.store.create(config)
            args = [sys.executable, "-m", "procurement.tools.benchmark", "worker", run_id,
                    "--state-dir", str(self.store.state_dir), "--export-dir", str(self.store.export_dir)]
            options = {"start_new_session": True} if os.name != "nt" else {
                "creationflags": subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP}
            try:
                # Output is intentionally discarded: structured diagnostics are persisted without secrets.
                process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                           stderr=subprocess.DEVNULL, close_fds=True, **options)
                # Reap on POSIX without tying worker lifetime to the API server.
                import threading
                threading.Thread(target=process.wait, daemon=True).start()
            except Exception:
                self.store.update(run_id, status="failed", summary={"reason": "worker_launch_failed"})
                raise
        return self.store.get(run_id)
