import csv
import hashlib
import json
import subprocess
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from html import escape

import httpx

from procurement.benchmark.metrics import Metrics
from procurement.benchmark.models import BenchmarkConfig, Sample, StageConfig
from procurement.benchmark.store import now, write_json
from procurement.common.cancellation import IngestionInterrupted
from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.bid_opening.resource import BidOpeningApi
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.ingestion.sources.muasamcong.contractor_result.resource import ContractorResultApi
from procurement.ingestion.sources.muasamcong.khlcnt.resource import KhlcntApi
from procurement.ingestion.sources.muasamcong.notify_contractor.resource import NotifyContractorApi
from procurement.ingestion.sources.muasamcong.project.resource import ProjectApi
from procurement.quality.contracts import load_config, validate_detail


class BenchmarkRunner:
    def __init__(self, store, run_id, *, client_factory=MuasamcongClient):
        self.store, self.run_id = store, run_id
        self.config = BenchmarkConfig.model_validate(store.get(run_id)["config"])
        self.folder = store.folder(run_id)
        self.client_factory = client_factory
        self.stop = threading.Event()
        self.reason = None
        self.revision = 0
        self.next_stage = None
        self.stage_number = 0
        self.metrics = None
        self.phases = []
        self.attempts = 0
        self.recent = deque(maxlen=20)
        self.event_lock = threading.Lock()
        self.events = None
        self.last_publish = 0
        self.budget = None
        self.started = time.monotonic()
        self.contract = load_config(resource="bid_opening")

    def halt(self, reason):
        # First cause wins, e.g. retain rate-limit evidence instead of subsequent cancellation.
        with self.event_lock:
            if self.reason is None:
                self.reason = reason
        self.stop.set()
        if self.budget is not None:
            self.budget.cancel()

    def observe(self, event):
        event = {**event, "at": now(), "stage": self.metrics.stage, "phase": self.metrics.phase}
        self.metrics.observe(event)
        reason = None
        with self.event_lock:
            self.events.write(json.dumps(event, ensure_ascii=False) + "\n")
            self.events.flush()
            if event["kind"] == "attempt":
                self.attempts += 1
                self.recent.append(bool(event["error"]))
                status = event["http_status"]
                if status in {401, 403}:
                    reason = f"http_{status}"
                elif status == 429 and self.config.stop_on_rate_limit:
                    reason = "rate_limited"
                elif (self.config.max_errors_in_window and len(self.recent) == 20
                      and sum(self.recent) >= self.config.max_errors_in_window):
                    reason = "error_rate_threshold"
                elif self.config.max_requests and self.attempts >= self.config.max_requests:
                    reason = "request_budget_reached"
        if reason:
            self.halt(reason)

    def poll(self):
        run = self.store.get(self.run_id)
        if run["command"].get("stop"):
            self.halt("user_stopped")
        elif run["revision"] > self.revision:
            self.revision = run["revision"]
            self.next_stage = StageConfig.model_validate(run["command"]["stage"])
        if self.config.max_run_seconds and time.monotonic() - self.started >= self.config.max_run_seconds:
            self.halt("run_time_limit")
        self.publish()

    def publish(self, force=False, status="running"):
        if not force and time.monotonic() - self.last_publish < 1:
            return
        summary = {"reason": self.reason, "attempts": self.attempts,
                   "elapsed_seconds": round(time.monotonic() - self.started, 3),
                   "phases": self.phases,
                   "current": self.metrics.snapshot() if self.metrics else None,
                   "pending_stage": self.next_stage.model_dump() if self.next_stage else None}
        self.store.update(self.run_id, status=status, summary=summary)
        self.last_publish = time.monotonic()
        return summary

    def sleep(self, seconds):
        if self.stop.wait(seconds):
            raise IngestionInterrupted("Benchmark stopped")

    def client(self, stage):
        self.budget = RequestBudget(stage.max_inflight, min_interval=stage.request_interval)
        return self.client_factory(
            token=settings.MUASAMCONG_TOKEN,
            request_budget=self.budget, observer=self.observe,
            max_attempts=self.config.max_attempts,
            max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
            shared_retry_cooldown=True, sleep=self.sleep,
        )

    def monitored(self, function):
        # Keep stop commands and heartbeat responsive during discovery/network waits.
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(function)
            while not future.done():
                self.poll()
                wait([future], timeout=.2)
            return future.result()

    def samples(self):
        if self.config.samples:
            return [s.model_dump(exclude_none=True) for s in self.config.samples]
        stage = self.config.stages[0].model_copy(update={"max_inflight": 1})
        self.metrics = Metrics(0, "discovery", stage.model_dump())
        with self.client(stage) as client:
            api = BidOpeningApi(client)
            samples, seen = [], set()
            # Discovery follows requested sample size, up to the source's 10,000-result window.
            for page in range(200):
                if self.stop.is_set():
                    break
                response = self.monitored(lambda page=page: api.search(
                    page_number=page, page_size=50,
                    window_from=f"{self.config.start_date}T00:00:00",
                    window_to=f"{self.config.end_date}T23:59:59",
                ))
                content = response["page"]["content"]
                if not isinstance(content, list):
                    raise TypeError("Invalid search content")
                previous_count = len(samples)
                for item in content:
                    sample = Sample.model_validate(item).model_dump(exclude_none=True)
                    key = sample.get("notifyId") or sample["id"]
                    if key not in seen:
                        samples.append(sample)
                        seen.add(key)
                    if len(samples) == self.config.sample_size:
                        break
                if (len(samples) == self.config.sample_size or len(content) < 50
                        or len(samples) == previous_count):
                    break
        self.metrics.finish()
        self.phases.append(self.metrics.snapshot())
        self.metrics = None
        return samples

    def record(self, api, sample):
        try:
            payload = api.fetch(sample, {})
            result = validate_detail(payload, sample, self.contract.contracts["bid_opening"])
            outcome = "valid" if result["status"] in {"pass", "warn"} else "invalid"
        except IngestionInterrupted:
            outcome = "cancelled"
        except (httpx.HTTPError, ValueError, TypeError, KeyError):
            outcome = "failed"
        self.metrics.record(outcome)

    def phase(self, api, samples, stage, name, seconds):
        self.metrics = Metrics(self.stage_number, name, stage.model_dump())
        deadline = time.monotonic() + seconds
        cursor = 0
        with ThreadPoolExecutor(max_workers=stage.max_inflight) as pool:
            pending = set()
            while True:
                self.poll()
                while (len(pending) < stage.max_inflight and time.monotonic() < deadline
                       and not self.stop.is_set() and self.next_stage is None):
                    pending.add(pool.submit(self.record, api, samples[cursor % len(samples)]))
                    cursor += 1
                if not pending:
                    break
                done, pending = wait(pending, timeout=.2, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
        self.metrics.finish()
        result = self.metrics.snapshot()
        result["completed_window"] = not self.stop.is_set() and self.next_stage is None
        self.phases.append(result)
        self.metrics = None
        self.publish(force=True)

    def cooldown(self):
        deadline = time.monotonic() + self.config.cooldown_seconds
        while time.monotonic() < deadline and not self.stop.is_set():
            self.poll()
            self.stop.wait(min(.2, max(0, deadline - time.monotonic())))

    def execute(self):
        with exclusive_file_lock(self.store.state_dir / "worker.lock"):
            if self.store.get(self.run_id)["status"] not in {"queued", "stopping"}:
                return
            status = "failed"
            with (self.folder / "attempts.jsonl").open("a", encoding="utf-8") as self.events:
                try:
                    self.poll()
                    if self.stop.is_set():
                        raise IngestionInterrupted("Stopped before start")
                    samples = self.samples()
                    if self.stop.is_set():
                        raise IngestionInterrupted("Stopped during discovery")
                    if not samples:
                        self.halt("no_samples")
                        raise IngestionInterrupted("No matching samples")
                    write_json(self.folder / "samples.json", samples)
                    try:
                        revision = subprocess.check_output(
                            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
                        dirty = bool(subprocess.check_output(
                            ["git", "status", "--porcelain"], text=True, stderr=subprocess.DEVNULL))
                    except (OSError, subprocess.SubprocessError):
                        revision, dirty = None, None
                    write_json(self.folder / "metadata.json", {
                        "created_at": now(), "code_revision": revision, "working_tree_dirty": dirty,
                        "quality_config_hash": self.contract.fingerprint,
                        "sample_sha256": hashlib.sha256(
                            json.dumps(samples, sort_keys=True).encode()).hexdigest(),
                        "sample_count": len(samples), "sample_policy": "repeat fixed sample order",
                    })
                    stages = iter(self.config.stages)
                    stage = self.next_stage or next(stages)
                    self.next_stage = None
                    while stage and not self.stop.is_set():
                        self.stage_number += 1
                        self.recent.clear()
                        with self.client(stage) as client:
                            api = BidOpeningApi(client)
                            if stage.warmup_seconds:
                                self.phase(api, samples, stage, "warmup", stage.warmup_seconds)
                            if not self.stop.is_set() and self.next_stage is None:
                                self.phase(api, samples, stage, "measure", stage.duration_seconds)
                        self.cooldown()
                        stage = self.next_stage or next(stages, None)
                        self.next_stage = None
                    status = "stopped" if self.stop.is_set() else "completed"
                except IngestionInterrupted:
                    status = "stopped"
                except Exception as exc:  # noqa: BLE001 - worker boundary persists a sanitized failure
                    # Persist the class only: HTTP exception messages can contain auth URLs.
                    self.halt(f"worker_error:{type(exc).__name__}")
                    status = "failed"
                finally:
                    if self.metrics is not None:
                        self.metrics.finish()
                        self.phases.append(self.metrics.snapshot())
                        self.metrics = None
                    summary = self.publish(force=True, status=status)
                    self.export(summary)

    def export(self, summary):
        write_json(self.folder / "summary.json", summary)
        columns = ["stage", "phase", "elapsed_seconds", "attempts", "retries", "valid_records",
                   "records_per_minute", "requests_per_second", "p95_ms", "cooldown_seconds"]
        with (self.folder / "stages.csv").open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows({k: phase.get(k) for k in columns} for phase in self.phases)
        rows = "".join("<tr>" + "".join(f"<td>{escape(str(p.get(c, '')))}</td>" for c in columns)
                       + "</tr>" for p in self.phases)
        html = ("<!doctype html><html lang='vi'><meta charset='utf-8'><title>Benchmark report</title>"
                "<style>body{font:14px system-ui;margin:32px}table{border-collapse:collapse}"
                "td,th{padding:10px;border:1px solid #ddd}pre{white-space:pre-wrap}</style>"
                f"<h1>Bid opening benchmark</h1><p>{self.run_id}</p><p>"
                "Repeated sample workload; throughput includes retries and drain time. "
                "Warmup and discovery are separate. Results do not establish a permanent safe rate.</p>"
                f"<table><tr>{''.join(f'<th>{c}</th>' for c in columns)}</tr>{rows}</table>"
                f"<h2>Details</h2><pre>{escape(json.dumps(summary, ensure_ascii=False, indent=2))}</pre></html>")
        (self.folder / "report.html").write_text(html, encoding="utf-8")
