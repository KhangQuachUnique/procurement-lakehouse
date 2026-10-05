import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from procurement.api.benchmark import get_benchmark_service, router
from procurement.benchmark.metrics import Metrics
from procurement.benchmark.models import BenchmarkConfig, StageConfig
from procurement.benchmark.runner import BenchmarkRunner
from procurement.benchmark.service import BenchmarkService
from procurement.benchmark.store import BenchmarkStore
from procurement.common.file_lock import exclusive_file_lock
from procurement.ingestion.sources.muasamcong.bid_opening.resource import ENDPOINTS
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient

FIXTURES = Path(__file__).parents[1] / "ingestion/muasamcong/fixtures"


@pytest.fixture
def payload():
    return json.loads((FIXTURES / "bid_opening.json").read_text(encoding="utf-8"))


@pytest.fixture
def store(tmp_path):
    return BenchmarkStore(tmp_path / "state", tmp_path / "exports")


def configuration(payload, **kwargs):
    sample = payload["notify"]["bidNoContractorResponse"]["bidNotification"]
    return BenchmarkConfig(samples=[sample], stages=[StageConfig(
        request_interval=.1, duration_seconds=10, warmup_seconds=0)], cooldown_seconds=0, **kwargs)


def factory(handler):
    def create(**kwargs):
        kwargs["token"] = "SECRET_TEST_TOKEN"
        return MuasamcongClient(transport=httpx.MockTransport(handler), **kwargs)
    return create


def handle_payload(payload, request):
    part = next(k for k, path in ENDPOINTS.items() if path == request.url.path)
    return httpx.Response(200, json=payload[part])


def test_retry_attempts_and_validated_record_survive_stop(store, payload):
    calls = 0
    run_id = store.create(configuration(payload, max_requests=6))

    def handle(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadError("SECRET_TEST_TOKEN must not be recorded", request=request)
        return handle_payload(payload, request)

    BenchmarkRunner(store, run_id, client_factory=factory(handle)).execute()
    run = store.get(run_id)
    assert run["status"] == "stopped"
    assert run["summary"]["reason"] == "request_budget_reached"
    phase = run["summary"]["phases"][0]
    assert phase["valid_records"] == 1
    assert phase["retries"] == 1
    assert phase["errors"] == {"ReadError": 1}
    assert phase["records"]["cancelled"] == 1
    folder = store.folder(run_id)
    for name in ("config.json", "samples.json", "metadata.json", "attempts.jsonl", "summary.json", "stages.csv", "report.html"):
        assert (folder / name).is_file()
        assert "SECRET_TEST_TOKEN" not in (folder / name).read_text(encoding="utf-8")
    events = [json.loads(line) for line in (folder / "attempts.jsonl").read_text().splitlines()]
    assert len([e for e in events if e["kind"] == "attempt"]) == 6


@pytest.mark.parametrize("status,reason", [(401, "http_401"), (403, "http_403"), (429, "rate_limited")])
def test_block_response_stops_without_retry_or_next_stage(store, payload, status, reason):
    config = configuration(payload)
    config.mode = "auto"
    config.stages *= 2
    run_id = store.create(config)
    runner = BenchmarkRunner(store, run_id, client_factory=factory(
        lambda request: httpx.Response(status, headers={"Retry-After": "10"})))
    runner.execute()
    result = store.get(run_id)
    assert result["summary"]["reason"] == reason
    assert result["summary"]["attempts"] == 1
    assert len(result["summary"]["phases"]) == 1


def test_manual_change_drains_old_stage_and_persists_new_config(store, payload):
    run_id = store.create(configuration(payload, max_requests=12))
    changed = False

    def handle(request):
        nonlocal changed
        if not changed:
            changed = True
            store.command(run_id, StageConfig(max_inflight=2, request_interval=.1,
                                               duration_seconds=10, warmup_seconds=0))
        return handle_payload(payload, request)

    BenchmarkRunner(store, run_id, client_factory=factory(handle)).execute()
    phases = store.get(run_id)["summary"]["phases"]
    assert [p["stage"] for p in phases] == [1, 2]
    assert phases[0]["valid_records"] == 1
    assert phases[0]["config"]["max_inflight"] == 1
    assert phases[1]["config"]["max_inflight"] == 2
    assert not phases[0]["completed_window"]


def test_stop_interrupts_retry_sleep_and_keeps_artifacts(store, payload):
    run_id = store.create(configuration(payload))
    called = threading.Event()

    def handle(request):
        called.set()
        return httpx.Response(503, headers={"Retry-After": "30"})

    runner = BenchmarkRunner(store, run_id, client_factory=factory(handle))
    thread = threading.Thread(target=runner.execute)
    thread.start()
    assert called.wait(3)
    store.command(run_id)
    thread.join(timeout=3)
    assert not thread.is_alive()
    result = store.get(run_id)
    assert result["status"] == "stopped"
    assert result["summary"]["reason"] == "user_stopped"
    assert (store.folder(run_id) / "summary.json").is_file()


def test_invalid_complete_payload_is_not_counted_as_success(store, payload):
    payload["bid_open"] = {"bidSubmissionByContractorViewResponse": None}
    run_id = store.create(configuration(payload, max_requests=5))
    BenchmarkRunner(store, run_id, client_factory=factory(
        lambda request: handle_payload(payload, request))).execute()
    phase = store.get(run_id)["summary"]["phases"][0]
    assert phase["valid_records"] == 0
    assert phase["records"]["invalid"] == 1


def test_store_single_active_stop_not_overwritten_and_recovery(store, payload):
    config = configuration(payload)
    run_id = store.create(config)
    with pytest.raises(ValueError, match="Another benchmark"):
        store.create(config)
    store.command(run_id)
    store.update(run_id, status="running", summary={"attempts": 2})
    assert store.get(run_id)["status"] == "stopping"
    old = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    with store.connect() as db:
        db.execute("UPDATE runs SET updated_at=? WHERE id=?", (old, run_id))
        db.commit()
    service = BenchmarkService(store)
    with exclusive_file_lock(store.state_dir / "worker.lock"):
        service.recover()
        assert store.get(run_id)["status"] == "stopping"
    service.recover()
    assert store.get(run_id)["status"] == "interrupted"
    assert store.create(config) != run_id


def test_metrics_keep_attempt_failures_after_success_and_union_cooldown():
    metric = Metrics(1, "measure", {})
    metric.started = time.monotonic() - 10
    metric.observe({"kind": "attempt", "endpoint": "/detail", "attempt": 1,
                    "latency_ms": 100, "error": "ReadError"})
    metric.observe({"kind": "attempt", "endpoint": "/detail", "attempt": 2,
                    "latency_ms": 20, "error": None})
    metric.record("valid")
    metric.cooldowns = [(metric.started+1, metric.started+4), (metric.started+2, metric.started+6)]
    result = metric.snapshot()
    assert result["first_attempt_success_rate"] == 0
    assert result["record_success_rate"] == 1
    assert result["retries"] == 1
    assert result["cooldown_seconds"] == 5
    assert result["p95_ms"] == 100


def test_ui_api_controls_validation_history_and_downloads(store, payload):
    app = FastAPI()
    app.include_router(router)
    service = BenchmarkService(store)
    app.dependency_overrides[get_benchmark_service] = lambda: service
    client = TestClient(app)
    run_id = store.create(configuration(payload))
    headers = {"X-Benchmark-Control": "1"}
    assert client.get("/ops/benchmark").status_code == 200
    assert "Chạy benchmark" in client.get("/ops/benchmark").text
    assert "Trần thời gian toàn lượt" in client.get("/ops/benchmark").text
    assert "Dừng lượt chạy khi gặp HTTP 429" in client.get("/ops/benchmark").text
    assert client.get("/ops/benchmark.js").status_code == 200
    assert client.get("/api/benchmarks").json()[0]["id"] == run_id
    assert client.post(f"/api/benchmarks/{run_id}/stop", json={}).status_code == 403
    assert client.post(f"/api/benchmarks/{run_id}/stage", json={"max_inflight": 0}, headers=headers).status_code == 422
    assert client.get(f"/api/benchmarks/{run_id}/artifacts/config.json").json()["resource"] == "bid_opening"
    assert client.get(f"/api/benchmarks/{run_id}/artifacts/index.sqlite3").status_code == 404
    assert client.get("/api/benchmarks/not-an-id").status_code == 404
    assert client.post(f"/api/benchmarks/{run_id}/stop", json={}, headers=headers).status_code == 200
    assert client.post(f"/api/benchmarks/{run_id}/stage", json={}, headers=headers).status_code == 409


def test_auto_stages_complete_with_separate_warmup_metrics(store, payload):
    config = configuration(payload)
    config.mode = "auto"
    config.stages = [StageConfig(request_interval=.1, warmup_seconds=.1, duration_seconds=10)] * 2
    run_id = store.create(config)
    runner = BenchmarkRunner(store, run_id, client_factory=factory(
        lambda request: handle_payload(payload, request)))
    runner.execute()
    result = store.get(run_id)
    assert result["status"] == "completed"
    phases = result["summary"]["phases"]
    assert [p["phase"] for p in phases] == ["warmup", "measure", "warmup", "measure"]
    assert [p["stage"] for p in phases] == [1, 1, 2, 2]
    assert all(p["valid_records"] > 0 for p in phases)
    assert all(p["elapsed_seconds"] >= 10 for p in phases if p["phase"] == "measure")


def test_zero_interval_and_disabled_budgets_finish_at_stage_deadline(store, payload):
    config = configuration(payload, max_requests=0, max_run_seconds=0)
    config.stages = [StageConfig(request_interval=0, duration_seconds=.1, warmup_seconds=0)]
    run_id = store.create(config)
    runner = BenchmarkRunner(store, run_id, client_factory=factory(
        lambda request: handle_payload(payload, request)))
    runner.started -= 8000  # The former hardcoded two-hour limit must not stop this run.
    runner.execute()
    run = store.get(run_id)
    assert run["status"] == "completed"
    assert run["summary"]["attempts"] >= 4
    assert run["summary"]["phases"][0]["valid_records"] >= 1


def test_optional_rate_limit_stop_can_be_disabled(store, payload):
    config = configuration(payload, max_requests=2, max_attempts=1,
                           stop_on_rate_limit=False, max_errors_in_window=0)
    config.stages[0].request_interval = 0
    run_id = store.create(config)
    calls = 0

    def handle(request):
        nonlocal calls
        calls += 1
        return httpx.Response(429) if calls == 1 else handle_payload(payload, request)

    BenchmarkRunner(store, run_id, client_factory=factory(handle)).execute()
    assert calls == 2
    assert store.get(run_id)["summary"]["reason"] == "request_budget_reached"
    assert store.get(run_id)["summary"]["phases"][0]["errors"] == {"HTTP_429": 1}


def test_discovery_fetches_more_than_four_pages_when_requested(store):
    from datetime import date
    from io import StringIO

    config = BenchmarkConfig(start_date=date(2023, 1, 1), end_date=date(2023, 1, 7),
                             sample_size=201, stages=[StageConfig(request_interval=0)])
    run_id = store.create(config)
    pages = []

    def handle(request):
        page = int(json.loads(request.content)[0]["pageNumber"])
        pages.append(page)
        return httpx.Response(200, json={"page": {"content": [
            {"id": str(i), "notifyNo": f"IB{i}"} for i in range(page*50, (page+1)*50)
        ]}})

    runner = BenchmarkRunner(store, run_id, client_factory=factory(handle))
    runner.events = StringIO()
    samples = runner.samples()
    assert len(samples) == 201
    assert pages == [0, 1, 2, 3, 4]
    assert runner.phases[0]["config"]["request_interval"] == 0
