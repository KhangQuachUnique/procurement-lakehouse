import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from threading import Event
from unittest.mock import Mock

import fsspec
import pytest
from fastapi.testclient import TestClient

from procurement.api.main import app
from procurement.api.ops.dependencies import get_ops_runtime
from procurement.models.control import DayManifest, DayStatus, RunManifest, RunStatus
from procurement.ops.index import IndexNotReady, OpsIndex, journal_mode
from procurement.ops.repositories.indexed import IndexedControlRepository, IndexedErrorRepository
from procurement.ops.service import OpsService
from procurement.ops.sync import IndexSynchronizer

NOW = datetime(2026, 9, 18, tzinfo=UTC)


@pytest.fixture
def index_env(tmp_path):
    bucket = (tmp_path / "bucket").as_posix()
    fs = fsspec.filesystem("file", auto_mkdir=True, skip_instance_cache=True)
    fs.makedirs(bucket)
    index = OpsIndex(tmp_path / "ops.sqlite3", namespace=bucket)
    sync = IndexSynchronizer(index, fs, bucket=bucket)
    return fs, bucket, index, sync


def write(fs, key, payload):
    fs.pipe_file(key, json.dumps(payload).encode())


def seed(fs, bucket, *, run_id="old", status="success", started=NOW):
    prefix = f"{bucket}/_control/muasamcong/project/run_id={run_id}"
    run = RunManifest(
        run_id=run_id, source="muasamcong", resource="project", start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1), status=RunStatus(status), total_dates=1, started_at=started,
    )
    day = DayManifest(
        run_id=run_id, source="muasamcong", resource="project", source_date=date(2025, 1, 1),
        status=DayStatus(status), started_at=started, bronze_records=10 if status == "success" else 0,
    )
    write(fs, prefix + "/run.json", run.model_dump(mode="json"))
    key = prefix + "/source_date=2025-01-01/day.json"
    write(fs, key, day.model_dump(mode="json"))
    return key


def service(db, fs, state):
    return OpsService(IndexedControlRepository(db, fs), IndexedErrorRepository(db), sync_status=state)


def test_initial_sync_restart_and_hot_queries_make_no_storage_reads(index_env, monkeypatch):
    fs, bucket, index, sync = index_env
    with pytest.raises(IndexNotReady), index.snapshot():
        pass
    seed(fs, bucket)
    sync.sync_once()
    read = Mock(wraps=fs.cat_file)
    monkeypatch.setattr(fs, "cat_file", read)
    sync.sync_once()
    read.assert_not_called()  # metadata unchanged; completed manifests aren't downloaded again
    reopened = OpsIndex(index.path, namespace=bucket)
    monkeypatch.setattr(fs, "find", Mock(side_effect=AssertionError("no request-time listing")))
    with reopened.snapshot() as (db, state):
        ops = service(db, fs, state)
        assert len(ops.list_runs()) == 1
        assert ops.list_dates("project", start_date=date(2025, 1, 1), end_date=date(2025, 1, 1))[0].bronze_records == 10
        assert ops.get_date("project", date(2025, 1, 1)).effective_run_id == "old"
        assert ops.list_errors() == []
    read.assert_not_called()


def test_failed_refresh_preserves_success_and_failed_sync_preserves_snapshot(index_env, monkeypatch):
    fs, bucket, index, sync = index_env
    seed(fs, bucket)
    sync.sync_once()
    seed(fs, bucket, run_id="refresh", status="failed", started=NOW + timedelta(seconds=1))
    sync.sync_once()
    with index.snapshot() as (db, state):
        ops = service(db, fs, state)
        assert ops.get_date("project", date(2025, 1, 1)).effective_run_id == "old"
        assert ops.list_runs(resource="project", status="failed", limit=1)[0].run_id == "refresh"
    previous = index.get_status()["last_success_at"]
    seed(fs, bucket, run_id="unpublished", status="running")
    with monkeypatch.context() as mp:
        mp.setattr(fs, "cat_file", Mock(side_effect=OSError("storage offline")))
        with pytest.raises(OSError):
            sync.sync_once()
    state = index.get_status()
    assert state["last_success_at"] == previous
    assert state["last_error"] == "storage offline"
    with index.snapshot() as (db, state):
        assert len(service(db, fs, state).list_runs()) == 2
    sync.sync_once()
    assert index.get_status()["last_error"] is None
    with index.snapshot() as (db, state):
        assert len(service(db, fs, state).list_runs()) == 3


def test_transaction_rolls_back_rows_and_checkpoint_together(index_env):
    fs, bucket, index, sync = index_env
    key = seed(fs, bucket)
    sync.sync_once()
    previous = index.get_status()
    with index.connect() as db:
        payload = json.loads(db.execute("SELECT payload FROM objects WHERE key=?", (key,)).fetchone()[0])
    payload["status"] = "failed"
    with pytest.raises(sqlite3.IntegrityError):
        index.apply([
            (key, "changed", "day", "muasamcong", "project", "old", payload),
            (None, None, None, None, None, None, {}),
        ], [], started_at="new")
    assert index.get_status() == previous
    with index.snapshot() as (db, state):
        assert service(db, fs, state).get_date("project", date(2025, 1, 1)).status == "success"


def test_incomplete_listing_does_not_delete_but_confirmed_deletion_does(index_env, monkeypatch):
    fs, bucket, index, sync = index_env
    day_key = seed(fs, bucket)
    sync.sync_once()
    find = fs.find

    def omit_day(*args, **kwargs):
        return {key: value for key, value in find(*args, **kwargs).items() if key != day_key}

    monkeypatch.setattr(fs, "find", omit_day)
    sync.sync_once()
    with index.snapshot() as (db, state):
        assert service(db, fs, state).get_date("project", date(2025, 1, 1)).status == "success"
    fs.rm(day_key)
    sync.sync_once()
    with index.snapshot() as (db, state):
        assert service(db, fs, state).get_date("project", date(2025, 1, 1)).status == "no_attempt"


def test_reconcile_repairs_metadata_collision_and_errors_are_idempotent(index_env, monkeypatch):
    fs, bucket, index, sync = index_env
    seed(fs, bucket, status="failed")
    error_key = f"{bucket}/_errors/muasamcong/project/run_id=old/source_date=2025-01-01/page-000000.jsonl"
    error = {
        "schema_version": 2, "error_id": "e1", "source": "muasamcong", "resource": "project",
        "run_id": "old", "source_date": "2025-01-01", "page_number": 0, "stage": "detail",
        "error_type": "ReadTimeout", "message": "before", "occurred_at": NOW.isoformat(),
    }
    write(fs, error_key, error)
    monkeypatch.setattr(sync, "_fingerprint", lambda _: "same")
    sync.sync_once()
    error["message"] = "after"
    write(fs, error_key, error)
    sync.sync_once(force=True)
    sync.sync_once(force=True)
    with index.snapshot() as (db, state):
        errors = service(db, fs, state).list_errors(resource="project", stage="detail", limit=1)
        assert len(errors) == 1
        assert errors[0].message == "after"


def test_cross_storage_index_reuse_is_rejected(index_env):
    _, _, index, _ = index_env
    with pytest.raises(ValueError, match="another storage"):
        OpsIndex(index.path, namespace="other")


def test_reader_snapshot_remains_consistent_during_sync(index_env):
    fs, bucket, index, sync = index_env
    seed(fs, bucket)
    sync.sync_once()
    started = Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with index.snapshot() as (db, state):
            ops = service(db, fs, state)
            assert len(ops.list_runs()) == 1
            seed(fs, bucket, run_id="new")

            def update():
                started.set()
                sync.sync_once()

            future = pool.submit(update)
            assert started.wait(5)
            assert len(ops.list_runs()) == 1
        future.result(timeout=10)
    with index.snapshot() as (db, state):
        assert len(service(db, fs, state).list_runs()) == 2


def test_run_pagination_is_stable_and_filters_before_limit(index_env):
    fs, bucket, index, sync = index_env
    for n in range(5):
        seed(fs, bucket, run_id=str(n), status="failed" if n % 2 else "success")
    sync.sync_once()
    with index.snapshot() as (db, state):
        ops = service(db, fs, state)
        assert [item.run_id for item in ops.list_runs(limit=2)] == ["4", "3"]
        assert [item.run_id for item in ops.list_runs(limit=2, offset=2)] == ["2", "1"]
        assert [item.run_id for item in ops.list_runs(status="failed", limit=1, offset=1)] == ["1"]


def test_writer_ownership_prevents_competing_background_sync(index_env, monkeypatch):
    from procurement.common.file_lock import exclusive_file_lock

    _, _, index, sync = index_env
    called = Event()
    sync.interval = 0.01
    monkeypatch.setattr(sync, "sync_once", lambda: called.set())
    try:
        with exclusive_file_lock(index.path.with_suffix(".sync.lock")):
            sync.start()
            assert not called.wait(0.05)
        assert called.wait(5)
    finally:
        sync.stop()


@pytest.mark.parametrize("version,expected", [
    ((3, 49, 1), "DELETE"), ((3, 51, 2), "DELETE"), ((3, 51, 3), "WAL"),
    ((3, 44, 6), "WAL"), ((3, 50, 7), "WAL"), ((3, 52, 0), "WAL"),
])
def test_journal_mode_avoids_unpatched_wal_reset_race(version, expected):
    assert journal_mode(version) == expected


def test_api_bootstrap_and_ui_freshness_use_index_without_network(index_env, monkeypatch):
    fs, bucket, index, sync = index_env
    monkeypatch.setattr(sync, "start", lambda: None)
    monkeypatch.setattr(sync, "stop", lambda: None)
    app.dependency_overrides[get_ops_runtime] = lambda: sync
    try:
        with TestClient(app) as client:
            assert client.get("/ops").status_code == 503
            assert client.get("/api/ops/runs").status_code == 503
            assert client.get("/api/ops/sync").json()["ready"] is False
            seed(fs, bucket)
            sync.sync_once()
            monkeypatch.setattr(fs, "find", Mock(side_effect=AssertionError("unexpected listing")))
            response = client.get("/ops")
            assert response.status_code == 200
            assert "Updated" in response.text
            assert response.headers["X-Ops-Last-Sync"] == index.get_status()["last_success_at"]
            assert len(client.get("/api/ops/runs").json()) == 1
            assert client.get("/ops/calendar?resource=project&year=2025").status_code == 200
            index.record_failure("offline")
            response = client.get("/ops")
            assert "Sync delayed" in response.text
            assert response.headers["X-Ops-Sync-State"] == "degraded"
    finally:
        app.dependency_overrides.clear()
