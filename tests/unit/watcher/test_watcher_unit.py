"""Unit tests for the watcher package."""
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from procurement.watcher import (
    NOTICE_ROOTS,
    WatcherService,
    WatchStore,
    calculate_due_date,
    namespace,
)

DAY = date(2025, 8, 3)
NOW = datetime(2025, 8, 22, tzinfo=UTC)
CTX = {
    "id": "uuid",
    "notifyNo": "IB1",
    "notifyVersion": "00",
    "isInternet": 1,
    "publicDate": "2025-08-03T12:00:00",
    "bidOpenDate": "2025-08-21T15:00:00",
}


def test_namespace_returns_consistent_hash():
    ns = namespace()
    assert isinstance(ns, str)
    assert len(ns) == 64


def test_notice_roots_configuration():
    assert "notify_contractor_standard_detail" in NOTICE_ROOTS
    assert "notify_contractor_vk_adb_detail" in NOTICE_ROOTS
    assert "notify_contractor_reoffer_detail" in NOTICE_ROOTS


def test_calculate_due_date_with_schedule():
    ctx = {"bidOpenDate": "2025-08-21T15:00:00"}
    due = calculate_due_date(ctx, NOW)
    assert due >= NOW


def test_calculate_due_date_without_schedule():
    ctx = {}
    due = calculate_due_date(ctx, NOW)
    assert due == NOW


def test_watch_store_lifecycle_and_status(tmp_path: Path):
    db_path = tmp_path / "test_watch.sqlite3"
    store = WatchStore(db_path)
    try:
        status = store.status(NOW)
        assert status["pending"] == 0
        assert status["captured"] == 0
        assert status["due_days"] == 0
        assert status["coverage_gaps"] == 0
    finally:
        store.close()

    # Reopening in read_only mode should work and match namespace
    store_ro = WatchStore(db_path, read_only=True)
    try:
        assert store_ro.due_days(now=NOW) == []
    finally:
        store_ro.close()


def test_watch_store_due_days_validation(tmp_path: Path):
    db_path = tmp_path / "test_watch.sqlite3"
    store = WatchStore(db_path)
    try:
        with pytest.raises(ValueError, match="limit must be positive"):
            store.due_days(limit=0)
    finally:
        store.close()


def test_watcher_service_facade(tmp_path: Path):
    db_path = tmp_path / "service_test.sqlite3"
    service = WatcherService(fs=Mock(), path=db_path)
    # Initialize store first
    store = WatchStore(db_path)
    store.close()

    assert service.due_days() == []
    status = service.read_status()
    assert status["pending"] == 0
