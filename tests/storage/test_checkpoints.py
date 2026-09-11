import io
from datetime import UTC, date, datetime, timedelta

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.storage.checkpoints import (
    ActiveLockError,
    IncompatibleCheckpointError,
    acquire_daily_lock,
    calculate_query_fingerprint,
    ensure_compatible,
    read_daily_success,
    release_daily_lock,
    write_daily_success,
)


class MemoryFile(io.BytesIO):
    def __init__(self, fs: "MemoryFilesystem", key: str, initial: bytes = b"") -> None:
        super().__init__(initial)
        self.fs = fs
        self.key = key

    def close(self) -> None:
        if not self.closed:
            self.fs.objects[self.key] = self.getvalue()
        super().close()


class MemoryFilesystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.objects

    def open(self, key: str, mode: str) -> MemoryFile:
        initial = self.objects.get(key, b"") if mode == "rb" else b""
        return MemoryFile(self, key, initial)

    def rm(self, key: str) -> None:
        del self.objects[key]


KHLCNT = ResourceIdentity("muasamcong", "khlcnt")


def test_fingerprint_is_stable_and_detects_query_change() -> None:
    first = calculate_query_fingerprint({"page_size": 50, "type": "plan"})
    same = calculate_query_fingerprint({"type": "plan", "page_size": 50})
    changed = calculate_query_fingerprint({"page_size": 100, "type": "plan"})

    assert first == same
    assert first != changed
    ensure_compatible({"query_fingerprint": first}, same)
    with pytest.raises(IncompatibleCheckpointError):
        ensure_compatible({"query_fingerprint": first}, changed)


def test_daily_success_round_trip() -> None:
    fs = MemoryFilesystem()
    source_date = date(2026, 9, 10)
    metadata = {"status": "completed", "query_fingerprint": "abc"}

    write_daily_success(fs, KHLCNT, source_date, metadata)  # type: ignore[arg-type]

    assert read_daily_success(fs, KHLCNT, source_date) == metadata  # type: ignore[arg-type]


def test_active_lock_blocks_other_run_and_owner_can_release() -> None:
    fs = MemoryFilesystem()
    source_date = date(2026, 9, 10)
    now = datetime(2026, 9, 11, tzinfo=UTC)
    acquire_daily_lock(  # type: ignore[arg-type]
        fs, KHLCNT, source_date, "run-1", now=now
    )

    with pytest.raises(ActiveLockError):
        acquire_daily_lock(  # type: ignore[arg-type]
            fs, KHLCNT, source_date, "run-2", now=now + timedelta(hours=1)
        )

    release_daily_lock(fs, KHLCNT, source_date, "run-1")  # type: ignore[arg-type]
    acquire_daily_lock(  # type: ignore[arg-type]
        fs, KHLCNT, source_date, "run-2", now=now + timedelta(hours=1)
    )


def test_resources_use_independent_control_namespaces() -> None:
    fs = MemoryFilesystem()
    source_date = date(2026, 9, 10)
    tbmt = ResourceIdentity("muasamcong", "tbmt")

    write_daily_success(fs, KHLCNT, source_date, {"resource": "khlcnt"})  # type: ignore[arg-type]
    write_daily_success(fs, tbmt, source_date, {"resource": "tbmt"})  # type: ignore[arg-type]

    assert len(fs.objects) == 2
    assert read_daily_success(fs, KHLCNT, source_date) == {"resource": "khlcnt"}  # type: ignore[arg-type]
    assert read_daily_success(fs, tbmt, source_date) == {"resource": "tbmt"}  # type: ignore[arg-type]
