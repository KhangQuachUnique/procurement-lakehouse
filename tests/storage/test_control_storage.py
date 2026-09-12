import io
from datetime import UTC, date, datetime, timedelta

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.storage.locks import ActiveLockError, acquire_daily_lock, release_daily_lock
from procurement.storage.manifests import read_daily_success, write_daily_success


class MemoryFile(io.BytesIO):
    def __init__(self, fs: "MemoryFilesystem", key: str, initial: bytes = b"") -> None:
        super().__init__(initial); self.fs = fs; self.key = key

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


def test_daily_success_round_trip() -> None:
    fs = MemoryFilesystem(); source_date = date(2026, 9, 10)
    metadata = {"status": "completed", "query_fingerprint": "abc"}
    write_daily_success(fs, KHLCNT, source_date, metadata)  # type: ignore[arg-type]
    assert read_daily_success(fs, KHLCNT, source_date) == metadata  # type: ignore[arg-type]


def test_active_lock_blocks_other_run() -> None:
    fs = MemoryFilesystem(); source_date = date(2026, 9, 10); now = datetime(2026, 9, 11, tzinfo=UTC)
    acquire_daily_lock(fs, KHLCNT, source_date, "run-1", now=now)  # type: ignore[arg-type]
    with pytest.raises(ActiveLockError):
        acquire_daily_lock(fs, KHLCNT, source_date, "run-2", now=now + timedelta(hours=1))  # type: ignore[arg-type]
    release_daily_lock(fs, KHLCNT, source_date, "run-1")  # type: ignore[arg-type]
