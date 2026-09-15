import fnmatch
import io
from datetime import UTC, date, datetime

from procurement.common.resources import ResourceIdentity
from procurement.models.control import (
    DayManifest,
    DayStatus,
    PageManifest,
    PageStatus,
    RunManifest,
    RunStatus,
)
from procurement.storage.control import (
    read_day_manifest,
    read_page_manifest,
    read_run_manifest,
    write_day_manifest,
    write_page_manifest,
    write_run_manifest,
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

    def glob(self, pattern: str) -> list[str]:
        return [key for key in self.objects if fnmatch.fnmatch(key, pattern)]


KHLCNT = ResourceIdentity("muasamcong", "khlcnt")
NOW = datetime(2026, 9, 12, tzinfo=UTC)


def test_control_hierarchy_round_trip() -> None:
    fs = MemoryFilesystem()
    source_date = date(2026, 9, 10)
    run = RunManifest(
        run_id="run-a",
        source="muasamcong",
        resource="khlcnt",
        start_date=source_date,
        end_date=source_date,
        status=RunStatus.RUNNING,
        total_dates=1,
        started_at=NOW,
    )
    day = DayManifest(
        run_id="run-a",
        source="muasamcong",
        resource="khlcnt",
        source_date=source_date,
        status=DayStatus.RUNNING,
        started_at=NOW,
    )
    page = PageManifest(
        run_id="run-a",
        source_date=source_date,
        page_number=0,
        page_size=50,
        status=PageStatus.SUCCESS,
        started_at=NOW,
        completed_at=NOW,
    )

    write_run_manifest(fs, KHLCNT, run)  # type: ignore[arg-type]
    write_day_manifest(fs, KHLCNT, day)  # type: ignore[arg-type]
    write_page_manifest(fs, KHLCNT, page)  # type: ignore[arg-type]

    assert read_run_manifest(fs, KHLCNT, "run-a") == run  # type: ignore[arg-type]
    assert read_day_manifest(fs, KHLCNT, "run-a", source_date) == day  # type: ignore[arg-type]
    assert read_page_manifest(fs, KHLCNT, "run-a", source_date, 0) == page  # type: ignore[arg-type]
    keys = "\n".join(fs.objects)
    assert "run_id=run-a/source_date=2026-09-10/day.json" in keys
    assert "run_id=run-a/source_date=2026-09-10/pages/page-000000.json" in keys
