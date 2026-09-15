import io
import json
from datetime import date

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.storage.errors import build_error_record, save_error_records


class CapturingBuffer(io.BytesIO):
    def __init__(self, fs: "FakeFilesystem", key: str) -> None:
        super().__init__()
        self.fs = fs
        self.key = key

    def close(self) -> None:
        if not self.closed:
            self.fs.objects[self.key] = self.getvalue()
        super().close()


class FakeFilesystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def open(self, key: str, mode: str) -> CapturingBuffer:
        assert mode == "wb"
        return CapturingBuffer(self, key)


def test_error_event_preserves_stage_type_and_message_without_error_code() -> None:
    identity = ResourceIdentity("muasamcong", "khlcnt")
    record = build_error_record(
        identity=identity,
        run_id="run-1",
        stage="plan_detail",
        source_date=date(2026, 9, 10),
        page_number=2,
        exc=httpx.ReadTimeout("timeout while reading plan detail"),
        source_id="p-1",
    )
    fs = FakeFilesystem()
    save_error_records(
        fs=fs,  # type: ignore[arg-type]
        identity=identity,
        source_date=date(2026, 9, 10),
        run_id="run-1",
        page_number=2,
        records=[record],
    )

    key, raw = next(iter(fs.objects.items()))
    stored = json.loads(raw.decode().strip())
    assert "run_id=run-1/source_date=2026-09-10/page-000002.jsonl" in key
    assert stored["schema_version"] == 2
    assert stored["error_id"]
    assert stored["resource"] == "khlcnt"
    assert stored["stage"] == "plan_detail"
    assert stored["error_type"] == "ReadTimeout"
    assert stored["message"] == "timeout while reading plan detail"
    assert "code" not in stored
    assert "retryable" not in stored
