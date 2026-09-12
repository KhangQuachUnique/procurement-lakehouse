import io
import json
from datetime import date

import httpx

from procurement.common.errors import ErrorClassification, ErrorCode, ErrorStage
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


def test_error_event_is_typed_immutable_fact_without_retry_state() -> None:
    identity = ResourceIdentity("muasamcong", "khlcnt")
    record = build_error_record(
        identity=identity,
        run_id="run-1",
        stage=ErrorStage.PLAN_DETAIL,
        source_date=date(2026, 9, 10),
        page_number=2,
        exc=httpx.ReadTimeout("timeout"),
        classification=ErrorClassification(ErrorCode.SOURCE_TIMEOUT, True),
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
    assert stored["error_id"]
    assert stored["stage"] == "plan_detail"
    assert stored["code"] == "SOURCE_TIMEOUT"
    assert "retryable" not in stored
    assert "retry_input" not in stored
