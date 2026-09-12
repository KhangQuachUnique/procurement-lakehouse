import io
import json
from datetime import date

import httpx

from procurement.common.errors import ErrorClassification, ErrorCode, ErrorStage
from procurement.common.resources import ResourceIdentity
from procurement.storage.errors import build_error_record, save_error_records


class CapturingBuffer(io.BytesIO):
    def __init__(self, fs: "FakeFilesystem", key: str) -> None:
        super().__init__(); self.fs = fs; self.key = key

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


def test_error_event_has_id_and_does_not_embed_retry_success() -> None:
    identity = ResourceIdentity("muasamcong", "khlcnt")
    record = build_error_record(
        identity=identity, run_id="run-1", stage=ErrorStage.PLAN_DETAIL,
        source_date=date(2026, 9, 10), search_page=2,
        exc=httpx.ReadTimeout("timeout"),
        classification=ErrorClassification(ErrorCode.SOURCE_TIMEOUT, True),
        retry_input={"plan_id": "p-1"}, source_id="p-1",
    )
    fs = FakeFilesystem()
    save_error_records(
        fs=fs, identity=identity, source_date=date(2026, 9, 10),
        run_id="run-1", stage=ErrorStage.PLAN_DETAIL, page_number=2, records=[record],
    )
    stored = json.loads(next(iter(fs.objects.values())).decode().strip())
    assert stored["error_id"]
    assert stored["retryable"] is True
    assert "retry_success" not in stored
