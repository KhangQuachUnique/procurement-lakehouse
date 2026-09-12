from collections.abc import Iterator
from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine import daily_runner
from procurement.ingestion.engine.fingerprint import (
    IncompatibleCheckpointError,
    calculate_page_fingerprint,
    calculate_query_fingerprint,
)
from procurement.ingestion.engine.models import ResourceSpec

SOURCE_DATE = date(2026, 9, 10)
WINDOW_FROM = "2026-09-10T00:00:00.000Z"
WINDOW_TO = "2026-09-10T23:59:59.999Z"
IDENTITY = ResourceIdentity("test-source", "test-resource")


class FakePipeline:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.records: list[dict[str, Any]] = []

    def run(self, records: Iterator[dict[str, Any]]) -> SimpleNamespace:
        self.events.append("bronze")
        self.records = list(records)
        return SimpleNamespace(loads_ids=["load-1"])


class Harness:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.events: list[str] = []
        self.manifests: list[dict[str, Any]] = []
        self.checkpoints: list[dict[str, Any]] = []
        self.saved_errors: list[dict[str, Any]] = []
        self.success: dict[str, Any] | None = None
        self.page_checkpoint: dict[str, Any] | None = None
        self.pipeline = FakePipeline(self.events)

        monkeypatch.setattr(daily_runner, "read_daily_success", lambda *_: self.success)
        monkeypatch.setattr(daily_runner, "read_page_checkpoint", lambda *_: self.page_checkpoint)
        monkeypatch.setattr(daily_runner, "acquire_daily_lock", lambda *_: self.events.append("lock"))
        monkeypatch.setattr(daily_runner, "refresh_daily_lock", lambda *_: self.events.append("refresh"))
        monkeypatch.setattr(daily_runner, "release_daily_lock", lambda *_: self.events.append("release"))
        monkeypatch.setattr(daily_runner, "write_run_manifest", self._write_manifest)
        monkeypatch.setattr(daily_runner, "write_daily_success", self._write_success)
        monkeypatch.setattr(daily_runner, "write_page_checkpoint", self._write_checkpoint)
        monkeypatch.setattr(daily_runner, "save_raw_search_page", self._save_raw)
        monkeypatch.setattr(daily_runner, "save_error_records", self._save_errors)
        monkeypatch.setattr(daily_runner, "create_bronze_destination", lambda **_: object())
        monkeypatch.setattr(daily_runner, "create_bronze_resource", lambda records, **_: records)
        monkeypatch.setattr(daily_runner.dlt, "pipeline", lambda **_: self.pipeline)

    def _write_manifest(self, *args: Any) -> str:
        self.manifests.append(dict(args[-1]))
        return "s3://manifest"

    def _write_success(self, *args: Any) -> str:
        self.events.append("success")
        self.success = dict(args[-1])
        return "s3://success"

    def _write_checkpoint(self, *args: Any) -> str:
        self.events.append("checkpoint")
        self.checkpoints.append(dict(args[-1]))
        return "s3://checkpoint"

    def _save_raw(self, **_: Any) -> str:
        self.events.append("raw")
        return "s3://raw/page.json.gz"

    def _save_errors(self, **kwargs: Any) -> str:
        self.events.append("errors")
        self.saved_errors.extend(kwargs["records"])
        return "s3://errors/page.jsonl"


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Harness:
    return Harness(monkeypatch)


def _page(content: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    items = [{"id": "item-1"}] if content is None else content
    return {"page": {"content": items, "totalElements": len(items), "last": True}}


def _spec(*, iter_records: Any | None = None) -> ResourceSpec:
    def fetch(**_: Any) -> dict[str, Any]:
        return _page()

    def records(*, stats: Any, **_: Any) -> Iterator[dict[str, Any]]:
        stats.record("notice")
        yield {"_resource": "test_notice", "_source_id": "item-1"}

    def query_definition(*, source_date: date, window_from: str, window_to: str, page_size: int) -> dict[str, Any]:
        return {"source": IDENTITY.source, "resource": IDENTITY.resource, "source_date": source_date.isoformat(), "window_from": window_from, "window_to": window_to, "page_size": page_size}

    return ResourceSpec(identity=IDENTITY, pipeline_name="test_pipeline", dataset_name="test_dataset", fetch_page=fetch, build_query_definition=query_definition, iter_records=iter_records or records)


def _fingerprint(spec: ResourceSpec) -> str:
    return calculate_query_fingerprint(spec.query_definition(source_date=SOURCE_DATE, window_from=WINDOW_FROM, window_to=WINDOW_TO, page_size=50))


def _run(spec: ResourceSpec, *, force: bool = False):
    return daily_runner.run_daily_resource(fs=object(), spec=spec, run_id="run-1", source_date=SOURCE_DATE, page_size=50, force=force)  # type: ignore[arg-type]


def test_success_is_raw_then_bronze_then_checkpoint(harness: Harness) -> None:
    result = _run(_spec())
    assert result["status"] == "completed"
    assert harness.events == ["lock", "raw", "bronze", "checkpoint", "refresh", "success", "release"]
    assert harness.checkpoints[0]["search_page_fingerprint"] == calculate_page_fingerprint([{"id": "item-1"}])


def test_checkpoint_resume_rejects_changed_page_order(harness: Harness) -> None:
    spec = _spec()
    harness.page_checkpoint = {"query_fingerprint": _fingerprint(spec), "search_page_fingerprint": calculate_page_fingerprint([{"id": "different"}]), "search_items": 1, "record_counts": {}, "error_counts": {}}
    with pytest.raises(IncompatibleCheckpointError):
        _run(spec)
    assert "bronze" not in harness.events


def test_record_error_finishes_crawl_but_keeps_historical_error(harness: Harness) -> None:
    def records(*, errors: list[dict[str, Any]], stats: Any, **_: Any):
        stats.error("notice")
        errors.append({"stage": "plan_detail", "error_id": "err-1"})
        yield {"_resource": "test_notice", "_source_id": "ok"}

    result = _run(_spec(iter_records=records))
    assert result["status"] == "completed_with_errors"
    assert harness.success is not None
    assert harness.success["crawl_complete"] is True
    assert harness.saved_errors[0]["error_id"] == "err-1"


def test_force_reprocesses_append_only_bronze(harness: Harness) -> None:
    spec = _spec()
    harness.success = {"run_id": "old", "query_fingerprint": _fingerprint(spec)}
    result = _run(spec, force=True)
    assert result["status"] == "completed"
    assert "bronze" in harness.events
