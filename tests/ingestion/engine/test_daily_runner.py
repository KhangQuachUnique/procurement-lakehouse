from collections.abc import Iterator
from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine import daily_runner
from procurement.ingestion.engine.models import ResourceSpec
from procurement.storage.checkpoints import calculate_query_fingerprint

SOURCE_DATE = date(2026, 9, 10)
WINDOW_FROM = "2026-09-10T00:00:00.000Z"
WINDOW_TO = "2026-09-10T23:59:59.999Z"
IDENTITY = ResourceIdentity("test-source", "test-resource")


class FakePipeline:
    def __init__(self, events: list[str], *, error: Exception | None = None) -> None:
        self.events = events
        self.error = error
        self.records: list[dict[str, Any]] = []

    def run(self, records: Iterator[dict[str, Any]]) -> SimpleNamespace:
        self.events.append("bronze")
        self.records = list(records)
        if self.error is not None:
            raise self.error
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

        monkeypatch.setattr(daily_runner, "read_daily_success", self._read_success)
        monkeypatch.setattr(daily_runner, "read_page_checkpoint", self._read_checkpoint)
        monkeypatch.setattr(daily_runner, "acquire_daily_lock", self._acquire_lock)
        monkeypatch.setattr(daily_runner, "refresh_daily_lock", self._refresh_lock)
        monkeypatch.setattr(daily_runner, "release_daily_lock", self._release_lock)
        monkeypatch.setattr(daily_runner, "write_run_manifest", self._write_manifest)
        monkeypatch.setattr(daily_runner, "write_daily_success", self._write_success)
        monkeypatch.setattr(daily_runner, "write_page_checkpoint", self._write_checkpoint)
        monkeypatch.setattr(daily_runner, "save_raw_search_page", self._save_raw)
        monkeypatch.setattr(daily_runner, "save_error_records", self._save_errors)
        monkeypatch.setattr(daily_runner, "create_bronze_destination", lambda **_: object())
        monkeypatch.setattr(
            daily_runner,
            "create_bronze_resource",
            lambda records, **_: records,
        )
        monkeypatch.setattr(daily_runner.dlt, "pipeline", lambda **_: self.pipeline)

    def _read_success(self, *_: Any) -> dict[str, Any] | None:
        return self.success

    def _read_checkpoint(self, *_: Any) -> dict[str, Any] | None:
        return self.page_checkpoint

    def _acquire_lock(self, *_: Any) -> None:
        self.events.append("lock")

    def _refresh_lock(self, *_: Any) -> None:
        self.events.append("refresh")

    def _release_lock(self, *_: Any) -> None:
        self.events.append("release")

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


def _page(*, total: int = 1, content: list[dict[str, Any]] | None = None):
    return {
        "page": {
            "content": [{"id": "item-1"}] if content is None else content,
            "totalElements": total,
            "last": True,
        }
    }


def _spec(
    *,
    fetch_page: Any | None = None,
    iter_records: Any | None = None,
) -> ResourceSpec:
    def default_fetch(**_: Any) -> dict[str, Any]:
        return _page()

    def default_records(*, stats: Any, **_: Any) -> Iterator[dict[str, Any]]:
        stats.record("notice")
        yield {"_resource": "test_notice", "_source_id": "item-1"}

    def query_definition(
        *, source_date: date, window_from: str, window_to: str, page_size: int
    ) -> dict[str, Any]:
        return {
            "source": IDENTITY.source,
            "resource": IDENTITY.resource,
            "source_date": source_date.isoformat(),
            "window_from": window_from,
            "window_to": window_to,
            "page_size": page_size,
        }

    return ResourceSpec(
        identity=IDENTITY,
        pipeline_name="test_pipeline",
        dataset_name="test_dataset",
        fetch_page=fetch_page or default_fetch,
        build_query_definition=query_definition,
        iter_records=iter_records or default_records,
    )


def _fingerprint(spec: ResourceSpec, page_size: int = 50) -> str:
    definition = spec.query_definition(
        source_date=SOURCE_DATE,
        window_from=WINDOW_FROM,
        window_to=WINDOW_TO,
        page_size=page_size,
    )
    return calculate_query_fingerprint(definition)


def _run(spec: ResourceSpec, *, force: bool = False):
    return daily_runner.run_daily_resource(
        fs=object(),  # type: ignore[arg-type]
        spec=spec,
        run_id="run-1",
        source_date=SOURCE_DATE,
        page_size=50,
        force=force,
    )


def test_success_writes_raw_then_bronze_then_checkpoint(harness: Harness) -> None:
    result = _run(_spec())

    assert result == {
        "status": "completed",
        "pages": 1,
        "search_items": 1,
        "errors": 0,
    }
    assert harness.events == [
        "lock",
        "raw",
        "bronze",
        "checkpoint",
        "refresh",
        "success",
        "release",
    ]
    assert harness.checkpoints[0]["record_counts"] == {"notice": 1}
    assert harness.checkpoints[0]["bronze_load_ids"] == ["load-1"]
    assert harness.manifests[-1]["status"] == "completed"


def test_existing_daily_success_skips_before_lock_and_pipeline(harness: Harness) -> None:
    spec = _spec()
    harness.success = {"run_id": "old-run", "query_fingerprint": _fingerprint(spec)}

    result = _run(spec)

    assert result["status"] == "skipped"
    assert harness.events == []
    assert harness.manifests == []


def test_page_checkpoint_resumes_without_extracting_or_loading(harness: Harness) -> None:
    def should_not_extract(**_: Any) -> Iterator[dict[str, Any]]:
        raise AssertionError("extractor must not run for a checkpointed page")
        yield

    spec = _spec(iter_records=should_not_extract)
    harness.page_checkpoint = {
        "run_id": "old-run",
        "query_fingerprint": _fingerprint(spec),
        "search_items": 1,
        "record_counts": {"notice": 2},
        "error_counts": {},
    }

    result = _run(spec)

    assert result == {
        "status": "completed",
        "pages": 1,
        "search_items": 1,
        "errors": 0,
    }
    assert "raw" not in harness.events
    assert "bronze" not in harness.events
    assert "checkpoint" not in harness.events
    assert harness.success["record_counts"] == {"notice": 2}  # type: ignore[index]


def test_force_ignores_success_and_page_checkpoint(harness: Harness) -> None:
    spec = _spec()
    fingerprint = _fingerprint(spec)
    harness.success = {"run_id": "old-run", "query_fingerprint": fingerprint}
    harness.page_checkpoint = {
        "run_id": "old-run",
        "query_fingerprint": fingerprint,
        "search_items": 1,
    }

    result = _run(spec, force=True)

    assert result["status"] == "completed"
    assert "raw" in harness.events
    assert "bronze" in harness.events
    assert "checkpoint" in harness.events


def test_record_error_completes_day_with_errors(harness: Harness) -> None:
    def records(
        *, errors: list[dict[str, Any]], stats: Any, **_: Any
    ) -> Iterator[dict[str, Any]]:
        stats.error("notice_detail")
        errors.append({"stage": "plan_detail"})
        yield {"_resource": "test_notice", "_source_id": "good-item"}

    result = _run(_spec(iter_records=records))

    assert result["status"] == "completed_with_errors"
    assert result["errors"] == 1
    assert harness.saved_errors == [{"stage": "plan_detail"}]
    assert harness.checkpoints[0]["error_counts"] == {"notice_detail": 1}


def test_search_failure_writes_error_failed_manifest_and_releases_lock(
    harness: Harness,
) -> None:
    def fail_search(**_: Any) -> dict[str, Any]:
        raise TimeoutError("search timeout")

    with pytest.raises(TimeoutError, match="search timeout"):
        _run(_spec(fetch_page=fail_search))

    assert harness.saved_errors[0]["stage"] == "search_page"
    assert harness.manifests[-1]["status"] == "failed"
    assert harness.events[-1] == "release"
    assert "success" not in harness.events


def test_search_limit_fails_before_raw_and_extraction(harness: Harness) -> None:
    with pytest.raises(daily_runner.SearchResultLimitError):
        _run(_spec(fetch_page=lambda **_: _page(total=10_000)))

    assert harness.saved_errors[0]["stage"] == "search_limit"
    assert "raw" not in harness.events
    assert "bronze" not in harness.events
    assert harness.events[-1] == "release"


def test_raw_storage_failure_prevents_bronze_and_checkpoint(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_raw(**_: Any) -> str:
        harness.events.append("raw")
        raise OSError("storage unavailable")

    monkeypatch.setattr(daily_runner, "save_raw_search_page", fail_raw)

    with pytest.raises(OSError, match="storage unavailable"):
        _run(_spec())

    assert harness.saved_errors[0]["stage"] == "raw_storage"
    assert "bronze" not in harness.events
    assert "checkpoint" not in harness.events
    assert harness.events[-1] == "release"


def test_bronze_failure_keeps_raw_uri_and_does_not_checkpoint(harness: Harness) -> None:
    harness.pipeline.error = RuntimeError("load failed")

    with pytest.raises(RuntimeError, match="load failed"):
        _run(_spec())

    bronze_error = harness.saved_errors[-1]
    assert bronze_error["stage"] == "bronze_load"
    assert bronze_error["retry_input"]["raw_search_uri"] == "s3://raw/page.json.gz"
    assert "checkpoint" not in harness.events
    assert harness.manifests[-1]["status"] == "failed"
    assert harness.events[-1] == "release"
