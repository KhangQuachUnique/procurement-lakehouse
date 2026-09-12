from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import pytest

from procurement.common.errors import ErrorCode, ErrorStage
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine import daily_runner
from procurement.ingestion.engine.models import BronzeItem, ResourceSpec
from procurement.models.bronze import BronzeRecord
from procurement.models.control import DayStatus, PageStatus
from procurement.models.errors import ErrorRecord

SOURCE_DATE = date(2026, 9, 10)
IDENTITY = ResourceIdentity("test-source", "test-resource")


class FakePipeline:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.loads: list[tuple[str, list[BronzeRecord]]] = []
        self.fail = False

    def run(self, resource: tuple[str, list[BronzeRecord]]) -> None:
        self.events.append("bronze")
        if self.fail:
            raise RuntimeError("load failed")
        self.loads.append(resource)


class Harness:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.events: list[str] = []
        self.days = []
        self.pages = []
        self.errors: list[ErrorRecord] = []
        self.pipeline = FakePipeline(self.events)

        monkeypatch.setattr(
            daily_runner, "acquire_daily_lock", lambda *_: self.events.append("lock")
        )
        monkeypatch.setattr(
            daily_runner, "refresh_daily_lock", lambda *_: self.events.append("refresh")
        )
        monkeypatch.setattr(
            daily_runner, "release_daily_lock", lambda *_: self.events.append("release")
        )
        monkeypatch.setattr(daily_runner, "write_day_manifest", self._write_day)
        monkeypatch.setattr(daily_runner, "write_page_manifest", self._write_page)
        monkeypatch.setattr(daily_runner, "save_error_records", self._save_errors)
        monkeypatch.setattr(daily_runner, "create_bronze_destination", lambda **_: object())
        monkeypatch.setattr(
            daily_runner,
            "create_bronze_resource",
            lambda records, *, name: (name, list(records)),
        )
        monkeypatch.setattr(daily_runner.dlt, "pipeline", lambda **_: self.pipeline)

    def _write_day(self, _fs: Any, _identity: Any, manifest: Any) -> str:
        self.days.append(manifest)
        self.events.append(f"day:{manifest.status.value}")
        return "s3://day"

    def _write_page(self, _fs: Any, _identity: Any, manifest: Any) -> str:
        self.pages.append(manifest)
        self.events.append(f"page:{manifest.status.value}")
        return "s3://page"

    def _save_errors(self, **kwargs: Any) -> str:
        self.errors.extend(kwargs["records"])
        self.events.append("errors")
        return "s3://errors"


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Harness:
    return Harness(monkeypatch)


def _page(content: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    items = [{"id": "item-1"}] if content is None else content
    return {"page": {"content": items, "totalElements": len(items), "last": True}}


def _bronze_item(source_id: str = "item-1") -> BronzeItem:
    return BronzeItem(
        table="test_notice",
        record=BronzeRecord(
            source_id=source_id,
            run_id="run-1",
            source_date=SOURCE_DATE,
            ingested_at=datetime(2026, 9, 10, tzinfo=UTC),
            content_hash="hash",
            payload={"id": source_id},
        ),
    )


def _spec(*, iter_records: Any | None = None, fetch_page: Any | None = None) -> ResourceSpec:
    def fetch(**_: Any) -> dict[str, Any]:
        return _page()

    def records(*, stats: Any, **_: Any) -> Iterator[BronzeItem]:
        stats.record("notice")
        yield _bronze_item()

    return ResourceSpec(
        identity=IDENTITY,
        pipeline_name="test_pipeline",
        dataset_name="test_dataset",
        fetch_page=fetch_page or fetch,
        iter_records=iter_records or records,
    )


def _run(spec: ResourceSpec, *, run_id: str = "run-1"):
    return daily_runner.run_daily_resource(
        fs=object(),  # type: ignore[arg-type]
        spec=spec,
        run_id=run_id,
        source_date=SOURCE_DATE,
        page_size=50,
    )


def test_success_commits_day_only_after_page_bronze_succeeds(harness: Harness) -> None:
    result = _run(_spec())

    assert result == {
        "status": "success",
        "pages": 1,
        "search_items": 1,
        "bronze_records": 1,
        "errors": 0,
    }
    assert harness.pages[-1].status is PageStatus.SUCCESS
    assert harness.days[-1].status is DayStatus.SUCCESS
    assert harness.pipeline.loads[0][0] == "test_notice"
    assert harness.events.index("bronze") < len(harness.events) - 2


def test_any_detail_error_fails_whole_day_and_skips_bronze_write(harness: Harness) -> None:
    def records(*, errors: list[ErrorRecord], stats: Any, **_: Any):
        stats.record("notice")
        stats.error("notice")
        errors.append(
            ErrorRecord(
                error_id="err-1",
                run_id="run-1",
                source=IDENTITY.source,
                resource=IDENTITY.resource,
                source_date=SOURCE_DATE,
                page_number=0,
                stage=ErrorStage.PLAN_DETAIL,
                code=ErrorCode.SOURCE_TIMEOUT,
                source_id="bad",
                error_type="ReadTimeout",
                message="Source request timed out",
                occurred_at=datetime(2026, 9, 10, tzinfo=UTC),
            )
        )
        yield _bronze_item("good")

    result = _run(_spec(iter_records=records))

    assert result["status"] == "failed"
    assert harness.days[-1].status is DayStatus.FAILED
    assert harness.pages[-1].status is PageStatus.FAILED
    assert harness.errors[0].error_id == "err-1"
    assert "bronze" not in harness.events


def test_bronze_load_error_fails_day(harness: Harness) -> None:
    harness.pipeline.fail = True

    result = _run(_spec())

    assert result["status"] == "failed"
    assert harness.days[-1].status is DayStatus.FAILED
    assert harness.errors[-1].stage is ErrorStage.BRONZE_LOAD


def test_new_run_starts_same_source_date_from_page_zero_again(harness: Harness) -> None:
    calls: list[int] = []

    def fetch(*, page_number: int, **_: Any) -> dict[str, Any]:
        calls.append(page_number)
        return _page()

    spec = _spec(fetch_page=fetch)
    _run(spec, run_id="run-a")
    _run(spec, run_id="run-b")

    assert calls == [0, 0]
    assert harness.events.count("bronze") == 2
