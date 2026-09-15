from datetime import UTC, date, datetime
from typing import Any

from procurement.models.control import (
    DayManifest,
    DayStatus,
    PageManifest,
    PageStatus,
    RunManifest,
    RunStatus,
)
from procurement.models.errors import ErrorRecord
from procurement.ops.models import DateIngestionStatus, ResourceHealth
from procurement.ops.service import OpsService

NOW = datetime(2026, 9, 15, 2, 0, tzinfo=UTC)


def _day(run_id: str, source_date: date, status: DayStatus, *, errors: int = 0) -> DayManifest:
    return DayManifest(
        run_id=run_id,
        source="muasamcong",
        resource="khlcnt",
        source_date=source_date,
        status=status,
        expected_pages=2,
        completed_pages=2 if status is DayStatus.SUCCESS else 1,
        search_items=20,
        bronze_records=20 if status is DayStatus.SUCCESS else 0,
        error_count=errors,
        started_at=NOW,
        completed_at=NOW,
    )


class StubControl:
    def __init__(self) -> None:
        self.attempts = [
            _day("run-a", date(2026, 9, 12), DayStatus.FAILED, errors=1),
            _day("run-b", date(2026, 9, 12), DayStatus.SUCCESS),
            _day("run-c", date(2026, 9, 13), DayStatus.FAILED, errors=2),
        ]
        self.run = RunManifest(
            run_id="run-b",
            source="muasamcong",
            resource="khlcnt",
            start_date=date(2026, 9, 12),
            end_date=date(2026, 9, 12),
            status=RunStatus.SUCCESS,
            total_dates=1,
            success_dates=1,
            failed_dates=0,
            started_at=NOW,
            completed_at=NOW,
        )

    def list_attempts(self, _identity: Any, **kwargs: Any) -> list[DayManifest]:
        values = list(self.attempts)
        if kwargs.get("run_id") is not None:
            values = [item for item in values if item.run_id == kwargs["run_id"]]
        if kwargs.get("source_date") is not None:
            values = [item for item in values if item.source_date == kwargs["source_date"]]
        if kwargs.get("start_date") is not None:
            values = [item for item in values if item.source_date >= kwargs["start_date"]]
        if kwargs.get("end_date") is not None:
            values = [item for item in values if item.source_date <= kwargs["end_date"]]
        return sorted(values, key=lambda item: item.started_at, reverse=True)

    def get_run(self, identity: Any, run_id: str) -> RunManifest | None:
        if identity.resource == "khlcnt" and run_id == "run-b":
            return self.run
        return None

    def get_attempt(self, _identity: Any, *, run_id: str, source_date: date) -> DayManifest | None:
        return next(
            (
                item
                for item in self.attempts
                if item.run_id == run_id and item.source_date == source_date
            ),
            None,
        )

    def list_pages(self, _identity: Any, **_kwargs: Any) -> list[PageManifest]:
        return [
            PageManifest(
                run_id="run-b",
                source_date=date(2026, 9, 12),
                page_number=0,
                page_size=50,
                status=PageStatus.SUCCESS,
                search_items=20,
                bronze_records=20,
                started_at=NOW,
                completed_at=NOW,
            )
        ]


class StubErrors:
    def list(self, identity: Any, **kwargs: Any) -> list[ErrorRecord]:
        if identity.resource != "khlcnt" or kwargs.get("run_id") != "run-b":
            return []
        return [
            ErrorRecord(
                error_id="err-1",
                run_id="run-b",
                source="muasamcong",
                resource="khlcnt",
                source_date=date(2026, 9, 12),
                page_number=0,
                stage="result_detail",
                error_type="ReadTimeout",
                message="timeout",
                occurred_at=NOW,
            )
        ]


def _service() -> OpsService:
    return OpsService(StubControl(), StubErrors())  # type: ignore[arg-type]


def test_date_projection_uses_successful_attempt_as_effective() -> None:
    dates = _service().list_dates(
        "khlcnt",
        start_date=date(2026, 9, 12),
        end_date=date(2026, 9, 14),
    )

    assert [item.status for item in dates] == [
        DateIngestionStatus.MISSING,
        DateIngestionStatus.FAILED,
        DateIngestionStatus.SUCCESS,
    ]
    assert dates[-1].effective_run_id == "run-b"
    assert dates[-1].attempt_count == 2


def test_resource_health_marks_latest_failed_date_as_failed(monkeypatch) -> None:
    monkeypatch.setattr(OpsService, "_today_vn", staticmethod(lambda: date(2026, 9, 15)))

    summary = _service().get_resource_summary("khlcnt")

    assert summary.health is ResourceHealth.FAILED
    assert summary.latest_source_date == date(2026, 9, 13)
    assert summary.latest_success_source_date == date(2026, 9, 12)
    assert summary.unresolved_failed_dates == 1


def test_attempt_detail_contains_pages_and_errors() -> None:
    detail = _service().get_attempt("run-b", date(2026, 9, 12))

    assert detail is not None
    assert detail.attempt.run_id == "run-b"
    assert detail.pages[0].page_number == 0
    assert detail.errors[0].error_type == "ReadTimeout"


def test_run_lookup_finds_resource_without_resource_path_parameter() -> None:
    detail = _service().get_run("run-b")

    assert detail is not None
    assert detail.run.resource == "khlcnt"
    assert [item.run_id for item in detail.attempts] == ["run-b"]
