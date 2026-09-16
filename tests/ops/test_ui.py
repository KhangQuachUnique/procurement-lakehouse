from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient

from procurement.api.main import app
from procurement.api.ops.dependencies import get_ops_service
from procurement.models.control import DayStatus, PageStatus, RunStatus
from procurement.ops.models import (
    AttemptDetail,
    AttemptSummary,
    DateDetail,
    DateIngestionStatus,
    DateSummary,
    ErrorSummary,
    PageSummary,
    RunDetail,
    RunSummary,
)

RUN_ID = "run-20260915-001"
SOURCE_DATE = date(2026, 9, 14)
NOW = datetime(2026, 9, 15, 2, 30, tzinfo=UTC)


class FakeOpsService:
    def identity(self, resource: str, *, source: str) -> object:
        if source != "muasamcong" or resource != "notify_contractor":
            raise ValueError("unsupported")
        return object()

    def _attempt(self) -> AttemptSummary:
        return AttemptSummary(
            run_id=RUN_ID,
            source="muasamcong",
            resource="notify_contractor",
            source_date=SOURCE_DATE,
            status=DayStatus.SUCCESS,
            expected_pages=2,
            completed_pages=2,
            search_items=0,
            bronze_records=0,
            error_count=0,
            started_at=NOW,
            completed_at=NOW,
            duration_seconds=3.5,
        )

    def _run(self) -> RunSummary:
        return RunSummary(
            run_id=RUN_ID,
            source="muasamcong",
            resource="notify_contractor",
            start_date=SOURCE_DATE,
            end_date=SOURCE_DATE,
            status=RunStatus.SUCCESS,
            total_dates=1,
            success_dates=1,
            failed_dates=0,
            started_at=NOW,
            completed_at=NOW,
            duration_seconds=3.5,
        )

    def list_runs(self, **_: object) -> list[RunSummary]:
        return [self._run()]

    def list_dates(
        self,
        resource: str,
        *,
        start_date: date,
        end_date: date,
        **_: object,
    ) -> list[DateSummary]:
        assert resource == "notify_contractor"
        assert start_date == date(2026, 1, 1)
        assert end_date == date(2026, 12, 31)
        items: list[DateSummary] = []
        cursor = end_date
        while cursor >= start_date:
            success = cursor == SOURCE_DATE
            items.append(
                DateSummary(
                    source="muasamcong",
                    resource=resource,
                    source_date=cursor,
                    status=(
                        DateIngestionStatus.SUCCESS
                        if success
                        else DateIngestionStatus.NO_ATTEMPT
                    ),
                    attempt_count=1 if success else 0,
                    effective_run_id=RUN_ID if success else None,
                    latest_run_id=RUN_ID if success else None,
                    latest_attempt_status=DayStatus.SUCCESS if success else None,
                    bronze_records=0,
                    error_count=0,
                    last_attempt_at=NOW if success else None,
                )
            )
            cursor = date.fromordinal(cursor.toordinal() - 1)
        return items

    def get_date(self, resource: str, source_date: date, **_: object) -> DateDetail:
        assert resource == "notify_contractor"
        if source_date == SOURCE_DATE:
            return DateDetail(
                source="muasamcong",
                resource=resource,
                source_date=source_date,
                status=DateIngestionStatus.SUCCESS,
                effective_run_id=RUN_ID,
                attempts=[self._attempt()],
            )
        return DateDetail(
            source="muasamcong",
            resource=resource,
            source_date=source_date,
            status=DateIngestionStatus.NO_ATTEMPT,
            effective_run_id=None,
            attempts=[],
        )

    def get_run(self, run_id: str, **_: object) -> RunDetail | None:
        if run_id != RUN_ID:
            return None
        return RunDetail(run=self._run(), attempts=[self._attempt()])

    def get_attempt(self, run_id: str, source_date: date, **_: object) -> AttemptDetail | None:
        if run_id != RUN_ID or source_date != SOURCE_DATE:
            return None
        error = ErrorSummary(
            error_id="err-1",
            run_id=RUN_ID,
            source="muasamcong",
            resource="notify_contractor",
            source_date=SOURCE_DATE,
            page_number=1,
            stage="detail_fetch",
            source_id="IB2600000001",
            error_type="HTTPStatusError",
            message="upstream returned 500",
            http_status=500,
            occurred_at=NOW,
        )
        return AttemptDetail(
            attempt=self._attempt(),
            pages=[
                PageSummary(
                    page_number=1,
                    page_size=10,
                    status=PageStatus.SUCCESS,
                    search_items=0,
                    bronze_records=0,
                    error_count=0,
                    started_at=NOW,
                    completed_at=NOW,
                    duration_seconds=1.5,
                )
            ],
            errors=[error],
        )

    def list_errors(self, **_: object) -> list[ErrorSummary]:
        detail = self.get_attempt(RUN_ID, SOURCE_DATE)
        assert detail is not None
        return detail.errors


@pytest.fixture
def client() -> TestClient:
    app.dependency_overrides[get_ops_service] = lambda: FakeOpsService()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_root_redirects_to_runs(client: TestClient) -> None:
    response = client.get("/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/ops"


def test_runs_is_primary_ops_view(client: TestClient) -> None:
    response = client.get("/ops")

    assert response.status_code == 200
    assert "<h1>Runs</h1>" in response.text
    assert f"/ops/runs/{RUN_ID}" in response.text
    assert "/ops/calendar" in response.text


def test_calendar_renders_full_year_heatmap_with_hover_details(client: TestClient) -> None:
    response = client.get("/ops/calendar?resource=notify_contractor&year=2026")

    assert response.status_code == 200
    assert 'class="heat-day success"' in response.text
    assert 'class="heat-day no_attempt"' in response.text
    assert "data-tip=" in response.text
    assert "0 records" in response.text
    assert "NO ATTEMPT" in response.text
    assert "year=2025" in response.text
    assert "year=2027" in response.text
    assert "Jan" in response.text
    assert "Dec" in response.text


def test_calendar_date_drills_into_attempt(client: TestClient) -> None:
    response = client.get(f"/ops/calendar/notify_contractor/{SOURCE_DATE}")

    assert response.status_code == 200
    assert f"/ops/attempts/{RUN_ID}/{SOURCE_DATE}" in response.text
    assert "success" in response.text


def test_run_and_attempt_details_are_navigable(client: TestClient) -> None:
    run = client.get(f"/ops/runs/{RUN_ID}")
    attempt = client.get(f"/ops/attempts/{RUN_ID}/{SOURCE_DATE}")

    assert run.status_code == 200
    assert SOURCE_DATE.isoformat() in run.text
    assert f"/ops/attempts/{RUN_ID}/{SOURCE_DATE}" in run.text
    assert attempt.status_code == 200
    assert "detail_fetch" in attempt.text
    assert "upstream returned 500" in attempt.text
    assert attempt.text.index("<h2>Errors</h2>") < attempt.text.index("<h2>Pages</h2>")


def test_legacy_resource_routes_redirect_to_calendar(client: TestClient) -> None:
    resource = client.get("/ops/resources/notify_contractor", follow_redirects=False)
    source_date = client.get(
        f"/ops/resources/notify_contractor/dates/{SOURCE_DATE}",
        follow_redirects=False,
    )

    assert resource.status_code == 307
    assert resource.headers["location"].startswith("/ops/calendar?")
    assert source_date.status_code == 307
    assert source_date.headers["location"].startswith(
        f"/ops/calendar/notify_contractor/{SOURCE_DATE}"
    )


def test_missing_run_and_attempt_render_html_404(client: TestClient) -> None:
    run = client.get("/ops/runs/missing")
    attempt = client.get(f"/ops/attempts/missing/{SOURCE_DATE}")

    assert run.status_code == 404
    assert "Run not found" in run.text
    assert attempt.status_code == 404
    assert "Attempt not found" in attempt.text
