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
    OpsOverview,
    PageSummary,
    ResourceHealth,
    ResourceSummary,
    RunDetail,
    RunSummary,
)

RUN_ID = "run-20260915-001"
SOURCE_DATE = date(2026, 9, 14)
NOW = datetime(2026, 9, 15, 2, 30, tzinfo=UTC)


class FakeOpsService:
    def _resource(self) -> ResourceSummary:
        return ResourceSummary(
            source="muasamcong",
            resource="notify_contractor",
            health=ResourceHealth.HEALTHY,
            latest_source_date=SOURCE_DATE,
            latest_success_source_date=SOURCE_DATE,
            freshness_days=1,
            unresolved_failed_dates=0,
            total_attempts=1,
        )

    def _attempt(self) -> AttemptSummary:
        return AttemptSummary(
            run_id=RUN_ID,
            source="muasamcong",
            resource="notify_contractor",
            source_date=SOURCE_DATE,
            status=DayStatus.SUCCESS,
            expected_pages=2,
            completed_pages=2,
            search_items=20,
            bronze_records=20,
            error_count=1,
            started_at=NOW,
            completed_at=NOW,
            duration_seconds=3.5,
        )

    def overview(self, *, source: str) -> OpsOverview:
        return OpsOverview(generated_at=NOW, resources=[self._resource()])

    def get_resource_summary(self, resource: str, *, source: str) -> ResourceSummary:
        assert resource == "notify_contractor"
        return self._resource()

    def list_dates(self, resource: str, **_: object) -> list[DateSummary]:
        assert resource == "notify_contractor"
        return [
            DateSummary(
                source="muasamcong",
                resource="notify_contractor",
                source_date=SOURCE_DATE,
                status=DateIngestionStatus.SUCCESS,
                attempt_count=1,
                effective_run_id=RUN_ID,
                latest_run_id=RUN_ID,
                latest_attempt_status=DayStatus.SUCCESS,
                bronze_records=20,
                error_count=1,
                last_attempt_at=NOW,
            )
        ]

    def get_date(self, resource: str, source_date: date, **_: object) -> DateDetail:
        assert resource == "notify_contractor"
        assert source_date == SOURCE_DATE
        return DateDetail(
            source="muasamcong",
            resource="notify_contractor",
            source_date=SOURCE_DATE,
            status=DateIngestionStatus.SUCCESS,
            effective_run_id=RUN_ID,
            attempts=[self._attempt()],
        )

    def get_run(self, run_id: str, **_: object) -> RunDetail | None:
        if run_id != RUN_ID:
            return None
        return RunDetail(
            run=RunSummary(
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
            ),
            attempts=[self._attempt()],
        )

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
                    search_items=10,
                    bronze_records=10,
                    error_count=1,
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


def test_root_redirects_to_ops(client: TestClient) -> None:
    response = client.get("/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/ops"


def test_overview_links_into_resource_timeline(client: TestClient) -> None:
    response = client.get("/ops")

    assert response.status_code == 200
    assert "Ingestion overview" in response.text
    assert "/ops/resources/notify_contractor" in response.text
    assert "/ops/errors" in response.text


def test_drilldown_pages_are_navigable(client: TestClient) -> None:
    resource = client.get("/ops/resources/notify_contractor")
    date_detail = client.get(f"/ops/resources/notify_contractor/dates/{SOURCE_DATE}")
    run = client.get(f"/ops/runs/{RUN_ID}")
    attempt = client.get(f"/ops/attempts/{RUN_ID}/{SOURCE_DATE}")
    errors = client.get("/ops/errors")

    assert resource.status_code == 200
    assert f"/ops/resources/notify_contractor/dates/{SOURCE_DATE}" in resource.text
    assert date_detail.status_code == 200
    assert f"/ops/attempts/{RUN_ID}/{SOURCE_DATE}" in date_detail.text
    assert run.status_code == 200
    assert SOURCE_DATE.isoformat() in run.text
    assert attempt.status_code == 200
    assert "detail_fetch" in attempt.text
    assert "upstream returned 500" in attempt.text
    assert errors.status_code == 200
    assert f"/ops/runs/{RUN_ID}" in errors.text


def test_missing_run_and_attempt_render_html_404(client: TestClient) -> None:
    run = client.get("/ops/runs/missing")
    attempt = client.get(f"/ops/attempts/missing/{SOURCE_DATE}")

    assert run.status_code == 404
    assert "Run not found" in run.text
    assert attempt.status_code == 404
    assert "Attempt not found" in attempt.text
