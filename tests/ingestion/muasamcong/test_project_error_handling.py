from datetime import date
from typing import Any

import httpx

from procurement.common.errors import ErrorCode, ErrorStage
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.project.extractor import iter_project_records
from procurement.models.errors import ErrorRecord

PROJECT = ResourceIdentity("muasamcong", "project")


class StubClient:
    def get_project_detail(self, project_id: str) -> dict[str, Any]:
        if project_id == "bad-project":
            raise httpx.ReadTimeout("timeout")
        if project_id == "fallback-version":
            return {
                "id": project_id,
                "pversion": "07",
                "linkedPublishPlan": [{"planNo": "PL2600000001"}],
            }
        return {
            "id": project_id,
            "projectDTO": {"version": "03"},
            "pversion": "02",
            "linkedPublishPlan": [{"planNo": "PL2600000002"}],
        }


def test_project_failure_is_typed_and_success_preserves_raw_payload() -> None:
    errors: list[ErrorRecord] = []
    stats = PageStats()

    records = list(
        iter_project_records(
            StubClient(),
            identity=PROJECT,
            search_items=[
                {"id": "bad-project"},
                {"id": "good-project"},
            ],
            run_id="run-1",
            source_date=date(2026, 9, 10),
            search_page=2,
            errors=errors,
            stats=stats,
        )
    )

    assert [item.record.source_id for item in records] == ["good-project"]
    assert records[0].record.source_version == "03"
    assert records[0].table == "project_detail"
    assert records[0].record.payload["linkedPublishPlan"] == [{"planNo": "PL2600000002"}]
    assert stats.record_counts["project"] == 1
    assert stats.error_counts["project"] == 1

    assert len(errors) == 1
    assert errors[0].stage is ErrorStage.PROJECT_DETAIL
    assert errors[0].code is ErrorCode.SOURCE_TIMEOUT
    assert errors[0].source_id == "bad-project"


def test_project_version_falls_back_to_top_level_pversion() -> None:
    records = list(
        iter_project_records(
            StubClient(),
            identity=PROJECT,
            search_items=[{"id": "fallback-version"}],
            run_id="run-1",
            source_date=date(2026, 9, 10),
            search_page=0,
            errors=[],
            stats=PageStats(),
        )
    )

    assert records[0].record.source_version == "07"


def test_missing_project_id_is_invalid_source_response() -> None:
    errors: list[ErrorRecord] = []

    records = list(
        iter_project_records(
            StubClient(),
            identity=PROJECT,
            search_items=[{}],
            run_id="run-1",
            source_date=date(2026, 9, 10),
            search_page=0,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert records == []
    assert len(errors) == 1
    assert errors[0].stage is ErrorStage.PROJECT_DETAIL
    assert errors[0].code is ErrorCode.SOURCE_INVALID_RESPONSE
    assert errors[0].source_id is None
