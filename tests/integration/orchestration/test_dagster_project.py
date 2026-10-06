"""Integration test verifying Dagster asset execution through the refactored IngestionService."""

from datetime import date
from unittest.mock import Mock

import pytest

pytest.importorskip("dagster")
from dagster import materialize

from procurement.ingestion.contracts import MaterializeDayResult
from procurement.orchestration import bronze

pytestmark = pytest.mark.integration


def test_dagster_bronze_project_asset_delegates_to_service(monkeypatch):
    day = date(2025, 1, 1)
    selected = next(a for a in bronze.bronze_assets if a.key.to_user_string() == "bronze_project")

    mock_service = Mock()
    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="project",
        source_date=day,
        reused=False,
        status="success",
        record_count=10,
        file_count=1,
        commit_id=None,
        attempt_id=None,
    )

    from procurement import bootstrap

    monkeypatch.setattr(bootstrap, "bootstrap_services", lambda: (mock_service, None))

    result = materialize(
        [selected],
        resources={"object_storage": Mock()},
        partition_key=str(day),
        run_config={"ops": {"bronze_project": {"config": {"refresh": False}}}},
    )

    assert result.success
    mock_service.materialize_day.assert_called_once()
    req = mock_service.materialize_day.call_args[0][0]
    assert req.resource == "project"
    assert req.source_date == day
    assert not req.refresh
