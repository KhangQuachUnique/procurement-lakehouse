from datetime import UTC, datetime

import pytest
from dagster import DefaultScheduleStatus, build_schedule_context

from procurement.orchestration.definitions import lakehouse_end_to_end_schedule


@pytest.mark.parametrize("year", [2026, 2027, 2028, 2035])
def test_end_to_end_schedule_advances_across_future_years_without_enabling_it(year):
    assert lakehouse_end_to_end_schedule.default_status is DefaultScheduleStatus.STOPPED
    tick = datetime(year, 1, 1, 3, tzinfo=UTC)
    requests = list(
        lakehouse_end_to_end_schedule(build_schedule_context(scheduled_execution_time=tick))
    )
    assert len(requests) == 1
    assert requests[0].partition_key == f"{year - 1}-12-31"
    configs = requests[0].run_config["ops"]
    assert all(item["config"].get("refresh", True) for item in configs.values())
    assert configs["silver_khlcnt"]["config"]["readiness_evidence"] is True
