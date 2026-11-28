from datetime import UTC, datetime

import pytest
from dagster import DefaultScheduleStatus, build_schedule_context

from procurement.orchestration.definitions import bronze_daily_schedule
from procurement.orchestration.spark_lakehouse import spark_lakehouse_daily_schedule


@pytest.mark.parametrize("year", [2026, 2027, 2028, 2035])
def test_bronze_daily_schedule_advances_across_future_years_without_enabling_it(year):
    assert bronze_daily_schedule.default_status is DefaultScheduleStatus.STOPPED
    tick = datetime(year, 1, 1, 3, tzinfo=UTC)
    requests = list(
        bronze_daily_schedule(build_schedule_context(scheduled_execution_time=tick))
    )
    assert len(requests) == 3
    assert [r.partition_key for r in requests] == [
        f"{year - 1}-12-29",
        f"{year - 1}-12-30",
        f"{year - 1}-12-31",
    ]
    for req in requests:
        configs = req.run_config["ops"]
        assert all(item["config"].get("refresh", True) for item in configs.values())


def test_spark_lakehouse_daily_schedule_defaults_stopped():
    assert spark_lakehouse_daily_schedule.default_status is DefaultScheduleStatus.STOPPED
    assert spark_lakehouse_daily_schedule.cron_schedule == "30 8 * * *"
