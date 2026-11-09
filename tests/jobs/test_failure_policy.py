from datetime import date
from types import SimpleNamespace

import pytest

from procurement.common.catalog import get_resource
from procurement.jobs import failures
from procurement.models.control import DayStatus


@pytest.mark.parametrize(
    "day_status,stage,http_status,count,expected",
    [
        (DayStatus.FAILED, "plan_detail", 404, 1, "source_failure"),
        (DayStatus.FAILED, "search_page", 503, 1, "source_failure"),
        (DayStatus.FAILED, "pagination", None, 1, "source_failure"),
        (DayStatus.FAILED, "plan_detail", 401, 1, "authentication_failure"),
        (DayStatus.FAILED, "search_page", 403, 1, "authentication_failure"),
        (DayStatus.FAILED, "bronze_load", None, 1, "storage_or_internal_failure"),
        (DayStatus.FAILED, "internal", None, 1, "storage_or_internal_failure"),
        (DayStatus.FAILED, None, None, 1, "unconfirmed_failure"),
        (DayStatus.FAILED, "plan_detail", 404, 2, "unconfirmed_failure"),
        (DayStatus.SUCCESS, None, None, 0, "unconfirmed_failure"),
        (DayStatus.RUNNING, None, None, 0, "unconfirmed_failure"),
        (None, None, None, 0, "unconfirmed_failure"),
    ],
)
def test_continue_requires_confirmed_source_failure(
    monkeypatch, day_status, stage, http_status, count, expected
):
    day = SimpleNamespace(status=day_status, error_count=count) if day_status else None
    monkeypatch.setattr(failures, "read_day_manifest", lambda *_: day)
    errors = [SimpleNamespace(stage=stage, http_status=http_status)] if stage else []
    monkeypatch.setattr(failures, "list_error_records", lambda *_, **__: errors)
    assert (
        failures.classify_day_failure(
            object(), get_resource("project").identity, "run", date(2025, 1, 1)
        )
        == expected
    )
