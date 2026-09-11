from datetime import date
from typing import Any

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.muasamcong.extractors.khlcnt import iter_khlcnt_records

KHLCNT = ResourceIdentity("muasamcong", "khlcnt")


class StubClient:
    def get_plan_detail(self, plan_id: str) -> dict[str, Any]:
        if plan_id == "bad-plan":
            raise httpx.ReadTimeout("timeout")
        return {"id": plan_id, "bidpPlanDetailToProjectList": []}

    def get_bid_package_detail(self, bid_package_id: str) -> dict[str, Any]:
        if bid_package_id == "bad-package":
            raise httpx.ReadTimeout("timeout")
        return {"id": bid_package_id}


class PackageStubClient(StubClient):
    def get_plan_detail(self, plan_id: str) -> dict[str, Any]:
        return {
            "id": plan_id,
            "bidpPlanDetailToProjectList": [
                {"id": "bad-package", "idPlan": plan_id},
                {"id": "good-package", "idPlan": plan_id},
            ],
        }


def test_plan_failure_is_recorded_and_next_plan_continues() -> None:
    errors: list[dict[str, Any]] = []

    records = list(iter_khlcnt_records(
        StubClient(),
        identity=KHLCNT,
        search_items=[{"id": "bad-plan"}, {"id": "good-plan"}],
        run_id="run-1", source_date=date(2026, 9, 10), search_page=2,
        errors=errors, stats=PageStats(),
    ))

    assert [record["_source_id"] for record in records] == ["good-plan"]
    assert len(errors) == 1
    assert errors[0]["stage"] == "plan_detail"
    assert errors[0]["error_code"] == "SOURCE_TIMEOUT"
    assert errors[0]["retry_input"] == {"plan_id": "bad-plan"}


def test_package_failure_is_recorded_and_next_package_continues() -> None:
    errors: list[dict[str, Any]] = []

    records = list(iter_khlcnt_records(
        PackageStubClient(),
        identity=KHLCNT,
        search_items=[{"id": "plan-1"}], run_id="run-1",
        source_date=date(2026, 9, 10), search_page=2, errors=errors,
        stats=PageStats(),
    ))

    assert [record["_source_id"] for record in records] == ["plan-1", "good-package"]
    assert len(errors) == 1
    assert errors[0]["stage"] == "bid_package_detail"
    assert errors[0]["retry_input"] == {
        "plan_id": "plan-1",
        "package_id": "bad-package",
    }
