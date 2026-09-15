from datetime import date
from typing import Any

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.khlcnt.extractor import iter_khlcnt_records
from procurement.models.errors import ErrorRecord

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


def test_plan_failure_keeps_raw_error_and_successful_record_keeps_bronze_contract() -> None:
    errors: list[ErrorRecord] = []

    records = list(
        iter_khlcnt_records(
            StubClient(),
            identity=KHLCNT,
            search_items=[{"id": "bad-plan"}, {"id": "good-plan", "planVersion": "02"}],
            run_id="run-1",
            source_date=date(2026, 9, 10),
            search_page=2,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert [item.record.source_id for item in records] == ["good-plan"]
    assert records[0].record.source_version == "02"
    assert records[0].table == "khlcnt_plan_detail"
    assert len(errors) == 1
    assert errors[0].stage == "plan_detail"
    assert errors[0].error_type == "ReadTimeout"
    assert errors[0].message == "timeout"
    assert errors[0].source_id == "bad-plan"


def test_package_version_is_not_inherited_from_parent_plan() -> None:
    errors: list[ErrorRecord] = []

    records = list(
        iter_khlcnt_records(
            PackageStubClient(),
            identity=KHLCNT,
            search_items=[{"id": "plan-1", "planVersion": "07"}],
            run_id="run-1",
            source_date=date(2026, 9, 10),
            search_page=2,
            errors=errors,
            stats=PageStats(),
        )
    )

    good_package = next(item for item in records if item.record.source_id == "good-package")
    assert good_package.table == "khlcnt_bid_package_detail"
    assert good_package.record.source_version is None
    assert len(errors) == 1
    assert errors[0].stage == "bid_package_detail"
    assert errors[0].error_type == "ReadTimeout"
    assert errors[0].message == "timeout"
    assert errors[0].source_id == "bad-package"
