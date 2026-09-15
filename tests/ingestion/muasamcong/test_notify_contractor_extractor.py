from datetime import date
from typing import Any

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.notify_contractor.extractor import (
    REOFFER_TABLE,
    ROUTING_STAGE,
    STANDARD_TABLE,
    iter_notify_contractor_records,
)
from procurement.models.errors import ErrorRecord

IDENTITY = ResourceIdentity("muasamcong", "notify_contractor")
SOURCE_DATE = date(2026, 9, 14)


class StubApi:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def get_standard_detail(self, notice_id: str) -> dict[str, Any]:
        self.calls.append(("standard", notice_id))
        if notice_id == "standard-timeout":
            raise httpx.ReadTimeout("standard detail timeout")
        return {
            "bidoNotifyContractorM": {
                "id": notice_id,
                "notifyNo": "IB2600458347",
                "notifyVersion": "00",
                "bidNo": "BP2600623185",
            },
            "bidoBidStatus": {"status": "PUB_KQLCNT"},
        }

    def get_reoffer_detail(self, notice_id: str) -> dict[str, Any]:
        self.calls.append(("reoffer", notice_id))
        return {
            "id": notice_id,
            "notifyNo": "IB2600535918",
            "notifyVersion": "01",
            "reofferNo": "IB2600535918",
            "reofferVersion": "01",
            "priceInit": 153000000,
        }


def test_standard_lifecycle_steps_share_standard_detail_api() -> None:
    api = StubApi()
    errors: list[ErrorRecord] = []

    records = list(
        iter_notify_contractor_records(
            api,
            identity=IDENTITY,
            search_items=[
                {
                    "id": "standard-1",
                    "notifyNo": "IB2600458347",
                    "notifyVersion": "00",
                    "stepCode": "notify-contractor-step-4-kqlcnt",
                }
            ],
            run_id="run-1",
            source_date=SOURCE_DATE,
            search_page=0,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert api.calls == [("standard", "standard-1")]
    assert errors == []
    assert len(records) == 1
    assert records[0].table == STANDARD_TABLE
    assert records[0].record.source_id == "IB2600458347"
    assert records[0].record.source_version == "00"
    assert records[0].record.payload["bidoBidStatus"]["status"] == "PUB_KQLCNT"


def test_reoffer_step_routes_to_reoffer_detail_and_preserves_raw_payload() -> None:
    api = StubApi()

    records = list(
        iter_notify_contractor_records(
            api,
            identity=IDENTITY,
            search_items=[
                {
                    "id": "reoffer-1",
                    "notifyNo": "IB2600535918",
                    "notifyVersion": "01",
                    "stepCode": "reoffer-price-step-1",
                }
            ],
            run_id="run-1",
            source_date=SOURCE_DATE,
            search_page=1,
            errors=[],
            stats=PageStats(),
        )
    )

    assert api.calls == [("reoffer", "reoffer-1")]
    assert records[0].table == REOFFER_TABLE
    assert records[0].record.source_id == "IB2600535918"
    assert records[0].record.source_version == "01"
    assert records[0].record.payload["priceInit"] == 153000000


def test_unknown_step_is_routing_error_and_does_not_guess_endpoint() -> None:
    api = StubApi()
    errors: list[ErrorRecord] = []

    records = list(
        iter_notify_contractor_records(
            api,
            identity=IDENTITY,
            search_items=[
                {
                    "id": "unknown-1",
                    "notifyNo": "IB-UNKNOWN",
                    "stepCode": "another-workflow-step-1",
                }
            ],
            run_id="run-1",
            source_date=SOURCE_DATE,
            search_page=2,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert records == []
    assert api.calls == []
    assert len(errors) == 1
    assert errors[0].stage == ROUTING_STAGE
    assert errors[0].error_type == "UnsupportedNotifyWorkflowError"
    assert errors[0].message == "Unsupported stepCode: another-workflow-step-1"
    assert errors[0].source_id == "IB-UNKNOWN"


def test_standard_detail_error_keeps_original_diagnostic_message() -> None:
    api = StubApi()
    errors: list[ErrorRecord] = []

    records = list(
        iter_notify_contractor_records(
            api,
            identity=IDENTITY,
            search_items=[
                {
                    "id": "standard-timeout",
                    "notifyNo": "IB-TIMEOUT",
                    "stepCode": "notify-contractor-step-1-tbmt",
                }
            ],
            run_id="run-1",
            source_date=SOURCE_DATE,
            search_page=3,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert records == []
    assert len(errors) == 1
    assert errors[0].stage == "standard_detail"
    assert errors[0].error_type == "ReadTimeout"
    assert errors[0].message == "standard detail timeout"
    assert errors[0].source_id == "IB-TIMEOUT"


def test_missing_id_is_routing_error_before_any_detail_call() -> None:
    api = StubApi()
    errors: list[ErrorRecord] = []

    records = list(
        iter_notify_contractor_records(
            api,
            identity=IDENTITY,
            search_items=[
                {
                    "notifyNo": "IB-MISSING-ID",
                    "stepCode": "notify-contractor-step-1-tbmt",
                }
            ],
            run_id="run-1",
            source_date=SOURCE_DATE,
            search_page=4,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert records == []
    assert api.calls == []
    assert errors[0].stage == ROUTING_STAGE
    assert errors[0].error_type == "KeyError"
    assert errors[0].message == "'id'"
    assert errors[0].source_id == "IB-MISSING-ID"
