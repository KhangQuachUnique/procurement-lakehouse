from datetime import date
from typing import Any

import httpx

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.contractor_result.extractor import (
    iter_contractor_result_records,
)
from procurement.models.errors import ErrorRecord

CONTRACTOR_RESULT = ResourceIdentity("muasamcong", "contractor_result")


class StubClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_result_detail(self, result_id: str) -> dict[str, Any]:
        self.calls.append(result_id)
        if result_id == "bad-result":
            raise httpx.ReadTimeout("timeout")
        return {
            "bideContractorInputResultDTO": {
                "id": result_id,
                "resultVersion": "00",
                "notifyNo": "IB2600501725",
                "notifyVersion": "03",
                "bidNo": "BP2600600025",
                "planNo": "PL2600241863",
                "lotResultDTO": [
                    {
                        "lotNo": "BP2600600025",
                        "contractorList": [
                            {
                                "taxCode": "3702797166",
                                "bidWiningPrice": 5_000_000,
                            }
                        ],
                    }
                ],
            },
            "bidpPlanDetailDTO": {
                "planNo": "PL2600241863",
                "bidNo": "BP2600600025",
            },
        }


def test_result_uses_input_result_id_and_preserves_raw_payload() -> None:
    client = StubClient()
    errors: list[ErrorRecord] = []
    stats = PageStats()

    records = list(
        iter_contractor_result_records(
            client,
            identity=CONTRACTOR_RESULT,
            search_items=[
                {
                    "id": "IB2600501725",
                    "notifyNo": "IB2600501725",
                    "notifyVersion": "99",
                    "inputResultId": "result-1",
                }
            ],
            run_id="run-1",
            source_date=date(2026, 9, 1),
            search_page=0,
            errors=errors,
            stats=stats,
        )
    )

    assert client.calls == ["result-1"]
    assert len(records) == 1
    assert records[0].table == "contractor_result_detail"
    assert records[0].record.source_id == "IB2600501725"
    assert records[0].record.source_version == "00"
    assert records[0].record.payload["bideContractorInputResultDTO"]["lotResultDTO"][0][
        "contractorList"
    ][0]["taxCode"] == "3702797166"
    assert records[0].record.payload["bidpPlanDetailDTO"]["planNo"] == "PL2600241863"
    assert errors == []
    assert stats.record_counts["contractor_result"] == 1


def test_timeout_keeps_raw_diagnostic_message() -> None:
    errors: list[ErrorRecord] = []

    records = list(
        iter_contractor_result_records(
            StubClient(),
            identity=CONTRACTOR_RESULT,
            search_items=[
                {
                    "notifyNo": "IB2600000001",
                    "inputResultId": "bad-result",
                }
            ],
            run_id="run-1",
            source_date=date(2026, 9, 1),
            search_page=2,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert records == []
    assert len(errors) == 1
    assert errors[0].stage == "result_detail"
    assert errors[0].source_id == "IB2600000001"
    assert errors[0].error_type == "ReadTimeout"
    assert errors[0].message == "timeout"


def test_missing_input_result_id_is_recorded_without_detail_call() -> None:
    client = StubClient()
    errors: list[ErrorRecord] = []

    records = list(
        iter_contractor_result_records(
            client,
            identity=CONTRACTOR_RESULT,
            search_items=[{"notifyNo": "IB2600000002"}],
            run_id="run-1",
            source_date=date(2026, 9, 1),
            search_page=1,
            errors=errors,
            stats=PageStats(),
        )
    )

    assert records == []
    assert client.calls == []
    assert len(errors) == 1
    assert errors[0].stage == "result_detail"
    assert errors[0].source_id == "IB2600000002"
    assert errors[0].error_type == "KeyError"
    assert errors[0].message == "'inputResultId'"


def test_notify_no_falls_back_to_search_item_but_result_version_stays_result_version() -> None:
    class FallbackClient:
        def get_result_detail(self, result_id: str) -> dict[str, Any]:
            return {
                "bideContractorInputResultDTO": {
                    "id": result_id,
                    "resultVersion": "01",
                    "notifyNo": None,
                }
            }

    records = list(
        iter_contractor_result_records(
            FallbackClient(),
            identity=CONTRACTOR_RESULT,
            search_items=[
                {
                    "notifyNo": "IB2600000003",
                    "notifyVersion": "07",
                    "inputResultId": "result-3",
                }
            ],
            run_id="run-1",
            source_date=date(2026, 9, 1),
            search_page=0,
            errors=[],
            stats=PageStats(),
        )
    )

    assert records[0].record.source_id == "IB2600000003"
    assert records[0].record.source_version == "01"
