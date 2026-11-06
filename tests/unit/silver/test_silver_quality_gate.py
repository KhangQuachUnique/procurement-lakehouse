from datetime import date
from unittest.mock import MagicMock

import pytest

from procurement.silver.application import process_silver_service
from procurement.silver.application.process_silver_service import (
    ProcessSilverService,
)
from procurement.silver.ports.outbound.bronze_repository import (
    BronzeFileDescriptor,
    CommittedBronzePartition,
)


@pytest.mark.parametrize(
    ("quarantined", "blocking_issues"),
    [(1, 0), (0, 1)],
)
def test_quality_gate_prevents_candidate_write(
    monkeypatch,
    quarantined,
    blocking_issues,
):
    day = date(2025, 1, 1)

    bronze_repo = MagicMock()
    silver_repo = MagicMock()

    file = BronzeFileDescriptor(
        table="bid_opening_detail",
        key="bucket/bronze/opening.parquet",
        sha256="abc",
    )

    partition = CommittedBronzePartition(
        resource="bid_opening",
        source_date=str(day),
        run_id="bronze-run",
        expected_records=1,
        files=(file,),
        manifest={},
    )

    bronze_repo.select_committed_partitions.return_value = [partition]

    bronze_repo.stream_partition_records.return_value = [
        (
            {
                "table": file.table,
                "key": file.key,
                "sha256": file.sha256,
            },
            0,
            {},
        )
    ]

    monkeypatch.setattr(
        process_silver_service,
        "transform",
        lambda *args: {"dummy": True},
    )
    monkeypatch.setattr(
        process_silver_service,
        "assemble",
        lambda rows: {},
    )
    monkeypatch.setattr(
        process_silver_service,
        "validate",
        lambda tables: {
            "accepted": 1 - quarantined,
            "quarantined": quarantined,
            "issues": blocking_issues,
            "blocking_issues": blocking_issues,
        },
    )

    def unexpected_cast(*args):
        pytest.fail("Blocked data must not be cast or written")

    monkeypatch.setattr(
        process_silver_service,
        "typed_arrow_rows",
        unexpected_cast,
    )

    service = ProcessSilverService(
        bronze_repo=bronze_repo,
        silver_repo=silver_repo,
        bucket="bucket",
        resource_name="bid_opening",
    )

    report = service.execute(
        start_date=day,
        end_date=day,
    )

    assert report["status"] == "needs_review"
    assert report["days"][0]["status"] == "needs_review"
    assert report["days"][0]["files"] == {}
    assert report["days"][0]["iceberg"] is None

    silver_repo.write_candidate_tables.assert_not_called()
    silver_repo.save_run_report.assert_called()


def test_quality_gate_allows_valid_candidate_write(monkeypatch):
    from procurement.silver.application.resources import get_silver_resource

    day = date(2025, 1, 1)
    bronze_repo = MagicMock()
    silver_repo = MagicMock()

    file = BronzeFileDescriptor(
        table="bid_opening_detail",
        key="bucket/bronze/opening.parquet",
        sha256="abc",
    )

    partition = CommittedBronzePartition(
        resource="bid_opening",
        source_date=str(day),
        run_id="bronze-run",
        expected_records=1,
        files=(file,),
        manifest={},
    )

    bronze_repo.select_committed_partitions.return_value = [partition]
    bronze_repo.stream_partition_records.return_value = [
        (
            {
                "table": file.table,
                "key": file.key,
                "sha256": file.sha256,
            },
            0,
            {},
        )
    ]

    contract = get_silver_resource("bid_opening")
    tables = {name: [] for name in contract.output_tables}

    monkeypatch.setattr(
        process_silver_service,
        "transform",
        lambda *args: {"dummy": True},
    )
    monkeypatch.setattr(
        process_silver_service,
        "assemble",
        lambda rows: tables,
    )
    monkeypatch.setattr(
        process_silver_service,
        "validate",
        lambda rows: {
            "accepted": 1,
            "quarantined": 0,
            "issues": 0,
            "blocking_issues": 0,
        },
    )
    monkeypatch.setattr(
        process_silver_service,
        "typed_arrow_rows",
        lambda name, rows: ("mock_arrow_table", name),
    )

    expected_paths = {
        name: f"mock://silver/{name}.parquet" for name in contract.output_tables
    }
    silver_repo.write_candidate_tables.return_value = expected_paths

    service = ProcessSilverService(
        bronze_repo=bronze_repo,
        silver_repo=silver_repo,
        bucket="bucket",
        resource_name="bid_opening",
    )

    report = service.execute(day, day)

    assert report["status"] == "completed"
    assert report["days"][0]["status"] == "completed"
    assert report["days"][0]["quality"]["blocking_issues"] == 0
    assert report["days"][0]["files"] == expected_paths

    silver_repo.write_candidate_tables.assert_called_once()

    args, _ = silver_repo.write_candidate_tables.call_args
    assert args[1] == day
    assert set(args[2]) == set(contract.output_tables)

    silver_repo.save_run_report.assert_called()
