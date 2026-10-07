from datetime import date
from unittest.mock import MagicMock, patch
from uuid import uuid4

from procurement.cli.ingest import build_parser, main, run_ingest
from procurement.ingestion.contracts import MaterializeDayResult


def test_build_parser_parses_arguments():
    parser = build_parser()
    args = parser.parse_args(["--resource", "project", "--date", "2024-03-01", "--refresh"])
    assert args.resource == "project"
    assert args.source_date == date(2024, 3, 1)
    assert args.refresh is True
    assert args.request_id is None
    assert args.owner_id is None


def test_run_ingest_calls_service():
    mock_service = MagicMock()
    commit_id = uuid4()
    attempt_id = uuid4()

    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="project",
        source_date=date(2024, 3, 1),
        reused=False,
        status="success",
        record_count=42,
        file_count=2,
        commit_id=commit_id,
        attempt_id=attempt_id,
        metrics={"test": 1},
    )

    with patch("procurement.cli.ingest.get_ingestion_service", return_value=mock_service):
        parser = build_parser()
        args = parser.parse_args(["--resource", "project", "--date", "2024-03-01"])
        report = run_ingest(args)

    assert report["resource"] == "project"
    assert report["source_date"] == "2024-03-01"
    assert report["status"] == "success"
    assert report["reused"] is False
    assert report["commit_id"] == str(commit_id)
    assert report["attempt_id"] == str(attempt_id)
    assert report["record_count"] == 42
    assert report["file_count"] == 2
    assert report["metrics"] == {"test": 1}


def test_cli_main_exit_codes():
    mock_service = MagicMock()
    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="project",
        source_date=date(2024, 3, 1),
        reused=True,
        status="success",
        record_count=0,
        file_count=0,
    )

    with patch("procurement.cli.ingest.get_ingestion_service", return_value=mock_service):
        code = main(["--resource", "project", "--date", "2024-03-01"])
        assert code == 0

    mock_service.materialize_day.return_value = MaterializeDayResult(
        source="muasamcong",
        resource="project",
        source_date=date(2024, 3, 1),
        reused=False,
        status="failed",
        record_count=0,
        file_count=0,
    )

    with patch("procurement.cli.ingest.get_ingestion_service", return_value=mock_service):
        code = main(["--resource", "project", "--date", "2024-03-01"])
        assert code == 1
