"""Resource CLI and day service tests, using only in-memory object storage."""

import json
from datetime import UTC, date, datetime

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from procurement.cli import silver
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.silver import service
from procurement.silver.application.resources import get_silver_resource
from procurement.silver.domain.selection import file_hash

runner = CliRunner()


@pytest.mark.parametrize("command", ["notice", "tbmt"])
def test_resource_command_defaults_to_all_families(monkeypatch, command):
    calls = []
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "completed", "output_prefix": "bucket/candidate"}

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(silver.app, [command, "--date", "2025-01-01"])
    assert result.exit_code == 0, result.output
    assert calls[0][0] == ("fs", "bucket", "notice", date(2025, 1, 1), date(2025, 1, 1))
    assert calls[0][1]["family"] is None
    assert "report.json" in result.output


@pytest.mark.parametrize("command", ["khlcnt", "plan", "package"])
def test_khlcnt_command_defaults_to_all_families(monkeypatch, command):
    calls = []
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "completed", "output_prefix": "bucket/candidate"}

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(silver.app, [command, "--date", "2025-01-01"])
    assert result.exit_code == 0, result.output
    assert calls[0][0] == ("fs", "bucket", "khlcnt", date(2025, 1, 1), date(2025, 1, 1))
    assert calls[0][1]["family"] is None
    assert "report.json" in result.output


def test_khlcnt_family_selection(monkeypatch):
    calls = []
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "completed", "output_prefix": "bucket/candidate"}

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(
        silver.app, ["khlcnt", "--date", "2025-01-01", "--family", "package"]
    )
    assert result.exit_code == 0, result.output
    assert calls[0][1]["family"] == "package"


def test_range_family_and_failure_exit(monkeypatch):
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        assert args[3:] == (date(2025, 1, 1), date(2025, 1, 3))
        assert kwargs["family"] == "reoffer" and kwargs["continue_on_error"]
        return {"status": "failed", "output_prefix": "bucket/candidate"}

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(
        silver.app,
        [
            "notice",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-03",
            "--family",
            "reoffer",
            "--continue-on-error",
        ],
    )
    assert result.exit_code == 1


@pytest.mark.parametrize(
    "options",
    [
        [],
        ["--date", "bad"],
        ["--start-date", "2025-01-01"],
        ["--date", "2025-01-01", "--end-date", "2025-01-02"],
        ["--start-date", "2025-02-01", "--end-date", "2025-01-01"],
        ["--date", "2025-01-01", "--family", "unknown"],
    ],
)
def test_invalid_options_fail_before_storage(monkeypatch, options):
    def unexpected():
        pytest.fail("Invalid arguments must not access storage")

    monkeypatch.setattr(silver, "get_storage", unexpected)
    assert runner.invoke(silver.app, ["notice", *options]).exit_code == 2


@pytest.fixture
def bronze(monkeypatch):
    fs = fsspec.filesystem("memory")
    bucket = "silver-service-test"
    contract = get_silver_resource("notice")

    def freeze(_fs, resources, start, end):
        assert resources == ["notify_contractor"] and start == end
        files = []
        for family, table in contract.families:
            root = {
                "id": f"id-{family}",
                "notifyNo": f"IB-{family}",
                "notifyVersion": "00",
                "bidName": "Test",
                "bidPrice": 123,
                "bidPriceUnit": "VND",
            }
            if family == "standard":
                payload = {"bidoNotifyContractorM": root}
            elif family == "vk_adb":
                payload = {"bidoNotifyContractorP": root}
            else:
                payload = {**root, "reofferNo": root["notifyNo"], "reofferVersion": "00"}
            row = {
                "payload": json.dumps(payload),
                "source_id": root["notifyNo"],
                "source_version": "00",
                "run_id": "bronze-run",
                "source_date": str(start),
                "ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
                "content_hash": calculate_content_hash(payload),
            }
            key = f"{bucket}/bronze/{table}/{start}.parquet"
            with fs.open(key, "wb") as stream:
                pq.write_table(pa.Table.from_pylist([row]), stream)
            files.append({"key": key, "sha256": file_hash(fs, key), "table": table})
        return [
            {
                "resource": "notify_contractor",
                "source_date": str(start),
                "run_id": "bronze-run",
                "expected_records": 3,
                "files": files,
            }
        ]

    monkeypatch.setattr(service, "freeze", freeze)
    return fs, bucket


def test_service_runs_days_in_order_and_writes_all_families(bronze):
    fs, bucket = bronze
    seen = []
    report = service.run_resource(
        fs,
        bucket,
        "notice",
        date(2025, 1, 1),
        date(2025, 1, 2),
        on_progress=lambda entry: seen.append(entry["date"]),
    )
    assert report["status"] == "completed"
    assert seen == ["2025-01-01", "2025-01-02"]
    for entry in report["days"]:
        assert entry["quality"]["accepted"] == 3
        assert entry["input_counts"] == {"standard": 1, "vk_adb": 1, "reoffer": 1}
        with fs.open(entry["files"]["notice_revision"], "rb") as stream:
            table = pq.ParquetFile(stream).read()
        assert table.num_rows == 3
        assert table.schema.field("bid_price").type == pa.decimal128(38, 6)
        with fs.open(entry["files"]["observation"], "rb") as stream:
            observations = pq.ParquetFile(stream).read().to_pylist()
        assert all(len(row["file_sha256"]) == 64 for row in observations)
        assert all(row["observed_at"] == datetime(2026, 1, 1, tzinfo=UTC) for row in observations)
    with fs.open(report["output_prefix"] + "/report.json") as stream:
        assert json.load(stream)["status"] == "completed"
    again = service.run_resource(fs, bucket, "tbmt", date(2025, 1, 1), date(2025, 1, 1))
    assert again["output_prefix"] != report["output_prefix"]


def test_family_filter_keeps_full_input_reconciliation(bronze):
    fs, bucket = bronze
    report = service.run_resource(
        fs, bucket, "notice", date(2025, 1, 1), date(2025, 1, 1), family="reoffer"
    )
    assert report["days"][0]["quality"]["accepted"] == 1
    assert sum(report["days"][0]["input_counts"].values()) == 3


@pytest.mark.parametrize("keep_going,expected_days", [(False, 1), (True, 2)])
def test_missing_commit_is_failure_not_empty_success(
    bronze, monkeypatch, keep_going, expected_days
):
    def missing(*args):
        raise ValueError("No committed attempt")

    monkeypatch.setattr(service, "freeze", missing)
    fs, bucket = bronze
    report = service.run_resource(
        fs, bucket, "notice", date(2025, 1, 1), date(2025, 1, 2), continue_on_error=keep_going
    )
    assert report["status"] == "failed"
    assert len(report["days"]) == expected_days
    assert all("files" not in entry for entry in report["days"])


def test_skip_existing_skips_already_processed_days(bronze):
    fs, bucket = bronze
    # First run: processes normally
    first_report = service.run_resource(
        fs, bucket, "notice", date(2025, 1, 1), date(2025, 1, 1), skip_existing=False
    )
    assert first_report["status"] == "completed"
    assert first_report["days"][0]["status"] == "completed"

    # Second run with skip_existing=True: skips execution
    skip_report = service.run_resource(
        fs, bucket, "notice", date(2025, 1, 1), date(2025, 1, 1), skip_existing=True
    )
    assert skip_report["status"] == "completed"
    assert skip_report["days"][0]["status"] == "skipped"
    assert "Skipped" in skip_report["days"][0]["message"]


def test_cli_skip_existing_flag_passed(monkeypatch):
    calls = []
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "status": "completed",
            "output_prefix": "bucket/candidate",
            "days": [{"date": "2025-01-01", "status": "skipped", "quality": {}}],
        }

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(
        silver.app, ["notice", "--date", "2025-01-01", "--skip-existing"]
    )
    assert result.exit_code == 0, result.output
    assert calls[0][1]["skip_existing"] is True


def test_cli_reset_aborts_on_no(monkeypatch):
    mock_fs = fsspec.filesystem("memory")
    mock_fs.store.clear()
    mock_fs.makedirs("bucket/silver", exist_ok=True)
    mock_fs.touch("bucket/silver/sample.parquet")
    monkeypatch.setattr(silver, "get_storage", lambda: (mock_fs, "bucket"))

    result = runner.invoke(silver.app, ["reset"], input="n\n")
    assert result.exit_code == 0
    assert "Huề cả làng" in result.output
    assert mock_fs.exists("bucket/silver/sample.parquet")


def test_cli_reset_proceeds_on_yes(monkeypatch):
    mock_fs = fsspec.filesystem("memory")
    mock_fs.store.clear()
    mock_fs.makedirs("bucket/silver", exist_ok=True)
    mock_fs.touch("bucket/silver/sample.parquet")
    monkeypatch.setattr(silver, "get_storage", lambda: (mock_fs, "bucket"))

    result = runner.invoke(silver.app, ["reset"], input="y\n")
    assert result.exit_code == 0
    assert "thanh tẩy thành công" in result.output
    assert not mock_fs.exists("bucket/silver/sample.parquet")


def test_cli_reset_force_flag_bypasses_prompt(monkeypatch):
    mock_fs = fsspec.filesystem("memory")
    mock_fs.store.clear()
    mock_fs.makedirs("bucket/silver", exist_ok=True)
    mock_fs.touch("bucket/silver/sample.parquet")
    monkeypatch.setattr(silver, "get_storage", lambda: (mock_fs, "bucket"))

    result = runner.invoke(silver.app, ["reset", "--force"])
    assert result.exit_code == 0
    assert "CẢNH BÁO NGUY CẤP" not in result.output
    assert "thanh tẩy thành công" in result.output
    assert not mock_fs.exists("bucket/silver/sample.parquet")


def test_split_date_range_by_year():
    assert service.split_date_range_by_year(date(2022, 1, 1), date(2022, 12, 31)) == [
        (date(2022, 1, 1), date(2022, 12, 31))
    ]
    chunks = service.split_date_range_by_year(date(2022, 3, 15), date(2024, 7, 20))
    assert chunks == [
        (date(2022, 3, 15), date(2022, 12, 31)),
        (date(2023, 1, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 7, 20)),
    ]


def test_merge_run_reports():
    rep1 = {
        "run_id": "r1",
        "resource": "notice",
        "start_date": "2022-01-01",
        "end_date": "2022-12-31",
        "status": "completed",
        "days": [{"date": "2022-01-01", "status": "completed"}],
        "output_prefix": "bucket/silver/_reports/notice/r1",
    }
    rep2 = {
        "run_id": "r2",
        "resource": "notice",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "status": "completed",
        "days": [{"date": "2023-01-01", "status": "completed"}],
        "output_prefix": "bucket/silver/_reports/notice/r2",
    }
    merged = service.merge_run_reports([rep1, rep2])
    assert merged["status"] == "completed"
    assert merged["start_date"] == "2022-01-01"
    assert merged["end_date"] == "2023-12-31"
    assert len(merged["days"]) == 2


def test_cli_multi_year_parallel_execution(monkeypatch):
    calls = []
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        start_d = args[3]
        return {
            "run_id": f"r_{start_d.year}",
            "resource": args[2],
            "start_date": str(start_d),
            "end_date": str(args[4]),
            "status": "completed",
            "output_prefix": f"bucket/reports/{start_d.year}",
            "days": [{"date": str(start_d), "status": "completed", "quality": {"accepted": 10}}],
        }

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(
        silver.app,
        ["notice", "--start-date", "2022-01-01", "--end-date", "2024-12-31"],
    )
    assert result.exit_code == 0, result.output
    # Expect 3 calls corresponding to 2022, 2023, 2024
    called_years = {c[0][3].year for c in calls}
    assert called_years == {2022, 2023, 2024}


def test_cli_multi_year_no_parallel_flag(monkeypatch):
    calls = []
    monkeypatch.setattr(silver, "get_storage", lambda: ("fs", "bucket"))

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "status": "completed",
            "output_prefix": "bucket/reports",
            "days": [],
        }

    monkeypatch.setattr(silver, "run_resource", run)
    result = runner.invoke(
        silver.app,
        ["notice", "--start-date", "2022-01-01", "--end-date", "2024-12-31", "--no-parallel"],
    )
    assert result.exit_code == 0, result.output
    # When --no-parallel is set, it executes a single run with the full range
    assert len(calls) == 1
    assert calls[0][0][3] == date(2022, 1, 1)
    assert calls[0][0][4] == date(2024, 12, 31)



