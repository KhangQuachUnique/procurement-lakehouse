from datetime import date

import fsspec
import httpx
import pytest

from procurement.common.settings import settings
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.contractor_result.resource import (
    create_contractor_result_spec,
)
from procurement.storage.io import read_json
from procurement.tools import recover_result_2024 as job

DAY = date(2024, 9, 12)
NOTICE = "IB2400340694"


class Client:
    def __init__(self, status=500):
        self.status = status

    def post(self, path, body):
        if body["id"] == "bad":
            httpx.Response(self.status, request=httpx.Request("POST", "https://example.test" + path)).raise_for_status()
        return {"bideContractorInputResultDTO": {"notifyNo": NOTICE, "resultVersion": "01"}}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", (tmp_path / "bucket").as_posix())
    return fsspec.filesystem("file", auto_mkdir=True, skip_instance_cache=True), tmp_path


def extract(spec, *, day=DAY, notice=NOTICE):
    errors, stats = [], PageStats(search_items=2)
    rows = list(spec.records(search_items=[{"id": "1", "notifyNo": notice, "inputResultId": "bad"},
                                          {"id": "2", "notifyNo": "IB2400000001", "inputResultId": "good"}],
                             run_id="run", source_date=day, search_page=0, errors=errors, stats=stats))
    return rows, errors, stats


@pytest.mark.parametrize("day,notice", [
    (date(2024, 9, 9), "IB2400333221"),
    (date(2024, 9, 12), "IB2400340694"),
    (date(2024, 9, 16), "IB2400347507"),
])
def test_exact_exception_and_unchanged_normal_ingestion(setup, day, notice):
    fs, root = setup
    rows, errors, stats = extract(job.recovery_spec(Client(), fs, root, "job"), day=day, notice=notice)
    assert len(rows) == 1 and not errors
    assert stats.quality_observations[0]["context"]["inputResultId"] == "bad"
    key = fs.glob(f"{settings.OBJECT_STORAGE_BUCKET}/_quality/**/recovery-*.json")[0]
    evidence = read_json(fs, key)
    assert evidence["search_items"] == evidence["collected_records"] + evidence["excluded_records"] == 2
    assert evidence["excluded"][0]["error"]["http_status"] == 500
    assert len(extract(create_contractor_result_spec(Client()), day=day, notice=notice)[1]) == 1


@pytest.mark.parametrize("status,day,notice", [
    (503, DAY, NOTICE), (429, DAY, NOTICE), (403, DAY, NOTICE),
    (500, date(2024, 9, 9), NOTICE), (500, DAY, "IB2400000001"),
])
def test_unapproved_failures_are_not_skipped(setup, status, day, notice):
    fs, root = setup
    _, errors, stats = extract(job.recovery_spec(Client(status), fs, root, "job"), day=day, notice=notice)
    assert len(errors) == 1 and not stats.quality_observations


def test_recovered_detail_is_collected(setup):
    fs, root = setup
    rows, errors, stats = extract(job.recovery_spec(Client(200), fs, root, "job"))
    assert len(rows) == 2 and not errors and not stats.quality_observations


def test_evidence_failure_blocks_page(setup, monkeypatch):
    fs, root = setup
    def broken(*args):
        raise OSError("evidence upload failed")
    monkeypatch.setattr(job, "write_storage_json", broken)
    with pytest.raises(OSError):
        extract(job.recovery_spec(Client(), fs, root, "job"))


def test_outside_dates_are_rejected(setup):
    fs, root = setup
    with pytest.raises(ValueError, match="restricted"):
        extract(job.recovery_spec(Client(), fs, root, "job"), day=date(2024, 9, 13))


@pytest.mark.parametrize("reason,expected", [("source_failure", 3), ("unconfirmed_failure", 1)])
def test_continue_only_after_confirmed_source_failure(setup, monkeypatch, reason, expected):
    fs, root = setup
    monkeypatch.setattr(job, "create_s3_filesystem", lambda: fs)
    seen = []
    def flow(args, **kwargs):
        seen.append(args.start_date)
        assert args.retry_stale and args.start_date == args.end_date
        return {"resources": [], "execution_errors": [{"reason": reason}]}, 1
    monkeypatch.setattr(job, "execute_flow", flow)
    assert job.main(["--continue-on-error", "--retry-stale", "--output-dir", str(root / "output"),
                     "--lock-dir", str(root / "locks")]) == 1
    assert seen == list(job.DAYS)[:expected]
