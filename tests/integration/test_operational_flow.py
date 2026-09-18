import json
from threading import Barrier, Lock

import httpx
import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.contractor_result.resource import (
    CONTRACTOR_RESULT_DETAIL_PATH,
)
from procurement.ingestion.sources.muasamcong.khlcnt.resource import (
    BID_PACKAGE_DETAIL_PATH,
    PLAN_DETAIL_PATH,
)
from procurement.ingestion.sources.muasamcong.notify_contractor.resource import (
    REOFFER_DETAIL_PATH,
    STANDARD_DETAIL_PATH,
)
from procurement.ingestion.sources.muasamcong.project.resource import PROJECT_DETAIL_PATH
from procurement.jobs import ingest, runner
from procurement.storage.errors import list_error_records
from procurement.storage.execution import read_execution

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "failure_status,continue_mode,attempted", [(404, True, 8), (404, False, 1), (401, True, 1)]
)
def test_continue_across_days_and_resources_then_repair_only_failed_day(
    store, monkeypatch, failure_status, continue_mode, attempted
):
    fs, _ = store
    source = Source()
    source.failed_result = False
    current_day = None
    failing = True

    def handler(request):
        nonlocal current_day
        body = json.loads(request.content)
        if isinstance(body, list):
            filters = body[0]["query"][0]["filters"]
            current_day = next(
                item["from"][:10] for item in filters if item["searchType"] == "range"
            )
        if request.url.path == PROJECT_DETAIL_PATH and current_day == "2025-01-01" and failing:
            return httpx.Response(failure_status)
        return source(request)

    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "test")
    monkeypatch.setattr(runner, "create_s3_filesystem", lambda: fs)
    monkeypatch.setattr(
        runner,
        "MuasamcongClient",
        lambda **kwargs: MuasamcongClient(
            **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    options = ingest._parser().parse_args(
        ["backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-02",
         "--resource-workers", "1"]
    )
    options.continue_on_error = continue_mode
    report, code = ingest.execute_flow(options, fs=fs)
    assert code == 1
    assert report["attempted_days"] == attempted
    assert report["planned_days"] == 8
    assert report["stopped_early"] is (attempted == 1)
    assert report["execution_errors"][0]["reason"] == (
        "authentication_failure" if failure_status == 401 else "source_failure"
    )
    if attempted == 8:
        assert report["resources"][0]["missing_dates"] == ["2025-01-01"]
        assert all(item["verified"]["days"] == 2 for item in report["resources"][1:])
        failing = False
        source.searches.clear()
        options.mode = "repair"
        report, code = ingest.execute_flow(options, fs=fs)
        assert code == 0
        assert report["planned_days"] == report["attempted_days"] == 1
        assert source.searches == ["project"]
        assert sum(item["verified"]["records"] for item in report["resources"]) == 12


def test_live_empty_page_contract_commits_without_parquet(store, monkeypatch):
    fs, bucket = store
    calls = []

    def empty_source(request):
        calls.append(request)
        assert isinstance(json.loads(request.content), list)  # no detail call on an empty day
        return httpx.Response(
            200,
            json={
                "page": {
                    "content": [],
                    "totalElements": 0,
                    "totalPages": 0,
                    "currentPage": 0,
                    "pageSize": 50,
                    "empty": True,
                    "last": True,
                }
            },
        )

    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "fake_token")
    monkeypatch.setattr(runner, "create_s3_filesystem", lambda: fs)
    monkeypatch.setattr(
        runner,
        "MuasamcongClient",
        lambda **kwargs: MuasamcongClient(**kwargs, transport=httpx.MockTransport(empty_source)),
    )
    args = ingest._parser().parse_args(
        ["backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-01"]
    )
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 0
    assert len(calls) == 4
    assert all(
        item["verified"] == {"days": 1, "files": 0, "records": 0} for item in report["resources"]
    )
    assert not fs.glob(f"{bucket}/bronze/**/*.parquet")


class Source:
    """Known adapter contracts over real HTTPX; no network call to MuaSamCong."""

    def __init__(self):
        self.failed_result = True
        self.searches = []

    def __call__(self, request):
        body = json.loads(request.content)
        if isinstance(body, list):
            filters = {item["fieldName"]: item for item in body[0]["query"][0]["filters"]}
            kind = filters["type"]["fieldValues"][0]
            if kind == "es-bidp-project-p":
                resource, content = "project", [{"id": "p"}]
            elif kind == "es-plan-project-p":
                resource, content = "khlcnt", [{"id": "plan", "planVersion": "01"}]
            elif "publicDateKqlcnt" in filters:
                resource, content = (
                    "contractor_result",
                    [{"inputResultId": "result", "notifyNo": "N1"}],
                )
            else:
                resource = "notify_contractor"
                content = [
                    {"id": "n1", "stepCode": "notify-contractor-step-1-tbmt"},
                    {"id": "n2", "stepCode": "reoffer-price-step-1"},
                ]
            self.searches.append(resource)
            return httpx.Response(
                200,
                json={
                    "page": {
                        "content": content,
                        "totalElements": len(content),
                        "totalPages": 1,
                        "number": 0,
                        "size": 50,
                    }
                },
            )
        responses = {
            PROJECT_DETAIL_PATH: {"id": "p", "projectDTO": {"version": "01"}},
            PLAN_DETAIL_PATH: {"id": "plan", "bidpPlanDetailToProjectList": [{"id": "pkg"}]},
            BID_PACKAGE_DETAIL_PATH: {"id": "pkg"},
            STANDARD_DETAIL_PATH: {
                "bidoNotifyContractorM": {"notifyNo": "N1", "notifyVersion": "00"}
            },
            REOFFER_DETAIL_PATH: {"notifyNo": "N2", "notifyVersion": "00"},
            CONTRACTOR_RESULT_DETAIL_PATH: {
                "bideContractorInputResultDTO": {"notifyNo": "N1", "resultVersion": "01"}
            },
        }
        if request.url.path == CONTRACTOR_RESULT_DETAIL_PATH and self.failed_result:
            return httpx.Response(503)
        return httpx.Response(200, json=responses[request.url.path])


def test_full_flow_repair_idempotence_and_failed_refresh(store, monkeypatch):
    fs, _ = store
    source = Source()
    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "fixture-token")
    monkeypatch.setattr(runner, "create_s3_filesystem", lambda: fs)
    monkeypatch.setattr(
        runner,
        "MuasamcongClient",
        lambda **kwargs: MuasamcongClient(
            **kwargs, transport=httpx.MockTransport(source), sleep=lambda _: None
        ),
    )
    args = ingest._parser().parse_args(
        ["backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-01"]
    )
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 1
    assert [item["missing_dates"] for item in report["resources"]] == [[], [], [], ["2025-01-01"]], "\n".join(
        error.message for name in ("project", "khlcnt")
        for error in list_error_records(fs, get_resource(name).identity)
    )
    assert report["execution_errors"][0]["resource"] == "contractor_result"

    source.failed_result = False
    source.searches.clear()
    args.mode = "repair"
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 0
    assert source.searches == ["contractor_result"]
    assert sum(item["verified"]["records"] for item in report["resources"]) == 6
    for item in report["resources"]:
        run_id = item["dates"][0]["effective_run_id"]
        assert (
            read_execution(fs, get_resource(item["resource"]).identity, run_id)["state"]
            == "finished"
        )

    source.searches.clear()
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 0
    assert source.searches == []  # rerunning a completed plan does not recrawl

    args.resource = "contractor_result"
    previous = report["resources"][-1]["dates"][0]["effective_run_id"]
    args.refresh = True
    source.failed_result = True
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 1  # even when previous successful coverage still exists
    assert report["resources"][0]["dates"][0]["effective_run_id"] == previous
    assert report["resources"][0]["verified"]["records"] == 1

    # Corrupt only this disposable test bucket: verification must catch invalid committed data.
    from procurement.storage.committed import select_committed_days

    start, end = ingest.resolve_dates(args, today=ingest.today_vn())
    selected = select_committed_days(fs, get_resource("contractor_result"), start, end)
    with fs.open(selected[0].files[0][1], "wb") as file:
        file.write(b"broken-parquet")
    args.mode = "verify"
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 1
    assert report["execution_errors"]


def test_parallel_packages_and_resources_preserve_failed_day_and_repair(store, monkeypatch):
    fs, _ = store
    source = Source()
    source.failed_result = False
    resource_gate, package_gate = Barrier(2), Barrier(3)
    lock = Lock()
    active = peak = searches = 0
    failing = True

    def handler(request):
        nonlocal active, peak, searches
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            body = json.loads(request.content)
            if isinstance(body, list):
                with lock:
                    searches += 1
                    number = searches
                if number <= 2:
                    resource_gate.wait(timeout=10)  # two real day runners must overlap
                response = source(request)
                payload = response.json()
                filters = body[0]["query"][0]["filters"]
                if any(item.get("fieldValues") == ["es-plan-project-p"] for item in filters):
                    day = next(item["from"][:10] for item in filters if "from" in item)
                    payload["page"]["content"] = [{"id": day, "planVersion": "01"}]
                return httpx.Response(200, json=payload)
            if request.url.path == PLAN_DETAIL_PATH:
                return httpx.Response(200, json={
                    "id": body["id"],
                    "bidpPlanDetailToProjectList": [
                        {"id": f"{body['id']}-pkg-{n}"} for n in range(7)
                    ],
                })
            if request.url.path == BID_PACKAGE_DETAIL_PATH:
                if body["id"].endswith(("-0", "-1", "-2")):
                    package_gate.wait(timeout=10)
                if failing and body["id"] == "2025-01-01-pkg-3":
                    return httpx.Response(404)
                return httpx.Response(200, json={"id": body["id"]})
            return source(request)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "fixture-token")
    monkeypatch.setattr(runner, "create_s3_filesystem", lambda: fs)
    monkeypatch.setattr(
        runner, "MuasamcongClient",
        lambda **kwargs: MuasamcongClient(**kwargs, transport=httpx.MockTransport(handler)),
    )
    args = ingest._parser().parse_args([
        "backfill", "--start-date", "2025-01-01", "--end-date", "2025-01-02",
        "--continue-on-error", "--resource-workers", "2", "--khlcnt-package-workers", "3",
        "--source-max-inflight", "3",
    ])
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 1
    assert peak == 3
    assert report["attempted_days"] == 8
    assert len(report["execution_errors"]) == 1
    assert report["execution_errors"][0]["reason"] == "source_failure"
    assert report["resources"][1]["missing_dates"] == ["2025-01-01"]
    assert all(not item["missing_dates"] for item in report["resources"] if item["resource"] != "khlcnt")

    failing = False
    source.searches.clear()
    args.mode = "repair"
    report, code = ingest.execute_flow(args, fs=fs)
    assert code == 0
    assert report["attempted_days"] == report["planned_days"] == 1
    assert source.searches == ["khlcnt"]
    assert sum(item["verified"]["records"] for item in report["resources"]) == 24
