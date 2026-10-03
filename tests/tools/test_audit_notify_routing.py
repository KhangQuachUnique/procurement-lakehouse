import json
from datetime import date

import pytest

from procurement.ingestion.engine.pagination import PaginationInvariantError
from procurement.tools.audit_notify_routing import routing_reasons, save_report, scan_day


def test_known_search_examples_and_existing_reoffer():
    base = {"id": "notice", "stepCode": "notify-contractor-step-4-kqlcnt"}
    assert routing_reasons({**base, "bidForm": "CGTTRG", "processApply": "LDT"}) == [
        "cgttrg_routed_standard"
    ]
    assert routing_reasons({**base, "processApply": "KHAC", "isInternet": 0}) == [
        "khac_routed_ldt", "offline_needs_verification"
    ]
    assert routing_reasons({**base, "processApply": "KHAC", "isInternet": 1}) == [
        "khac_routed_ldt"
    ]
    assert routing_reasons({**base, "processApply": "LDT", "isInternet": 1}) == []
    assert routing_reasons({**base, "stepCode": "reoffer-price-step-1", "bidForm": "CGTTRG"}) == []
    assert routing_reasons({"id": "notice"}) == ["unsupported_step_code"]


class SearchApi:
    def search(self, **kwargs):
        assert kwargs["window_from"] == "2025-01-02T00:00:00.000Z"
        assert kwargs["window_to"] == "2025-01-02T23:59:59.999Z"
        return {"page": {"totalElements": 2, "content": [{
            "id": str(kwargs["page_number"]),
            "stepCode": "notify-contractor-step-4-kqlcnt",
            "processApply": "KHAC", "isInternet": 0,
        }]}}


def test_full_pagination_unique_candidate_count_and_report(tmp_path):
    day = scan_day(SearchApi(), date(2025, 1, 2), 1)
    assert day["scanned"] == day["suspected"] == 2
    assert day["reasons"] == {"khac_routed_ldt": 2, "offline_needs_verification": 2}
    path = tmp_path / "report.json"
    save_report(path, {"status": "incomplete", "days": [day]})
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["status"] == "incomplete"
    assert report["summary"]["suspected_records"] == 2
    assert report["summary"]["affected_dates"] == ["2025-01-02"]


def test_partial_day_not_returned_as_complete():
    class BrokenApi(SearchApi):
        def search(self, **kwargs):
            response = super().search(**kwargs)
            if kwargs["page_number"] == 1:
                response["page"]["totalElements"] = 3
            return response

    with pytest.raises(PaginationInvariantError):
        scan_day(BrokenApi(), date(2025, 1, 2), 1)


def test_inventory_includes_unflagged_unknown_and_separate_bid_modes(tmp_path):
    standard = {
        "stepCode": "notify-contractor-step-4-kqlcnt", "processApply": "LDT",
        "bidForm": "DTRR", "isInternet": 1, "bidMode": "1_MTHS",
    }
    items = [{**standard, "id": str(i)} for i in range(7)] + [
        {**standard, "id": "other-mode", "bidMode": "1_HTHS"},
        {"id": "unknown", "stepCode": "new-workflow", "processApply": "NEW"},
        {"id": "missing-fields"},
    ]

    class Api:
        def search(self, **kwargs):
            return {"page": {"totalElements": len(items), "content": items}}

    days = [scan_day(Api(), date(2025, 1, day), 50) for day in (1, 2)]
    path = tmp_path / "report.json"
    save_report(path, {"status": "complete", "days": days})
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["summary"]["workflow_count"] == 4
    assert report["summary"]["scanned_records"] == 20
    assert report["summary"]["suspected_records"] == 4
    assert sum(group["records"] for group in report["workflows"]) == 20
    largest = report["workflows"][0]
    assert largest["records"] == 14
    assert largest["suspected_records"] == 0
    assert len(largest["samples"]) == 5
    assert largest["dates"] == [
        {"date": "2025-01-01", "records": 7}, {"date": "2025-01-02", "records": 7},
    ]
    assert largest["current_route"]["endpoint"].endswith("/lcnt_tbmt_ttc_ldt")
    assert report["workflows"][-1]["fields"]["bidMode"] is None
    assert report["workflows"][-1]["current_route"]["endpoint"] is None


def test_empty_day_inventory_and_reoffer_route(tmp_path):
    from procurement.tools.audit_notify_routing import current_route

    class Api:
        def search(self, **kwargs):
            return {"page": {"totalElements": 0, "content": []}}

    path = tmp_path / "empty.json"
    save_report(path, {"days": [scan_day(Api(), date(2025, 1, 1), 50)]})
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["workflows"] == []
    assert report["summary"]["scanned_records"] == 0
    assert current_route({"stepCode": "reoffer-price-step-1", "processApply": "LDT",
                          "isInternet": 1, "bidForm": "CGTTRG", "bidMode": "1_MTHS"})["endpoint"].endswith(
        "/online-reoffer/detail"
    )


def test_resume_preserves_complete_days_and_rejects_gaps(tmp_path):
    from procurement.tools.audit_notify_routing import load_resume

    path = tmp_path / "resume.json"
    report = {
        "schema_version": 2, "year": 2025, "status": "incomplete",
        "error": "ConnectError", "failed_date": "2025-01-02",
        "days": [{"date": "2025-01-01", "scanned": 0, "workflows": []}],
    }
    path.write_text(json.dumps(report), encoding="utf-8")
    resumed, start = load_resume(path, 2025)
    assert start == date(2025, 1, 2)
    assert resumed["days"] == report["days"]
    assert "error" not in resumed
    with pytest.raises(ValueError, match="match --year"):
        load_resume(path, 2024)
    report["days"][0]["date"] = "2025-01-02"
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="consecutive"):
        load_resume(path, 2025)


def test_safe_error_redacts_token(monkeypatch):
    from procurement.tools.audit_notify_routing import safe_error, settings

    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "secret-example")
    message = safe_error(ValueError("https://example.com?token=secret-example extra secret-example"))
    assert "secret-example" not in message
