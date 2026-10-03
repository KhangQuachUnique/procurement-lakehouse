import copy

import pytest

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.comparison import compare_audits
from procurement.quality.files import read_json, write_json


def make_reports(tmp_path, *, changed_copy=False, changed_version=False):
    metadata = {"resource": "notify_contractor", "year": 2025,
                "config_hash": "config", "storage_namespace": "storage"}
    old_rows = [{"source_id": "good", "source_version": "00", "content_hash": "keep",
                 "table": "standard", "result": {"status": "pass"}},
                {"source_id": "bad", "source_version": "00", "content_hash": "bad",
                 "table": "standard", "result": {"status": "fail"}}]
    new_rows = copy.deepcopy(old_rows)
    new_rows[1].update(content_hash="fixed", table="vk_adb", result={"status": "pass"})
    if changed_copy:
        new_rows[0]["content_hash"] = "unexpected"
    if changed_version:
        new_rows[1]["source_version"] = "01"
    selection = {"date": "2025-01-02", "run_id": "baseline"}
    for name, rows, run in (("before", old_rows, "baseline"), ("after", new_rows, "repair")):
        day = {"selection": {**selection, "run_id": run}, "status": "complete", "rows": rows}
        directory = tmp_path / name
        write_json(directory / "days/2025-01-02.json", day)
        write_json(directory / "selection.json", {
            **metadata, "status": "complete", "selection": [day["selection"]],
            "completed": {"2025-01-02": calculate_content_hash(day)},
        })
        write_json(directory / "summary.json", {
            "status": "complete", "records": 2, "quality_status": {"pass": 2 if name == "after" else 1},
            "days": {"complete": 1}, "tables": {}, "fully_verified": name == "after",
        })
    plan = {**metadata, "days": [{"selection": selection, "records": [
        {**row, "refetch": row["source_id"] == "bad"} for row in old_rows
    ]}]}
    plan["plan_hash"] = calculate_content_hash(plan)
    write_json(tmp_path / "work/2025-01-02.json", {
        "status": "success", "plan_hash": plan["plan_hash"], "run_id": "repair",
        "counts": {"copied": 1, "refetched": 1, "moved": 1},
    })
    return plan


def test_comparison_reconciles_repairs_and_retained_records(tmp_path):
    plan = make_reports(tmp_path)
    result = compare_audits(tmp_path / "before", tmp_path / "after", plan,
                            tmp_path / "work", tmp_path / "report")
    assert result["counts"] == {"copied": 1, "refetched": 1, "moved": 1, "resolved_failures": 1}
    assert result["repair_days"] == {"verified": 1}
    assert result["fully_verified"]


@pytest.mark.parametrize(("options", "message"), [
    ({"changed_copy": True}, "Unselected record changed"),
    ({"changed_version": True}, "Identity/version set changed"),
])
def test_comparison_rejects_unexpected_changes(tmp_path, options, message):
    plan = make_reports(tmp_path, **options)
    with pytest.raises(ValueError, match=message):
        compare_audits(tmp_path / "before", tmp_path / "after", plan,
                       tmp_path / "work", tmp_path / "report")


@pytest.mark.parametrize("status,blocked", [("fail", True), ("unresolved", True),
                                          ("unresolved", False), ("pass", False)])
def test_report_includes_unselected_days_with_remaining_quality_issues(tmp_path, status, blocked):
    plan = make_reports(tmp_path)
    plan.pop("plan_hash")
    plan["days"] = []
    plan["blocked"] = [{"date": "2025-01-02", "reason": "unresolved_context_or_route"}] if blocked else []
    plan["plan_hash"] = calculate_content_hash(plan)
    directory = tmp_path / "after"
    day = read_json(directory / "days/2025-01-02.json")
    day["rows"][1]["result"]["status"] = status
    write_json(directory / "days/2025-01-02.json", day)
    state = read_json(directory / "selection.json")
    state["completed"]["2025-01-02"] = calculate_content_hash(day)
    write_json(directory / "selection.json", state)
    summary = read_json(directory / "summary.json")
    summary.update(fully_verified=status == "pass", quality_status={"pass": 1, status: 1})
    write_json(directory / "summary.json", summary)
    result = compare_audits(tmp_path / "before", directory, plan, tmp_path / "work", tmp_path / "report")
    item = result["days"][0]
    assert item["repair_status"] == ("blocked" if blocked else "not_selected")
    assert item["needs_attention"] == (status != "pass")
    text = (tmp_path / "report/comparison.md").read_text(encoding="utf-8")
    assert ("- 2025-01-02:" in text) == (status != "pass")
    if status != "pass":
        assert f"{status}=1" in text
    if blocked:
        assert "unresolved_context_or_route" in text


def test_report_shows_failed_repair_receipt_reason(tmp_path):
    plan = make_reports(tmp_path)
    write_json(tmp_path / "work/2025-01-02.json", {
        "status": "blocked", "error": {"type": "ValueError", "message": "Another active run exists"},
    })
    result = compare_audits(tmp_path / "before", tmp_path / "after", plan,
                            tmp_path / "work", tmp_path / "report")
    assert result["days"][0]["needs_attention"]
    assert result["days"][0]["repair_status"] == "not_verified"
    assert "Another active run exists" in (tmp_path / "report/comparison.md").read_text(encoding="utf-8")
