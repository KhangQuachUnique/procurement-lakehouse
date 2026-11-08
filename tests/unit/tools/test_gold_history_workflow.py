import json
from unittest.mock import Mock

import pytest

from scripts.run_gold_v2_history_workflow import workflow


def local(**changes):
    return {
        "status": "LOCAL_SILVER_VALIDATED",
        "reviewed_resource_days": 0,
        "completed_resource_days": 4383,
        "raw_records": 5359973,
    } | changes


def test_reviewed_canonical_day_never_launches_publication(tmp_path):
    launcher = Mock()
    with pytest.raises(RuntimeError, match="coverage/quality"):
        workflow(
            tmp_path, waiter=Mock(return_value=local(reviewed_resource_days=1)), launcher=launcher
        )
    launcher.assert_not_called()
    state = json.loads((tmp_path / "full-history-workflow-v1/job.json").read_text())
    assert state["status"] == "BLOCKED"
    assert state["current"] == "canonical"


def test_only_real_full_evidence_can_complete_and_duplicate_cannot_run(tmp_path):
    calls = []

    def launch(unit, module, arguments, marker, expected_status, log):
        calls.append(module)
        if expected_status == "SILVER_READY":
            return {"status": expected_status, "namespace": "test_silver_v2_test"}
        path = tmp_path / "full-history-gold-v1/receipt.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "scope": ["2022-01-01", "2025-12-31"],
                    "actual_suffix_tested": True,
                    "verified_tables": 16,
                    "incremental_clean_mismatches": {"fact_award_daily": 0},
                    "recovery": {"resumed": True},
                    "actual_multi_revision_entities": {"result_revision": 1},
                }
            )
        )
        return {"status": "PASS"}

    result = workflow(tmp_path, waiter=Mock(return_value=local()), launcher=launch)
    assert result["status"] == "PASS"
    assert result["production_written"] is False
    assert calls == [
        "procurement.tools.gold_history_publish",
        "procurement.tools.gold_history_acceptance",
    ]
    with pytest.raises(RuntimeError, match="already exists"):
        workflow(tmp_path, waiter=Mock(return_value=local()), launcher=launch)
    assert len(calls) == 2


def test_failed_publication_never_starts_gold(tmp_path):
    launcher = Mock(side_effect=RuntimeError("Publication failed"))
    with pytest.raises(RuntimeError, match="Publication failed"):
        workflow(tmp_path, waiter=Mock(return_value=local()), launcher=launcher)
    assert launcher.call_count == 1
    state = json.loads((tmp_path / "full-history-workflow-v1/job.json").read_text())
    assert state["status"] == "BLOCKED"
    assert state["current"] == "publish"


def test_inspected_resume_requires_explicit_flag_and_preserves_remaining_limit(
    tmp_path, monkeypatch
):
    from scripts import run_gold_v2_history_workflow as runner

    marker = tmp_path / "job.json"
    marker.write_text(json.dumps({"status": "FAILED", "elapsed_active_seconds": 1000}))
    monkeypatch.setattr(runner, "properties", lambda _: {"ActiveState": "inactive"})
    invoked = []

    def command(args, **kwargs):
        invoked.extend(args)
        marker.write_text(json.dumps({"status": "SILVER_READY"}))
        return Mock(returncode=0)

    monkeypatch.setattr(runner.subprocess, "run", command)
    args = ("unit", "module", [], marker, "SILVER_READY", tmp_path / "job.log")
    with pytest.raises(RuntimeError, match="inspected"):
        runner.launch(*args)
    assert not invoked
    assert runner.launch(*args, resume=True, runtime_seconds=20600)["status"] == "SILVER_READY"
    assert "--property=RuntimeMaxSec=20600" in invoked
