from contextlib import nullcontext
from unittest.mock import Mock

import pytest
from dagster import Failure, build_op_context

from procurement.benchmark.models import BenchmarkConfig
from procurement.benchmark.service import BenchmarkService
from procurement.orchestration import workflows


@pytest.mark.parametrize("verified", [True, False])
def test_audit_resumes_frozen_selection_and_fails_on_unverified(tmp_path, monkeypatch, verified):
    (tmp_path / "selection.json").write_text("{}")
    audit = Mock(return_value={"fully_verified": verified, "quality_status": {"pass": 3}})
    monkeypatch.setattr(workflows, "run_audit", audit)
    config = workflows.QualityJobConfig(year=2024, directory=str(tmp_path))
    with build_op_context(resources={"object_storage": Mock()}) as context:
        if verified:
            assert workflows.quality_audit(context, config)["fully_verified"]
        else:
            with pytest.raises(Failure, match="requires attention"):
                workflows.quality_audit(context, config)
    assert audit.call_args.kwargs["resume"] is True


def test_quality_repair_propagates_failure_and_releases_host_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.settings, "MUASAMCONG_TOKEN", "fixture")
    lock = Mock(return_value=nullcontext())
    monkeypatch.setattr(workflows, "execution_lock", lock)
    monkeypatch.setattr(workflows, "MuasamcongClient", Mock(return_value=nullcontext(Mock())))
    repair = Mock(return_value={"status": "needs_attention", "report": "report.json"})
    monkeypatch.setattr(workflows, "run_workflow", repair)
    with (
        build_op_context(resources={"object_storage": Mock()}) as context,
        pytest.raises(Failure, match="resume"),
    ):
        workflows.quality_repair(context, workflows.QualityJobConfig(
            resource="bid_opening", year=2024, directory=str(tmp_path)))
    lock.assert_called_once()
    assert repair.call_args.kwargs["resource"] == "bid_opening"


def test_benchmark_api_cannot_start_another_control_plane(monkeypatch):
    monkeypatch.setattr(workflows.settings, "ORCHESTRATION_ENABLED", True)
    store = Mock()
    with pytest.raises(ValueError, match="Dagster"):
        BenchmarkService(store).start(BenchmarkConfig(start_date="2025-01-01", end_date="2025-01-01"))
    store.create.assert_not_called()


def test_benchmark_runs_synchronously_and_checks_terminal_status(monkeypatch):
    monkeypatch.setattr(workflows.settings, "MUASAMCONG_TOKEN", "fixture")
    monkeypatch.setattr(workflows, "execution_lock", Mock(return_value=nullcontext()))
    store = Mock()
    store.create.return_value = "fixture"
    store.get.return_value = {"status": "failed", "summary": {}}
    monkeypatch.setattr(workflows, "BenchmarkStore", Mock(return_value=store))
    runner = Mock()
    monkeypatch.setattr(workflows, "BenchmarkRunner", Mock(return_value=runner))
    with build_op_context() as context, pytest.raises(Failure, match="did not complete"):
        workflows.benchmark(context, workflows.BenchmarkJobConfig(plan_json=BenchmarkConfig(
            start_date="2025-01-01", end_date="2025-01-01").model_dump_json()))
    runner.execute.assert_called_once()
