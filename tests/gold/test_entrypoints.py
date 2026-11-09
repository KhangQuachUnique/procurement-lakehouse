from unittest.mock import Mock

from dagster import build_op_context
from typer.testing import CliRunner

from procurement.gold.adapters.driving.cli import app
from procurement.orchestration.gold_v2 import GoldRunConfig, execute_gold_v2, gold_v2_run_job


def test_cli_and_dagster_call_the_same_runtime(monkeypatch, tmp_path):
    execute = Mock(return_value={"phase": "COMMITTED", "run_id": "r1"})
    monkeypatch.setattr("procurement.gold.application.runtime.execute_run", execute)
    result = CliRunner().invoke(
        app, ["resume", "r1", str(tmp_path), "--allow-isolated-publication"]
    )
    assert result.exit_code == 0, result.output
    assert execute.call_args.args == ("r1", tmp_path)
    with build_op_context() as context:
        result = execute_gold_v2(
            context, GoldRunConfig(run_id="r1", root=str(tmp_path), allow_isolated_publication=True)
        )
    assert result["phase"] == "COMMITTED"
    assert execute.call_count == 2 and execute.call_args.kwargs["allow_isolated_publication"]


def test_runtime_refuses_remote_publication_without_explicit_opt_in(tmp_path):
    import pytest

    from procurement.gold.application.runtime import execute_run

    with pytest.raises(ValueError, match="opt-in"):
        execute_run("r1", tmp_path)
    assert not tmp_path.joinpath("state.duckdb").exists()


def test_new_job_has_one_compute_op():
    assert gold_v2_run_job.graph.node_names() == ["execute_gold_v2"]


def test_candidate_code_location_resolves_without_legacy_gold_runtime():
    from dagster import Definitions

    from procurement.orchestration.definitions import defs

    Definitions.validate_loadable(defs)
    assert defs.resolve_job_def("lakehouse_end_to_end")
    assert not any(s.name == "lakehouse_end_to_end_schedule" for s in defs.schedules)


def test_daily_adapter_fails_closed_without_current_run_silver_evidence():
    from unittest.mock import Mock

    import pytest

    from procurement.orchestration.gold_v2 import same_run_reports

    context = Mock()
    context.instance.all_logs.return_value = []
    with pytest.raises(ValueError, match="current-run"):
        same_run_reports(context)
