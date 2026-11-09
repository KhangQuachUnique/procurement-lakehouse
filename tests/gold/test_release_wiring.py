import tomllib
from pathlib import Path
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from procurement.gold.adapters.driving.cli import app
from procurement.storage.gold_views import ensure_gold_views


def test_release_entrypoints_share_the_only_gold_runtime():
    config = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())
    assert config["project"]["scripts"]["gold"] == "procurement.gold.adapters.driving.cli:main"
    from procurement.orchestration.definitions import defs

    assert defs.resolve_job_def("gold_v2_run_job")
    assert not any(s.default_status.value == "RUNNING" for s in defs.schedules)
    result = CliRunner().invoke(app, ["schema"])
    assert result.exit_code == 0
    import json

    assert len(json.loads(result.stdout)) == 16


def test_ui_reader_requires_explicit_v2_target_before_remote_access(monkeypatch):
    publisher = Mock()
    monkeypatch.setattr("procurement.storage.gold_views.IcebergPublisher", publisher)
    with pytest.raises(ValueError, match="explicit"):
        ensure_gold_views(Mock(), Mock(), target_namespace="gold_reporting")
    publisher.assert_not_called()
    publisher.return_value.ensure_views.return_value = {"tables": {"dim_date": {}}}
    assert ensure_gold_views(Mock(), Mock(), target_namespace="gold_v2_accepted") == ["dim_date"]
    call = publisher.return_value.ensure_views.call_args
    assert call.args[0].target_namespace == "gold_v2_accepted"
    assert call.kwargs["view_schema"] == "gold"


@pytest.mark.parametrize("version", [2, 3])
def test_daily_can_consume_approved_historical_bundle_without_erasing_quality(
    plan, tmp_path, version
):
    from procurement.orchestration.gold_v2 import load_prior_reports
    from procurement.source_quality import APPROVAL_DIGEST, APPROVED, NULL_POLICY_DIGEST

    value = plan.source.model_dump(mode="json") | {
        "receipt_version": version,
        "quarantined": 1,
        "blocking_issues": 1,
        "approved_exceptions": [
            APPROVED | ({"root_path": "bidPoBidpPlanProjectDetailView"} if version == 3 else {})
        ],
        "approval_digest": NULL_POLICY_DIGEST if version == 3 else APPROVAL_DIGEST,
        "quality_status": "ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION",
    }
    evidence = {
        "run_id": "published",
        "resource": "khlcnt",
        "mapping_version": "khlcnt-2",
        "entry": {
            "date": "2024-04-10",
            "status": "needs_review",
            "quality": {"quarantined": 1, "blocking_issues": 1},
        },
    }
    import json

    p = tmp_path / "baseline.json"
    p.write_text(json.dumps({"receipt": value, "evidence": [evidence]}))
    reports = load_prior_reports(p, "silver")
    assert reports[0]["days"][0]["status"] == "needs_review"
    assert reports[0]["days"][0]["quality"]["quarantined"] == 1
    with pytest.raises(ValueError, match="namespace"):
        load_prior_reports(p, "different_source")
