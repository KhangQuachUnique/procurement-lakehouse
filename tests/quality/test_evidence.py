import pytest

from procurement.quality.contracts import ENDPOINTS, load_config
from procurement.quality.evidence import (
    ProbeBudget,
    ProbeLimitError,
    classify_sample,
    probe,
    sample_workflows,
)
from procurement.quality.files import read_json


def test_sampling_early_middle_late_and_small_groups():
    days = {f"2025-01-0{i}": [{"id": str(i), "stepCode": "workflow"}] for i in range(1, 6)}
    group = sample_workflows(days)[0]
    assert [r["id"] for r in group["samples"]] == ["1", "3", "5"]
    assert len(sample_workflows({"2025-01-01": [{"id": "one"} ]})[0]["samples"]) == 1


def test_budget_counts_each_attempt():
    persisted = []
    budget = ProbeBudget(2, checkpoint=persisted.append)
    for _ in range(2):
        with budget.request():
            pass
    with pytest.raises(ProbeLimitError), budget.request():
        pass
    assert persisted == [1, 2]


def test_http_errors_and_multiple_valid_endpoints_are_not_promoted():
    checks = {kind: {"result": {"status": "pass"}} for kind in ENDPOINTS}
    assert classify_sample(checks) == ("ambiguous", None)
    checks["vk_adb"] = {"error": "connection reset"}
    assert classify_sample(checks) == ("unresolved", None)


def test_verification_needs_positive_identity_valid_detail_not_http_errors():
    checks = {kind: {"error": "source rejected this request", "http_status": 500}
              for kind in ENDPOINTS}
    assert classify_sample(checks) == ("unresolved", None)
    checks["standard"] = {"result": {"status": "pass"}}
    assert classify_sample(checks) == ("verified", "standard")
    checks["reoffer"] = {"error": "rate limited", "http_status": 429}
    assert classify_sample(checks) == ("unresolved", None)


def test_probe_validates_identity_and_preserves_endpoint_evidence(tmp_path):
    context = {"id": "notice", "notifyNo": "IB1", "notifyVersion": "00"}
    groups = sample_workflows({"2025-01-01": [context]})
    class Client:
        def post(self, endpoint, body):
            assert body == {"id": "notice"}
            return {"bidoNotifyContractorP": context} if endpoint == ENDPOINTS["vk_adb"] else {}
    output = tmp_path / "evidence.json"
    probe(Client(), groups, load_config(), output, {"workflows": {}})
    report = read_json(output)
    group = next(iter(report["workflows"].values()))
    assert group["status"] == "verified"
    assert group["contract"] == "vk_adb"
    assert len(next(iter(group["checks"].values()))["endpoints"]) == 3


def test_export_config_round_trip_and_no_unresolved_promotion(tmp_path):
    from procurement.quality.contracts import Route
    from procurement.quality.evidence import export_config

    config = load_config()
    workflow = {"stepCode": "notify-contractor-step-4-kqlcnt", "processApply": "KHAC",
                "bidForm": "DTRR", "isInternet": 1, "bidMode": "1_HTHS"}
    config.routes = [Route(name="seed", contract="vk_adb", evidence="test fixture",
                           match={**workflow, "id": "known-notice"})]
    report = {"status": "complete", "config_hash": config.fingerprint, "workflows": {
        "verified": {"status": "verified", "contract": "vk_adb", "workflow": workflow},
        "unknown": {"status": "unresolved", "contract": None, "workflow": workflow},
    }}
    path = tmp_path / "reviewed.toml"
    exported = export_config(report, config, path)
    assert exported.fingerprint == load_config(path).fingerprint
    assert len(exported.routes) == 1
    assert exported.routes[0].name == "observed-verified"


def test_export_preserves_family_exclusions_without_adding_overlaps(tmp_path):
    from procurement.quality.contracts import resolve_route
    from procurement.quality.evidence import export_config

    config = load_config()
    workflow = {"stepCode": "notify-contractor-step-2-kqmt", "processApply": "KHAC",
                "bidForm": "TCTVCN", "isInternet": 0, "bidMode": "1_MTHS"}
    report = {"status": "complete", "config_hash": config.fingerprint, "workflows": {
        "verified": {"status": "verified", "contract": "vk_adb", "workflow": workflow},
    }}
    path = tmp_path / "family.toml"
    exported = export_config(report, config, path)
    restored = load_config(path)
    assert restored.fingerprint == exported.fingerprint
    assert len(restored.routes) == len(config.routes)
    assert resolve_route(workflow, restored).contract == "vk_adb"
    assert resolve_route({**workflow, "bidForm": "CGTTRG"}, restored).contract == "reoffer"
    report["workflows"]["verified"]["contract"] = "standard"
    with pytest.raises(ValueError, match="conflicts"):
        export_config(report, config, tmp_path / "conflict.toml")
