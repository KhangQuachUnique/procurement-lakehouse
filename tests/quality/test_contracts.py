import copy

import pytest

from procurement.quality.contracts import (
    DetailValidationError,
    Route,
    Thresholds,
    cohort_warnings,
    load_config,
    resolve_route,
    validate_detail,
)


def context():
    return {"id": "id-1", "notifyNo": "IB1", "notifyVersion": "00"}


def payload():
    return {"bidoNotifyContractorM": {**context(), "optional": None, "files": [],
                                     "amount": 0, "enabled": False, "newField": "accepted"}}


@pytest.mark.parametrize("body", [{}, {"bidoNotifyContractorM": None},
                                  {"bidoNotifyContractorM": []}, {"error": "not found"}])
def test_empty_or_wrong_root_never_becomes_valid_using_search_identity(body):
    result = validate_detail(body, context(), load_config().contracts["standard"])
    assert result["status"] == "fail"


@pytest.mark.parametrize("field", ["id", "notifyNo", "notifyVersion"])
def test_wrong_identity_or_version_is_failure(field):
    body = payload()
    body["bidoNotifyContractorM"][field] = "wrong"
    result = validate_detail(body, context(), load_config().contracts["standard"])
    assert result["status"] == "fail"
    assert any(i["code"] == "identity_mismatch" for i in result["issues"])


def test_optional_null_empty_lists_zero_false_and_unknown_fields_are_valid():
    body = payload()
    original = copy.deepcopy(body)
    result = validate_detail(body, context(), load_config().contracts["standard"])
    assert result["status"] == "pass"
    assert result["metrics"]["populated_fields"] == 6
    assert body == original


def test_missing_request_context_is_unresolved_not_pass():
    assert validate_detail(payload(), {}, load_config().contracts["standard"])["status"] == "unresolved"


def test_default_registry_generalizes_lifecycle_but_rejects_unknown_family():
    config = load_config()
    supplied = {"stepCode": "notify-contractor-step-4-kqlcnt", "processApply": "KHAC",
                "bidForm": "DTRR", "isInternet": 1, "bidMode": "1_HTHS"}
    assert len(config.routes) == 8
    assert resolve_route(supplied, config).contract == "vk_adb"
    with pytest.raises(DetailValidationError, match="unresolved"):
        resolve_route({**supplied, "processApply": "new-unknown-process"}, config)
    assert resolve_route({**supplied, "stepCode": "new-step"}, config).contract == "vk_adb"
    config.routes.append(Route(name="overlap", contract="standard", evidence="test", match=supplied))
    with pytest.raises(DetailValidationError, match="ambiguous"):
        resolve_route(supplied, config)


def test_cohort_size_similarity_is_warning_only_and_scoped():
    config = load_config()
    rows = []
    for index in range(50):
        ctx = {"id": f"{index:03d}", "notifyNo": f"IB{index:03d}", "notifyVersion": "00"}
        root = dict(ctx)
        root.update({f"optional{i}": None for i in range(20)})
        if index >= 10:
            root["description"] = "x" * 10000
        result = validate_detail({"bidoNotifyContractorM": root}, ctx, config.contracts["standard"])
        rows.append({"source_id": ctx["notifyNo"], "source_version": "00",
                     "contract": "standard", "workflow": {}, "result": result})
    cohort_warnings(rows, Thresholds())
    assert all(row["result"]["status"] in {"warn", "pass"} for row in rows)
    assert "small_empty_cluster" in {i["code"] for i in rows[0]["result"]["issues"]}
    assert "small_payload" not in {i["code"] for i in rows[-1]["result"]["issues"]}


def test_same_size_healthy_records_are_not_flagged():
    result = validate_detail(payload(), context(), load_config().contracts["standard"])
    rows = [{"source_id": "IB1", "contract": "standard", "workflow": {},
             "result": copy.deepcopy(result)} for _ in range(40)]
    assert cohort_warnings(rows, Thresholds()) == {"pass": 40}


def test_supplied_vk_adb_browser_response():
    import json
    from pathlib import Path

    body = json.loads((Path(__file__).parent / "fixtures/vk_adb_supplied.json").read_text(encoding="utf-8"))
    config = load_config()
    result = validate_detail(body, body["bidoNotifyContractorP"],
                             config.contracts["vk_adb"])
    assert result["status"] in {"pass", "warn"}
    assert result["identity"]["notifyNo"] == "IB2500002384"


def test_duplicate_representations_must_agree_on_identity():
    body = payload()
    body["bidNoContractorResponse"] = {"bidNotification": context()}
    config = load_config()
    assert validate_detail(body, context(), config.contracts["standard"])["status"] == "pass"
    body["bidNoContractorResponse"]["bidNotification"]["id"] = "another"
    result = validate_detail(body, context(), config.contracts["standard"])
    assert result["status"] == "fail"
    assert "conflicting_business_roots" in {item["code"] for item in result["issues"]}


@pytest.mark.parametrize("name", ["standard_online", "standard_offline", "reoffer", "wb",
                                  "adb", "fta", "khac_online", "khac_offline"])
def test_verified_source_workflow_fixtures(name):
    import json
    from pathlib import Path

    fixture = json.loads((Path(__file__).parent / "fixtures" / f"verified_{name}.json")
                         .read_text(encoding="utf-8"))
    config = load_config()
    route = resolve_route(fixture["context"], config)
    assert route.contract == fixture["contract"]
    result = validate_detail(fixture["payload"], fixture["context"], config.contracts[route.contract])
    assert result["status"] in {"pass", "warn"}


def test_family_rules_preserve_all_91_previous_workflows():
    import json
    from pathlib import Path

    samples = json.loads((Path(__file__).parent / "fixtures/routing_workflows.json").read_text(encoding="utf-8"))
    assert len(samples) == 91
    config = load_config()
    for sample in samples:
        assert resolve_route(sample["context"], config).contract == sample["contract"]


@pytest.mark.parametrize("process,form,expected", [
    ("KHAC", "CGTTRG", "reoffer"), ("LDT", "CGTTRG", "reoffer"),
    ("KHAC", "TCTVCN", "vk_adb"), ("LDT", "DTHC", "standard"),
    ("LDT", "TVCN", "standard"), ("UKFTA", "DTRR", "standard"),
    ("EVFTA", "DTRR", "standard"), ("CPTPP", "DTRR", "standard"),
    ("WB", "DTRR", "vk_adb"), ("ADB", "DTRR", "vk_adb"),
])
def test_family_routing_ignores_lifecycle_mode_and_internet(process, form, expected):
    ctx = {"processApply": process, "bidForm": form, "stepCode": "new-step",
           "isInternet": 0, "bidMode": "2_MTHS"}
    assert resolve_route(ctx, load_config()).contract == expected
