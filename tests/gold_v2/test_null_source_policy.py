from copy import deepcopy
from datetime import date
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.gold.v2.contracts import SilverReadyInput
from procurement.readiness_with_exception import seal_with_null_policy
from procurement.source_quality import NULL_POLICY_DIGEST, null_path, verify_null_report
from tests.gold_v2.test_source_exception import fixture_report


def test_multiple_nulls_same_cohort_reconcile_and_keep_raw(tmp_path):
    reports = fixture_report(tmp_path)
    report = reports[0]
    entry = report["days"][0]
    files = entry["files"]
    obs = pq.ParquetFile(files["observation"]).read().to_pylist()
    bad = obs[0] | {
        "observation_id": "another_bad_observation",
        "source_id": "another_source",
        "row_ordinal": 537,
    }
    obs.append(bad)
    pq.write_table(pa.Table.from_pylist(obs), files["observation"])
    for name in ("quarantine", "quality_issue"):
        rows = pq.ParquetFile(files[name]).read().to_pylist()
        rows.append(rows[0] | {"observation_id": bad["observation_id"]})
        pq.write_table(pa.Table.from_pylist(rows), files[name])
    entry["quality"].update(observations=3, quarantined=2, blocking_issues=2)
    proofs = verify_null_report(report)
    assert len(proofs) == 2
    before = deepcopy(reports)
    cat = Mock()
    cat.table.side_effect = lambda ns, t: {
        "metadata": {
            "table-uuid": "uuid_" + t,
            "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1}],
        }
    }
    bundle = seal_with_null_policy(
        reports,
        start=date(2024, 4, 10),
        end=date(2024, 4, 10),
        namespace="silver",
        catalog=cat,
        count_rows=lambda *_: 3,
    )
    ready = SilverReadyInput.model_validate(bundle["receipt"])
    assert ready.receipt_version == 3 and ready.quarantined == ready.blocking_issues == 2
    assert ready.approval_digest == NULL_POLICY_DIGEST and reports == before
    assert len(ready.approved_exceptions) == 2
    with pytest.raises(ValueError):
        SilverReadyInput.model_validate(bundle["receipt"] | {"quarantined": 1})
    with pytest.raises(ValueError):
        SilverReadyInput.model_validate(bundle["receipt"] | {"approval_digest": "old-policy"})


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"error": "unauthorized"},
        {"bidPoBidpPlanProjectDetailView": {}},
        {"bidPoBidpPlanProjectDetailView": {"id": "valid"}},
        {"bidPoBidpPlanProjectDetailView": "invalid-string"},
    ],
)
def test_missing_malformed_or_valid_roots_are_not_blanket_exceptions(payload):
    with pytest.raises(ValueError):
        null_path(payload, "khlcnt_plan_detail")


def test_unknown_error_still_blocks(tmp_path):
    report = fixture_report(tmp_path)[0]
    files = report["days"][0]["files"]
    pq.write_table(
        pa.Table.from_pylist([{"observation_id": "unrelated", "reason": "invalid_money"}]),
        files["quarantine"],
    )
    with pytest.raises(ValueError, match="non-null"):
        verify_null_report(report)


def test_quarantined_observation_cannot_contribute_domain_revision(tmp_path):
    report = fixture_report(tmp_path)[0]
    files = report["days"][0]["files"]
    bad = pq.ParquetFile(files["quarantine"]).read().to_pylist()[0]["observation_id"]
    domain = next(path for name, path in files.items() if name.endswith("_revision"))
    pq.write_table(pa.table({"observation_id": [bad]}), domain)
    with pytest.raises(ValueError, match="Domain leakage"):
        verify_null_report(report)


@pytest.mark.parametrize(
    ("table", "payload", "path"),
    [
        (
            "khlcnt_plan_detail",
            {"bidPoBidpPlanProjectDetailView": None},
            "bidPoBidpPlanProjectDetailView",
        ),
        (
            "contractor_result_detail",
            {"bideContractorInputResultDTO": None},
            "bideContractorInputResultDTO",
        ),
        (
            "notify_contractor_standard_detail",
            {"bidNoContractorResponse": {"bidNotification": None}},
            "bidNoContractorResponse.bidNotification",
        ),
        (
            "notify_contractor_vk_adb_detail",
            {"bidoNotifyContractorP": None},
            "bidoNotifyContractorP",
        ),
        ("khlcnt_bid_package_detail", None, "$"),
        ("notify_contractor_reoffer_detail", None, "$"),
    ],
)
def test_explicit_null_roots_supported_without_treating_optional_columns_as_null_record(
    table, payload, path
):
    assert null_path(payload, table) == path


def test_present_notice_mirror_is_not_an_empty_record():
    with pytest.raises(ValueError, match="root exists"):
        null_path(
            {
                "bidoNotifyContractorM": None,
                "bidNoContractorResponse": {"bidNotification": {"notifyNo": "valid"}},
            },
            "notify_contractor_standard_detail",
        )


def test_payload_hash_and_domain_reconciliation_are_required(tmp_path):
    report = fixture_report(tmp_path)[0]
    files = report["days"][0]["files"]
    obs = pq.ParquetFile(files["observation"]).read().to_pylist()
    obs[0]["content_hash"] = "0" * 64
    pq.write_table(pa.Table.from_pylist(obs), files["observation"])
    with pytest.raises(ValueError, match="hash"):
        verify_null_report(report)
