import json
from copy import deepcopy
from datetime import UTC, date, datetime
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.gold.domain.models import SilverReadyInput
from procurement.readiness_with_exception import seal_with_exception
from procurement.source_quality import (
    APPROVAL_DIGEST,
    APPROVED,
    SourceException,
    verify_exception_report,
)
from tests.gold.test_readiness_producer import reports


def fixture_report(tmp_path):
    oid = APPROVED["observation_id"]
    payload = {
        "bidPoBidpPlanProjectDetailView": None,
        "bidpPlanDetailToProjectList": None,
        "project": None,
        "lsBidpPlanDetailDTO": None,
        "correctPacks": None,
    }
    row = {k: v for k, v in APPROVED.items() if k != "reason"}
    row["payload_json"] = json.dumps(payload)
    row["source_date"] = date(2024, 4, 10)
    valid = row | {"observation_id": "valid", "source_id": "other"}
    entries = {
        "observation": pa.Table.from_pylist([row, valid]),
        "quarantine": pa.Table.from_pylist([{"observation_id": oid, "reason": "missing_root"}]),
        "quality_issue": pa.Table.from_pylist(
            [{"observation_id": oid, "code": "missing_root", "path": "root", "severity": "error"}]
        ),
        "plan_revision": pa.table({"observation_id": ["valid"]}),
        "package_revision": pa.table({"observation_id": pa.array([], type=pa.string())}),
    }
    files = {}
    for name, table in entries.items():
        path = tmp_path / (name + ".parquet")
        pq.write_table(table, path)
        files[name] = str(path)
    result = reports()
    for r in result:
        r["days"][0]["date"] = "2024-04-10"
    bad = result[0]
    e = bad["days"][0]
    e.update(
        status="needs_review",
        files=files,
        quality={"accepted": 1, "observations": 2, "quarantined": 1, "blocking_issues": 1},
        input_files=[
            {
                "table": APPROVED["table_name"],
                "key": APPROVED["file_key"],
                "sha256": APPROVED["file_sha256"],
            }
        ],
    )
    return result


def test_v2_preserves_actual_errors_and_original_report(tmp_path):
    source = fixture_report(tmp_path)
    original = deepcopy(source)
    catalog = Mock()
    catalog.table.side_effect = lambda ns, t: {
        "metadata": {
            "table-uuid": "uuid_" + t,
            "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1}],
        }
    }
    bundle = seal_with_exception(
        source,
        start=date(2024, 4, 10),
        end=date(2024, 4, 10),
        namespace="test_silver_v2_exception",
        catalog=catalog,
        count_rows=lambda *_: 2,
        completed_at=datetime(2026, 11, 7, tzinfo=UTC),
    )
    ready = SilverReadyInput.model_validate(bundle["receipt"])
    assert ready.quarantined == ready.blocking_issues == 1
    assert ready.quality_status == "ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION"
    assert ready.approval_digest == APPROVAL_DIGEST
    assert source == original
    assert source[0]["days"][0]["status"] == "needs_review"


@pytest.mark.parametrize(
    "change",
    [
        {"approval_digest": "different"},
        {"quarantined": 2},
        {"blocking_issues": 0},
        {"approved_exceptions": []},
        {"receipt_version": 1},
        {"approved_exceptions": [APPROVED, APPROVED]},
        {"approved_exceptions": [APPROVED | {"content_hash": "different"}]},
        {"approved_exceptions": [APPROVED | {"row_ordinal": 537}]},
    ],
)
def test_no_broad_or_silent_exception(plan, change):
    value = plan.source.model_dump(mode="json") | {
        "receipt_version": 2,
        "quarantined": 1,
        "blocking_issues": 1,
        "approval_digest": APPROVAL_DIGEST,
        "approved_exceptions": [APPROVED],
        "quality_status": "ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION",
    }
    with pytest.raises(ValueError):
        SilverReadyInput.model_validate(value | change)


def test_new_error_and_domain_leak_still_block(tmp_path):
    source = fixture_report(tmp_path)
    report = source[0]
    assert verify_exception_report(report) == SourceException(**APPROVED)
    file = report["days"][0]["files"]["plan_revision"]
    pq.write_table(pa.table({"observation_id": [APPROVED["observation_id"]]}), file)
    with pytest.raises(ValueError, match="leaked"):
        verify_exception_report(report)
    pq.write_table(pa.table({"observation_id": ["valid"]}), file)
    file = report["days"][0]["files"]["quality_issue"]
    rows = pq.ParquetFile(file).read().to_pylist()
    rows.append(rows[0] | {"code": "new_error"})
    pq.write_table(pa.Table.from_pylist(rows), file)
    with pytest.raises(ValueError, match="Unapproved"):
        verify_exception_report(report)


def test_same_count_different_quarantine_is_not_allowed(tmp_path):
    report = fixture_report(tmp_path)[0]
    pq.write_table(
        pa.Table.from_pylist([{"observation_id": "some_other_record", "reason": "missing_root"}]),
        report["days"][0]["files"]["quarantine"],
    )
    with pytest.raises(ValueError, match="Unapproved"):
        verify_exception_report(report)
