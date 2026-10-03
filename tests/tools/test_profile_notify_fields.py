import csv
import json
from datetime import date

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.storage.committed import CommittedDay
from procurement.tools.profile_notify_fields import (
    ROOTS,
    TableProfile,
    field_rows,
    profile_day,
    profile_record,
    run_profile,
)


def record(payload, source_date="2025-01-01"):
    return {"payload": json.dumps(payload), "source_id": "IB1", "source_version": "00",
            "run_id": "run", "source_date": source_date,
            "content_hash": calculate_content_hash(payload)}


def test_array_counts_use_records_not_elements_and_preserve_zero_false():
    table = "notify_contractor_reoffer_detail"
    profile = TableProfile()
    profile_record(profile, table, record({
        "lots": [{"value": 0}, {"value": None}, {"value": False}],
        "blank": "  ", "empty": [], "a.b": 1, "a": {"b": 2},
    }))
    profile_record(profile, table, record({"lots": [], "blank": None}))
    rows = {row["field"]: row for row in field_rows({table: profile}, "paths")}
    values = rows['$["lots"][]["value"]']
    assert values["present_records"] == 1
    assert values["populated_records"] == 1
    assert values["null_records"] == 1
    assert values["occurrences"] == 3
    assert values["populated_occurrences"] == 2
    assert values["populated_pct"] == 50
    assert rows['$["blank"]']["empty_records"] == 2
    assert '$["a.b"]' in rows and '$["a"]["b"]' in rows


def test_root_fallback_is_whole_object_and_missing_roots_stay_in_denominator():
    table = "notify_contractor_standard_detail"
    profile = TableProfile()
    profile_record(profile, table, record({"bidoNotifyContractorM": None}))
    profile_record(profile, table, record({
        "bidoNotifyContractorM": {},
        "bidNoContractorResponse": {"bidNotification": {"id": "fallback", "bidId": "b"}},
    }))
    result = profile_record(profile, table, record({
        "bidoNotifyContractorM": {"id": "primary"},
        "bidNoContractorResponse": {"bidNotification": {"bidId": "do-not-fill"}},
    }))
    assert profile.missing_business_root == 1
    assert profile.business_fields["id"].populated_records == 2
    assert profile.business_fields["bidId"].populated_records == 1
    assert result["business_fields"] == 1
    rows = list(field_rows({table: profile}, "business_fields"))
    assert all(row["total_records"] == 3 for row in rows)


def make_day(tmp_path, number, table, payload):
    day = date(2025, 1, number)
    path = tmp_path / f"day-{number}.parquet"
    pq.write_table(pa.Table.from_pylist([record(payload, str(day))]), path)
    return CommittedDay(day, "run", 1, ((table, str(path)),))


def test_report_merges_verified_days_and_writes_per_record_csv(tmp_path):
    standard, reoffer, vk = ROOTS
    selection = (
        make_day(tmp_path, 1, standard, {"bidoNotifyContractorM": {"id": "a"}}),
        make_day(tmp_path, 2, standard, {"bidoNotifyContractorM": None}),
        make_day(tmp_path, 3, reoffer, {"id": "b", "priceInit": 0}),
        make_day(tmp_path, 4, vk, {"bidoNotifyContractorP": {"id": "c"}}),
        CommittedDay(date(2025, 1, 5), "run", 0, ()),
    )
    output = tmp_path / "report"
    output.mkdir()
    summary = run_profile(fsspec.filesystem("file"), selection, output, 2025, 2)
    assert summary["records"] == 4
    assert summary["tables"][standard]["missing_business_root"] == 1
    with (output / "business-fields.csv").open(encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))
    standard_id = next(row for row in rows if row["table"] == standard and row["field"] == "id")
    assert standard_id["populated_pct"] == "50.0"
    with (output / "records.csv").open(encoding="utf-8-sig") as file:
        assert len(list(csv.DictReader(file))) == 4
    assert (output / "summary.json").exists()


def test_count_mismatch_cannot_produce_completed_report(tmp_path):
    output = tmp_path / "report"
    output.mkdir()
    day = CommittedDay(date(2025, 1, 1), "run", 1, ())
    with pytest.raises(ValueError, match="count mismatch"):
        run_profile(fsspec.filesystem("file"), (day,), output, 2025, 1)
    assert not (output / "summary.json").exists()


def test_profile_verifies_hash_before_counting(tmp_path):
    path = tmp_path / "tampered.parquet"
    item = record({"id": "original"})
    item["payload"] = '{"id":"changed"}'
    pq.write_table(pa.Table.from_pylist([item]), path)
    day = CommittedDay(date(2025, 1, 1), "run", 1,
                       (("notify_contractor_reoffer_detail", str(path)),))
    with pytest.raises(ValueError, match="hash mismatch"):
        profile_day(fsspec.filesystem("file"), day)
