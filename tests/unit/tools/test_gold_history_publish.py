import json
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.tools import gold_history_candidate as candidate
from procurement.tools import gold_history_publish as publication
from tests.unit.tools.test_gold_history_candidate import DAY, bundle


def test_writer_inputs_keep_empty_result_children_and_governance(tmp_path):
    tables = {
        "observation": pa.table({"observation_id": ["o"]}),
        "result_revision": pa.table({"revision_id": ["r"]}),
        "award_lot": pa.table({"revision_id": pa.array([], type=pa.string())}),
        "award_contractor": pa.table({"revision_id": pa.array([], type=pa.string())}),
        "quality_issue": pa.table({"observation_id": pa.array([], type=pa.string())}),
        "quarantine": pa.table({"observation_id": pa.array([], type=pa.string())}),
    }
    files = {}
    for name, table in tables.items():
        path = tmp_path / (name + ".parquet")
        pq.write_table(table, path)
        files[name] = str(path)
    selected = publication.table_inputs(
        {"resource": "contractor_result", "days": [{"files": files}]}
    )
    assert set(selected) == set(tables)
    assert len(selected["award_contractor"]) == 0
    assert len(selected["quarantine"]) == 0


def test_empty_history_resume_and_corrupt_source_rejection(tmp_path, monkeypatch):
    archive = bundle(tmp_path / "raw.zip")
    source = tmp_path / "source"
    candidate.run([archive], DAY, DAY, source)
    catalog = Mock()
    catalog.table.side_effect = lambda ns, name: {
        "metadata": {"table-uuid": "uuid_" + name, "current-snapshot-id": None, "snapshots": []}
    }
    db = Mock()
    stage = Mock(return_value={})
    monkeypatch.setattr(publication, "stage_partition", stage)
    monkeypatch.setattr(publication, "ensure_views", lambda *_: None)
    output = tmp_path / "publication"
    result = publication.publish(source, output, db, catalog, namespace="test_silver_v2_fixture")
    assert result["status"] == "SILVER_READY"
    assert result["completed_resource_days"] == 3
    assert result["production_written"] is False
    first_calls = stage.call_count
    again = publication.publish(source, output, db, catalog)
    assert again["status"] == "SILVER_READY"
    assert stage.call_count == first_calls
    ready = json.loads((output / "silver-ready.json").read_text())
    from procurement.source_quality import NULL_POLICY_DIGEST

    assert ready["receipt"]["receipt_version"] == 3
    assert ready["receipt"]["approval_digest"] == NULL_POLICY_DIGEST
    assert ready["receipt"]["quarantined"] == 0
    assert ready["receipt"]["quality_status"] == "STRICT"
    assert all(s["rows"] == 0 for s in ready["receipt"]["tables"])
    receipt = json.loads(next(source.glob("days/*/*/accepted.json")).read_text())
    Path(receipt["files"][0]["path"]).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="corrupted"):
        publication.publish(source, output, db, catalog)


def test_production_namespace_rejected_before_any_catalog_request(tmp_path, monkeypatch):
    archive = bundle(tmp_path / "raw.zip")
    source = tmp_path / "source"
    candidate.run([archive], DAY, DAY, source)
    catalog, db = Mock(), Mock()
    with pytest.raises(ValueError, match="private test_silver_v2_"):
        publication.publish(source, tmp_path / "publication", db, catalog, namespace="silver")
    assert not catalog.mock_calls
    assert not db.mock_calls


@pytest.mark.parametrize("batch_days", [1, 7])
def test_prefix_receipt_is_frozen_before_suffix_and_not_resealed_on_resume(
    tmp_path, monkeypatch, batch_days
):
    from datetime import date

    before = date(2024, 12, 31)
    after = date(2025, 1, 1)
    a = bundle(tmp_path / "2024.zip", day=before)
    b = bundle(tmp_path / "2025.zip", day=after)
    source = tmp_path / "source"
    candidate.run([a, b], before, after, source)
    catalog, db = Mock(), Mock()
    catalog.table.side_effect = lambda ns, name: {
        "metadata": {"table-uuid": "uuid_" + name, "current-snapshot-id": None, "snapshots": []}
    }
    monkeypatch.setattr(publication, "stage_partition", Mock(return_value={}))
    monkeypatch.setattr(publication, "ensure_views", lambda *_: None)
    output = tmp_path / "publication"
    publication.publish(
        source, output, db, catalog, namespace="test_silver_v2_prefix", batch_days=batch_days
    )
    prefix = output / "silver-ready-through-2024-12-31.json"
    first_hash = candidate.file_hash(prefix)
    assert json.loads(prefix.read_text())["receipt"]["source_end"] == str(before)
    assert json.loads((output / "silver-ready.json").read_text())["receipt"]["source_end"] == str(
        after
    )
    publication.publish(source, output, db, catalog, batch_days=batch_days)
    assert candidate.file_hash(prefix) == first_hash


def test_batched_execution_is_frozen_without_changing_original_plan(tmp_path, monkeypatch):
    source = tmp_path / "source"
    candidate.run([bundle(tmp_path / "raw.zip")], DAY, DAY, source)
    catalog, db = Mock(), Mock()
    catalog.table.side_effect = lambda ns, name: {
        "metadata": {"table-uuid": "uuid_" + name, "current-snapshot-id": None, "snapshots": []}
    }
    monkeypatch.setattr(publication, "stage_partition", Mock(return_value={}))
    monkeypatch.setattr(publication, "ensure_views", lambda *_: None)
    output = tmp_path / "publication"
    publication.publish(source, output, db, catalog, batch_days=7)
    before = candidate.file_hash(output / "plan.json")
    publication.publish(source, output, db, catalog, batch_days=7)
    assert candidate.file_hash(output / "plan.json") == before
    with pytest.raises(ValueError, match="strategy differs"):
        publication.publish(source, output, db, catalog, batch_days=1)
