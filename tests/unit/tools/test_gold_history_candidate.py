import json
import zipfile
from datetime import date

import pytest

from procurement.tools import gold_history_candidate as history

DAY = date(2025, 1, 1)


def bundle(path, resources=history.RESOURCES, day=DAY):
    transfer = {"format_version": 2, "days": [], "objects": {}}
    with zipfile.ZipFile(path, "w") as archive:
        for name in resources:
            key = f"_control/{name}/day.json"
            raw = json.dumps(
                {
                    "status": "success",
                    "source_date": str(day),
                    "resource": name,
                    "run_id": "test",
                    "bronze_records": 0,
                    "completed_pages": 1,
                    "expected_pages": 1,
                    "error_count": 0,
                }
            ).encode()
            archive.writestr("objects/" + key, raw)
            transfer["days"].append(
                {"resource": name, "source_date": str(day), "run_id": "test", "objects": [key]}
            )
            transfer["objects"][key] = {"sha256": history.digest(raw), "size": len(raw)}
        archive.writestr("transfer.json", json.dumps(transfer))
    return path


def test_coverage_union_missing_and_duplicate(tmp_path):
    original = bundle(tmp_path / "original.zip", history.RESOURCES[:2])
    supplement = bundle(tmp_path / "supplement.zip", history.RESOURCES[2:])
    with pytest.raises(ValueError, match="Missing source day"):
        history.inventory([original], DAY, DAY)
    selected, _ = history.inventory([original, supplement], DAY, DAY)
    assert len(selected) == 3
    with pytest.raises(ValueError, match="Duplicate resource/day"):
        history.inventory([original, supplement, supplement], DAY, DAY)


def test_resume_reuses_only_hash_verified_checkpoints(tmp_path, monkeypatch):
    archive = bundle(tmp_path / "source.zip")
    output = tmp_path / "candidate"
    calls = []

    def interrupt(state):
        calls.append(state["current"])
        if len(calls) == 1:
            raise RuntimeError("Injected interruption")

    with pytest.raises(RuntimeError, match="Injected interruption"):
        history.run([archive], DAY, DAY, output, on_progress=interrupt)
    assert len(list(output.glob("days/*/*/accepted.json"))) == 1
    first = next(output.glob("days/*/*/accepted.json"))
    mtime = first.stat().st_mtime_ns
    result = history.run([archive], DAY, DAY, output)
    assert result["status"] == "LOCAL_SILVER_VALIDATED"
    assert result["completed_resource_days"] == 3
    assert first.stat().st_mtime_ns == mtime
    assert result["historical_silver_ready"] is False
    assert result["production_written"] is False
    receipt = json.loads(first.read_text())
    target = history.Path(receipt["files"][0]["path"])
    target.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="Checkpoint output corrupted"):
        history.run([archive], DAY, DAY, output)


def test_changed_mapper_cannot_resume(tmp_path, monkeypatch):
    archive = bundle(tmp_path / "source.zip")
    output = tmp_path / "candidate"
    history.run([archive], DAY, DAY, output)
    monkeypatch.setattr(history, "mapper_fingerprint", lambda: "changed")
    with pytest.raises(ValueError, match="Immutable full-history plan differs"):
        history.run([archive], DAY, DAY, output)


def test_budget_failure_keeps_no_false_pass(tmp_path):
    archive = bundle(tmp_path / "source.zip")
    output = tmp_path / "candidate"
    with pytest.raises(RuntimeError, match="wall-time"):
        history.run([archive], DAY, DAY, output, max_seconds=0)
    state = json.loads((output / "job.json").read_text())
    assert state["status"] == "FAILED"
    assert state["completed_resource_days"] == 0
