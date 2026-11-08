import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from procurement.tools.backfill_contract_performance import verify_pilot


class PilotRepo:
    def __init__(self):
        self.rows = [{"id": "report", "version": "00", "publicDate": "2025-10-17T09:00:00"}]

    def select_committed_partitions(self, resources, first, last):
        return [SimpleNamespace(source_date=str(first + timedelta(days=n))) for n in range(7)]

    def stream_partition_records(self, partition, **kwargs):
        # Existing adapter verifies/streams all families before the caller filters.
        yield (
            {"table": "contract_detail"},
            0,
            {"payload": json.dumps({"contract": {"id": "contract"}})},
        )
        if partition.source_date == "2025-10-17":
            for n, row in enumerate(self.rows):
                yield (
                    {"table": "contract_performance_detail"},
                    n,
                    {"payload": json.dumps({"performance": row})},
                )


def test_pilot_gate_checks_ids_dates_and_filters_verified_stream():
    result = verify_pilot(PilotRepo(), {("report", "00"): "2025-10-17"})
    assert result["status"] == "passed"
    assert result["daily_reports"] == 1


def test_pilot_gate_rejects_missing_and_duplicate_reports():
    repo = PilotRepo()
    with pytest.raises(ValueError, match="mismatch"):
        verify_pilot(repo, {("other-report", "00"): "2025-10-17"})
    repo.rows.append(repo.rows[0].copy())
    with pytest.raises(ValueError, match="duplicate"):
        verify_pilot(repo, {("report", "00"): "2025-10-17"})


def test_pilot_gate_rejects_timezone_boundary_mismatch():
    repo = PilotRepo()
    repo.rows[0]["publicDate"] = "2025-10-18T00:00:00"
    with pytest.raises(ValueError, match="source partition"):
        verify_pilot(repo, {("report", "00"): "2025-10-18"})


def test_silver_only_never_constructs_a_source_client(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    import procurement.storage.object_store as storage
    import procurement.tools.backfill_contract_performance as module

    output = tmp_path / "run.json"
    output.write_text(
        json.dumps(
            {
                "resource": "contract_performance",
                "year": 2025,
                "days": {},
                "source_error": {"http_status": 400, "effective_status": 429},
            }
        )
    )
    client = MagicMock(side_effect=AssertionError("Must not contact source"))
    monkeypatch.setattr(module, "MuasamcongClient", client)
    monkeypatch.setattr(module, "create_application_engine", MagicMock())
    monkeypatch.setattr(storage, "create_s3_filesystem", MagicMock())
    repo = MagicMock()
    repo.select_committed_partitions.return_value = []
    monkeypatch.setattr(module, "ArrowBronzeRepositoryAdapter", MagicMock(return_value=repo))
    assert module.main(["--silver-only", "--output", str(output)]) == 0
    client.assert_not_called()
    result = json.loads(output.read_text())
    assert result["status"] == "blocked"
    assert result["source_requests_in_silver_only"] == 0
    assert len(result["pending_dates"]) == 365
