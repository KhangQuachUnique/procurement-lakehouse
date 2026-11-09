from datetime import date
from unittest.mock import Mock

import pytest

from procurement.common.silver_gold_contract import INPUT_TABLES
from procurement.tools.gold_retained_scope import RetainedFence, retained_vector


def test_scope_refuses_shared_snapshot_from_next_year():
    report = {"resource": "khlcnt", "days": [{"date": "2023-12-31", "iceberg": {},
              "publication_batch": {"first": "khlcnt/2023-12-29", "last": "notify_contractor/2024-01-01"}}]}
    with pytest.raises(ValueError, match="shared Silver batch"):
        retained_vector([report], date(2022, 1, 1), date(2023, 12, 31))


def test_vector_is_chronological_not_current_and_retention_is_checked():
    def report(day, sid):
        return {"resource": "khlcnt", "days": [{"date": day, "iceberg": {
            t: {"table_uuid": "uuid", "snapshot_id": sid} for t in INPUT_TABLES}}]}
    reports, vector = retained_vector([report("2023-01-01", 2), report("2022-12-31", 1)],
                                     date(2022, 1, 1), date(2022, 12, 31))
    assert len(reports) == 1 and vector["observation"]["snapshot_id"] == 1
    cat = Mock()
    meta = {"table-uuid": "uuid", "current-snapshot-id": 2,
            "snapshots": [{"snapshot-id": 1}, {"snapshot-id": 2, "parent-snapshot-id": 1}]}
    cat.table.return_value = {"metadata": meta}
    fence = RetainedFence(cat, "test_silver_v2_scope", vector)
    assert fence.table("test_silver_v2_scope", "observation")["metadata"]["current-snapshot-id"] == 1
    assert meta["current-snapshot-id"] == 2
    meta["snapshots"] = [{"snapshot-id": 2}]
    with pytest.raises(ValueError, match="retained ancestry"):
        fence.table("test_silver_v2_scope", "observation")
