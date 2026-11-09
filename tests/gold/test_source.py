from unittest.mock import Mock

import pytest

from procurement.gold.adapters.driven.silver.iceberg_reader import IcebergReader
from procurement.gold.application.source import freeze
from tests.gold.test_business import populated
from tests.gold.test_pipeline import Reader


def test_source_receipt_checks_uuid_and_retained_snapshot(plan):
    catalog = Mock()
    catalog.table.side_effect = lambda ns, name: {
        "metadata": {"table-uuid": "uuid_" + name, "snapshots": [{"snapshot-id": 1}]}
    }
    reader = IcebergReader(None, catalog, "silver")
    reader.verify(plan.source)
    catalog.table.return_value = {
        "metadata": {"table-uuid": "wrong", "snapshots": [{"snapshot-id": 1}]}
    }
    catalog.table.side_effect = None
    with pytest.raises(ValueError, match="identity changed"):
        reader.verify(plan.source)


def test_partial_source_capture_resumes_without_rereading_completed_tables(db, plan):
    populated(db)
    reader = Reader(db)
    request = reader.plan(plan)
    original = reader.batches
    count = 0

    def fail_second(snapshot):
        nonlocal count
        count += 1
        if count == 2:
            raise RuntimeError("source interruption")
        yield from original(snapshot)

    reader.batches = fail_second
    with pytest.raises(RuntimeError):
        freeze(db, request, reader)
    assert db.execute("SELECT count(*) FROM control.captures").fetchone()[0] == 1
    first = reader.calls[0]
    reader.batches = original
    freeze(db, request, reader)
    assert reader.calls.count(first) == 1
    assert db.execute("SELECT count(*) FROM control.captures").fetchone()[0] == 8
