from unittest.mock import Mock

import pytest

from procurement.storage import bronze


def record(key="one"):
    from datetime import date

    from procurement.ingestion.engine.records import build_bronze_item
    return build_bronze_item(table="detail", source_id=key, source_version=None,
        run_id="r", source_date=date(2025, 1, 1), payload={"text": "thông báo"}).record


def test_writer_confirms_each_table_and_preserves_partial_count(monkeypatch):
    monkeypatch.setattr(bronze, "create_bronze_resource", lambda records, **_: records)
    receipt = Mock()
    pipeline = Mock()
    pipeline.run.side_effect = [receipt, RuntimeError("second table failed")]
    writer = bronze.DltBronzeWriter(lambda: pipeline)
    with pytest.raises(bronze.BronzeWriteError) as exc:
        writer.write_page({"plan": [object(), object()], "package": [object()]})
    assert exc.value.persisted_records == 2
    assert str(exc.value.cause) == "second table failed"
    receipt.raise_on_failed_jobs.assert_called_once()


def test_writer_rejects_missing_receipt(monkeypatch):
    monkeypatch.setattr(bronze, "create_bronze_resource", lambda records, **_: records)
    pipeline = Mock()
    pipeline.run.return_value = None
    with pytest.raises(bronze.BronzeWriteError, match="no load receipt"):
        bronze.DltBronzeWriter(lambda: pipeline).write_page({"plan": [object()]})


def test_buffer_receipts_wait_for_actual_load():
    writer = Mock()
    writer.write_page.side_effect = lambda tables: sum(map(len, tables.values()))
    batch = bronze.BufferedBronzeWriter(writer, max_records=3)
    assert batch.write_page({"detail": [record()]}) == 0
    assert batch.take_receipts() == {}
    assert batch.write_page({"detail": [record("two")]}) == 0
    assert batch.write_page({"detail": [record("three")]}) == 3
    assert batch.take_receipts() == {0: 1, 1: 1, 2: 1}
    assert writer.write_page.call_count == 1


def test_partial_table_load_never_confirms_whole_page():
    writer = Mock()
    writer.write_page.side_effect = [1, bronze.BronzeWriteError(RuntimeError("second table"), 0)]
    batch = bronze.BufferedBronzeWriter(writer)
    batch.write_page({"a": [record()], "b": [record()]})
    with pytest.raises(bronze.BronzeWriteError) as error:
        batch.flush()
    assert error.value.persisted_records == batch.persisted_records == 1
    assert batch.confirmed[0] == 1
    assert batch.take_receipts() == {}
    assert batch.pending == []


def test_oversized_page_gets_own_batch():
    writer = Mock()
    writer.write_page.side_effect = lambda tables: sum(map(len, tables.values()))
    batch = bronze.BufferedBronzeWriter(writer, max_bytes=1)
    assert batch.write_page({"detail": [record()]}) == 1
    assert batch.take_receipts() == {0: 1}
    assert not batch.pending
