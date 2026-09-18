from unittest.mock import Mock

import pytest

from procurement.storage import bronze


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
