"""Tests for Bronze writers and buffering behavior."""

from datetime import UTC, date, datetime
from unittest.mock import Mock

import pytest

from procurement.bronze import (
    BronzeRecord,
    BronzeWriteError,
    BufferedBronzeWriter,
    DltBronzeWriter,
)


def sample_record(source_id="1"):
    return BronzeRecord(
        source_id=source_id,
        run_id="run-test",
        source_date=date(2025, 1, 1),
        ingested_at=datetime.now(UTC),
        content_hash="a" * 64,
        payload={"id": source_id, "name": "test"},
    )


def test_dlt_writer_confirms_each_table_and_tracks_persisted(monkeypatch):
    monkeypatch.setattr(
        "procurement.bronze.dlt_writer.create_bronze_resource", lambda records, **_: records
    )
    receipt = Mock()
    pipeline = Mock()
    pipeline.run.side_effect = [receipt, RuntimeError("failed table")]
    writer = DltBronzeWriter(lambda: pipeline)

    with pytest.raises(BronzeWriteError) as exc:
        writer.write_page(
            {"table_a": [sample_record("1"), sample_record("2")], "table_b": [sample_record("3")]}
        )

    assert exc.value.persisted_records == 2
    assert "failed table" in str(exc.value.cause)
    receipt.raise_on_failed_jobs.assert_called_once()


def test_dlt_writer_rejects_missing_receipt(monkeypatch):
    monkeypatch.setattr(
        "procurement.bronze.dlt_writer.create_bronze_resource", lambda records, **_: records
    )
    pipeline = Mock()
    pipeline.run.return_value = None
    writer = DltBronzeWriter(lambda: pipeline)

    with pytest.raises(BronzeWriteError, match="no load receipt"):
        writer.write_page({"table_a": [sample_record("1")]})


def test_buffer_writer_receipts_wait_for_actual_flush():
    inner_writer = Mock()
    inner_writer.write_page.side_effect = lambda tables: sum(len(v) for v in tables.values())
    buffer = BufferedBronzeWriter(inner_writer, max_records=3)

    assert buffer.write_page({"item": [sample_record("1")]}) == 0
    assert buffer.take_receipts() == {}
    assert buffer.write_page({"item": [sample_record("2")]}) == 0
    assert buffer.write_page({"item": [sample_record("3")]}) == 3
    assert buffer.take_receipts() == {0: 1, 1: 1, 2: 1}
    assert inner_writer.write_page.call_count == 1
