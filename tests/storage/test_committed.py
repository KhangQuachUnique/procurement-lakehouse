import json
from datetime import date

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.storage.committed import CommittedDay, verify_committed


@pytest.mark.parametrize('change,expected,error', [
    ({'run_id': 'another-run'}, 1, 'lineage mismatch'),
    ({'source_date': '2025-01-02'}, 1, 'lineage mismatch'),
    ({'payload': '{"id":"tampered"}'}, 1, 'hash mismatch'),
    ({}, 2, 'count mismatch'),
])
def test_valid_parquet_with_wrong_content_cannot_pass_verification(tmp_path, change, expected, error):
    payload = {'id': 'original'}
    record = {'run_id': 'committed', 'source_date': '2025-01-01',
              'payload': json.dumps(payload), 'content_hash': calculate_content_hash(payload)}
    record.update(change)
    key = str(tmp_path / 'part.parquet')
    pq.write_table(pa.Table.from_pylist([record]), key)
    selection = (CommittedDay(date(2025, 1, 1), 'committed', expected, (('detail', key),)),)
    with pytest.raises(ValueError, match=error):
        verify_committed(fsspec.filesystem('file'), selection)


def test_empty_committed_day_needs_no_parquet():
    selection = (CommittedDay(date(2025, 1, 1), 'empty-run', 0, ()),)
    assert verify_committed(fsspec.filesystem('file'), selection) == {'days': 1, 'files': 0, 'records': 0}
