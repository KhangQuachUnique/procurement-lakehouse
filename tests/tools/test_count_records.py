from datetime import date
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.tools import count_records
from procurement.tools.bronze_explorer import BronzeTable


class Store:
    def __init__(self, data):
        self.data = data
        self.reads = []

    def cat_file(self, key, start, end):
        self.reads.append((start, end))
        return self.data[start:end]


def parquet_bytes(rows):
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.table({"id": list(range(rows)), "payload": ["data" * 1000] * rows}), buffer)
    return buffer.getvalue().to_pybytes()


def test_range_reader_counts_only_footer_and_rejects_corruption():
    data = parquet_bytes(37)
    fs = Store(data)
    assert count_records.footer_count(fs, "s3://bucket/file", len(data)) == 37
    assert len(fs.reads) == 2
    assert fs.reads[1][0] > 4  # No column data is fetched.
    fs.data = data[:-4] + b"FAIL"
    with pytest.raises(ValueError, match="Invalid Parquet footer"):
        count_records.footer_count(fs, "s3://bucket/file", len(data))


def test_cache_uses_object_signature_and_storage_namespace(tmp_path):
    data = parquet_bytes(37)
    fs = Store(data)
    uri = "s3://bucket/file"
    info = {uri: {"size": len(data), "ETag": "one"}}
    database = tmp_path / "counts.sqlite"
    for _ in range(2):
        assert count_records.count_files(fs, [uri], info, database, namespace="storage") == {uri: 37}
    assert len(fs.reads) == 2
    info[uri]["ETag"] = "two"
    count_records.count_files(fs, [uri], info, database, namespace="storage")
    assert len(fs.reads) == 4
    count_records.count_files(fs, [uri], info, database, namespace="another-storage")
    assert len(fs.reads) == 6
    del info[uri]["ETag"]
    for _ in range(2):
        count_records.count_files(fs, [uri], info, database, namespace="storage")
    assert len(fs.reads) == 10  # Unknown object version must not use persistent cache.


def test_report_counts_noti_once_and_checks_day_total():
    tables = [BronzeTable("muasamcong", f"notify_contractor_{kind}_detail")
              for kind in ("standard", "vk_adb", "reoffer")]
    paths = [f"s3://bucket/{table.table}/source_date=2024-12-31/run_id=current/file" for table in tables]
    selected = {table: [path] for table, path in zip(tables, paths, strict=True)}
    counts = dict(zip(paths, (10, 2, 3), strict=True))
    days = {"notify_contractor": {"2024-12-31": SimpleNamespace(bronze_records=15)}}
    report = count_records.build_report(selected, counts, days, [], date(2024, 12, 31), date(2024, 12, 31))
    assert report["totals"] == {"noti": 15}
    assert report["complete"]
    counts[paths[0]] = 9
    report = count_records.build_report(selected, counts, days, [], date(2024, 12, 31), date(2024, 12, 31))
    assert not report["complete"]
    assert report["issues"][0]["expected_records"] == 15
    assert report["issues"][0]["actual_records"] == 14


def test_missing_days_and_empty_resources_are_not_hidden():
    table = BronzeTable("muasamcong", "project_detail")
    report = count_records.build_report({table: []}, {}, {"project": {}}, [], date(2022, 1, 1), date(2022, 1, 2))
    assert report["totals"] == {"project": 0}
    assert report["missing_days"] == {"project": 2}
    assert not report["complete"]


def test_cached_cli_does_not_access_object_storage(tmp_path, monkeypatch, capsys):
    import json

    monkeypatch.setattr(count_records, "create_s3_filesystem", lambda: pytest.fail("Unexpected storage access"))
    # Use the exact same scope hashing as the command without a first network run.
    settings = count_records.settings
    namespace = count_records.hashlib.sha256(json.dumps([
        settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET,
    ]).encode()).hexdigest()
    path = tmp_path / f"2022-2025-all-{namespace[:12]}.json"
    path.write_text(json.dumps({"counted_at": "snapshot-time", "totals": {"noti": 42}, "complete": True}))
    assert count_records.main(["--start-year", "2022", "--end-year", "2025",
                               "--cached", "--cache-dir", str(tmp_path)]) == 0
    assert "CACHED SNAPSHOT at snapshot-time" in capsys.readouterr().out
