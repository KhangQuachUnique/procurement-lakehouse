from datetime import date
from typing import Any

from procurement.storage import bronze


def test_bronze_destination_partitions_by_source_date_and_run(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_filesystem(**kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(bronze, "filesystem", fake_filesystem)
    bronze.create_bronze_destination(
        source_partition_date=date(2026, 9, 7),
        run_id="run-a",
    )

    assert captured["layout"] == (
        "{table_name}/source_date={source_date}/run_id={run_id}/"
        "{load_id}.{file_id}.{ext}"
    )
    assert captured["extra_placeholders"] == {
        "source_date": "2026-09-07",
        "run_id": "run-a",
    }
