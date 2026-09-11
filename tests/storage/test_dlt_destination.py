from datetime import date
from typing import Any

from procurement.storage import dlt_destination


def test_bronze_destination_partitions_by_source_day(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_filesystem(**kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(dlt_destination, "filesystem", fake_filesystem)

    dlt_destination.create_bronze_destination(
        source_partition_date=date(2026, 9, 7)
    )

    assert captured["layout"] == (
        "{table_name}/source_year={source_year}/source_month={source_month}/"
        "source_day={source_day}/{load_id}.{file_id}.{ext}"
    )
    assert captured["extra_placeholders"] == {
        "source_year": "2026",
        "source_month": "09",
        "source_day": "07",
    }
