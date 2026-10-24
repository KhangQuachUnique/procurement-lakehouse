"""Silver Candidate Storage Adapter.

Persists Silver candidate Parquet tables and run reports to Object Storage (S3 / memory fs).
"""

import json
from collections.abc import Sequence
from datetime import date
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from procurement.silver.ports.outbound.silver_repository import SilverCandidateRepositoryPort


class S3SilverCandidateAdapter(SilverCandidateRepositoryPort):
    """S3 / Object storage implementation of SilverCandidateRepositoryPort."""

    def __init__(self, fs: Any) -> None:
        self.fs = fs

    def write_candidate_tables(
        self,
        output_prefix: str,
        target_date: date,
        tables: dict[str, pa.Table],
    ) -> dict[str, str]:
        """Write candidate Parquet tables for a single day.

        Layout: {output_prefix}/{table_name}/source_date={target_date}/data.parquet
        """
        paths: dict[str, str] = {}
        for name, table in tables.items():
            path = f"{output_prefix}/{name}/source_date={target_date}/data.parquet"
            with self.fs.open(path, "wb") as stream:
                pq.write_table(table, stream)
            paths[name] = path
        return paths

    def partition_exists(
        self,
        output_prefix: str,
        target_date: date,
        tables: Sequence[str],
    ) -> bool:
        """Check if candidate Parquet tables exist for a given date."""
        if not tables:
            return False
        return all(
            self.fs.exists(f"{output_prefix}/{table}/source_date={target_date}/data.parquet")
            for table in tables
        )

    def save_run_report(
        self,
        output_prefix: str,
        report: dict[str, Any],
    ) -> str:
        """Save run report JSON file."""
        report_path = f"{output_prefix}/report.json"
        with self.fs.open(report_path, "w") as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2, default=str)
        parent_dir = output_prefix.rsplit("/", 1)[0]
        latest_path = f"{parent_dir}/latest.json"
        try:
            with self.fs.open(latest_path, "w") as stream:
                json.dump(report, stream, ensure_ascii=False, indent=2, default=str)
        except Exception:  # noqa: BLE001, S110
            pass
        return report_path
