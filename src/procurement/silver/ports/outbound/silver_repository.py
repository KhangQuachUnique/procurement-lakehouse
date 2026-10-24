"""Silver Candidate Repository Port.

Defines the interface for persisting candidate Silver Parquet tables and run reports.
"""

from collections.abc import Sequence
from datetime import date
from typing import Any, Protocol

import pyarrow as pa


class SilverCandidateRepositoryPort(Protocol):
    """Outbound port for persisting Silver candidate output."""

    def write_candidate_tables(
        self,
        output_prefix: str,
        target_date: date,
        tables: dict[str, pa.Table],
    ) -> dict[str, str]:
        """Write candidate Parquet tables for a single day.

        Returns mapping of table_name -> storage_path.
        """
        ...

    def partition_exists(
        self,
        output_prefix: str,
        target_date: date,
        tables: Sequence[str],
    ) -> bool:
        """Check if candidate Parquet tables exist for a given date."""
        ...

    def save_run_report(
        self,
        output_prefix: str,
        report: dict[str, Any],
    ) -> str:
        """Save run report JSON file.

        Returns path to saved report.
        """
        ...
