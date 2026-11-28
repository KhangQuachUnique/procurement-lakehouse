"""Post-load Data Quality Gate running directly on an isolated Iceberg WAP branch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class GateCheckResult:
    check_name: str
    passed: bool
    detail: str


def run_post_load_gate(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    resource: str,
    primary_tables: tuple[tuple[str, str], ...],
    quarantine_table: str,
    branch_name: str,
    target_dates: list[date],
    expected_raw_rows: int,
) -> tuple[bool, list[GateCheckResult]]:
    """Verify committed candidate data on WAP branch before fast-forwarding main."""
    checks: list[GateCheckResult] = []
    quar_ref = f"{catalog}.{namespace}.{quarantine_table}"
    in_dates_sql = ", ".join(f"DATE '{d.isoformat()}'" for d in target_dates)
    total_valid_rows = 0

    # 1. Primary key non-null & uniqueness for every primary revision table on WAP branch
    for primary_table, pk_column in primary_tables:
        prim_ref = f"{catalog}.{namespace}.{primary_table}"
        pk_row = spark.sql(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT({pk_column}) AS non_null_pks,
                COUNT(DISTINCT {pk_column}) AS distinct_pks
            FROM {prim_ref} VERSION AS OF '{branch_name}'
            WHERE source_date IN ({in_dates_sql})
            """
        ).collect()[0]
        valid_rows = int(pk_row["total_rows"])
        non_null_pks = int(pk_row["non_null_pks"])
        distinct_pks = int(pk_row["distinct_pks"])
        total_valid_rows += valid_rows

        checks.append(
            GateCheckResult(
                check_name=f"{primary_table}:primary_key_not_null",
                passed=(valid_rows == non_null_pks),
                detail=f"rows={valid_rows}, non_null_pks={non_null_pks}",
            )
        )
        checks.append(
            GateCheckResult(
                check_name=f"{primary_table}:primary_key_unique",
                passed=(valid_rows == distinct_pks),
                detail=f"rows={valid_rows}, distinct_pks={distinct_pks}",
            )
        )

    # 2. Raw-grain reconciliation across all primary tables + shared quarantine (scoped by resource)
    quar_row = spark.sql(
        f"""
        SELECT COUNT(*) AS q_rows
        FROM {quar_ref} VERSION AS OF '{branch_name}'
        WHERE resource = '{resource}'
          AND source_date IN ({in_dates_sql})
        """
    ).collect()[0]
    quar_rows = int(quar_row["q_rows"])

    reconciled_ok = (total_valid_rows + quar_rows > 0) if expected_raw_rows > 0 else (total_valid_rows == 0)
    checks.append(
        GateCheckResult(
            check_name="raw_grain_non_empty_reconciliation",
            passed=reconciled_ok,
            detail=f"valid={total_valid_rows}, quarantined={quar_rows}, expected_raw={expected_raw_rows}",
        )
    )

    all_passed = all(c.passed for c in checks)
    return all_passed, checks
