"""Apache Iceberg Write-Audit-Publish (WAP) writer using native Iceberg branching."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ensure_namespace_and_tables(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    table_ddls: dict[str, str],
) -> None:
    """Create Iceberg namespace and tables with WAP enabled if they do not exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    for table_name, columns_ddl in table_ddls.items():
        full_name = f"{catalog}.{namespace}.{table_name}"
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {full_name} (
                {columns_ddl}
            )
            USING iceberg
            PARTITIONED BY (months(source_date))
            TBLPROPERTIES (
                'format-version' = '2',
                'write.wap.enabled' = 'true',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read'
            )
            """
        )


def stage_to_wap_branch(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    branch_name: str,
    tables_df: dict[str, DataFrame],
    target_dates: list[date],
    resource: str,
    resource_scoped_tables: set[str] | None = None,
) -> None:
    """Write candidate tables into an isolated Iceberg WAP branch for exact target_dates.

    Shared governance tables (silver_dq_issues, silver_quarantine) are overwritten scoped by
    BOTH resource AND exact target_dates so resources never clobber each other.
    """
    if not target_dates:
        return
    date_sql = ", ".join(f"DATE '{d.isoformat()}'" for d in target_dates)
    scoped_set = resource_scoped_tables or set()

    for table_name in tables_df:
        full_name = f"{catalog}.{namespace}.{table_name}"
        spark.sql(f"ALTER TABLE {full_name} CREATE OR REPLACE BRANCH `{branch_name}`")

    spark.conf.set("spark.wap.branch", branch_name)
    try:
        for table_name, df in tables_df.items():
            full_name = f"{catalog}.{namespace}.{table_name}"
            where_sql = f"source_date IN ({date_sql})"
            if table_name in scoped_set:
                where_sql = f"resource = '{resource}' AND {where_sql}"
            spark.sql(f"DELETE FROM {full_name} WHERE {where_sql}")
            df.writeTo(full_name).append()
    finally:
        spark.conf.unset("spark.wap.branch")


def publish_wap_branch(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    branch_name: str,
    table_names: list[str],
) -> dict[str, int]:
    """Fast-forward main to the verified WAP branch and drop the temporary branch."""
    snapshots: dict[str, int] = {}
    for table_name in table_names:
        full_name = f"{catalog}.{namespace}.{table_name}"
        spark.sql(
            f"CALL {catalog}.system.fast_forward('{namespace}.{table_name}', 'main', '{branch_name}')"
        )
        snap_row = spark.sql(
            f"SELECT snapshot_id FROM {full_name}.refs WHERE name = 'main'"
        ).collect()
        if snap_row:
            snapshots[table_name] = int(snap_row[0]["snapshot_id"])
        spark.sql(f"ALTER TABLE {full_name} DROP BRANCH IF EXISTS `{branch_name}`")
    return snapshots


def abort_wap_branch(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    branch_name: str,
    table_names: list[str],
) -> None:
    """Cleanly drop an unverified/failed WAP branch without touching main."""
    for table_name in table_names:
        full_name = f"{catalog}.{namespace}.{table_name}"
        spark.sql(f"ALTER TABLE {full_name} DROP BRANCH IF EXISTS `{branch_name}`")
