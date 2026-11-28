"""Direct Iceberg reader for ``lake.silver_spark.*`` (no local DuckDB copy or fake observation shim)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

SILVER_INPUT_TABLES: tuple[str, ...] = (
    "plan_revision",
    "package_revision",
    "package_location",
    "notice_revision",
    "notice_location",
    "opening_revision",
    "opening_submission",
    "result_revision",
    "award_lot",
    "award_contractor",
    "contract_revision",
    "contract_performance_revision",
)


def read_silver_snapshot_ids(
    spark: SparkSession,
    *,
    catalog: str = "lake",
    silver_namespace: str = "silver_spark",
    tables: tuple[str, ...] = SILVER_INPUT_TABLES,
) -> dict[str, int | None]:
    """Capture the current ``main`` Iceberg snapshot_id for each Silver input table.

    Used both for audit receipts and as the ``start-snapshot-id`` / ``end-snapshot-id``
    watermark pair for incremental Gold runs.
    """
    snapshots: dict[str, int | None] = {}
    for tbl in tables:
        full_name = f"{catalog}.{silver_namespace}.{tbl}"
        rows = spark.sql(
            f"SELECT snapshot_id FROM {full_name}.refs WHERE name = 'main'"
        ).collect()
        snapshots[tbl] = int(rows[0]["snapshot_id"]) if rows else None
    return snapshots


def load_silver_views(
    spark: SparkSession,
    *,
    catalog: str = "lake",
    silver_namespace: str = "silver_spark",
    view_prefix: str = "silver_",
    tables: tuple[str, ...] = SILVER_INPUT_TABLES,
    snapshot_ids: dict[str, int | None] | None = None,
) -> dict[str, DataFrame]:
    """Register direct Iceberg DataFrames from ``silver_spark`` as Spark TempViews.

    If ``snapshot_ids`` is provided, each table is pinned to that exact Iceberg snapshot
    so the entire Gold run reads a globally consistent point-in-time cut of Silver.
    """
    views: dict[str, DataFrame] = {}
    for tbl in tables:
        full_name = f"{catalog}.{silver_namespace}.{tbl}"
        reader = spark.read.format("iceberg")
        snap = snapshot_ids.get(tbl) if snapshot_ids else None
        if snap is not None:
            reader = reader.option("snapshot-id", str(snap))
        df = reader.load(full_name)
        view_name = f"{view_prefix}{tbl}"
        df.createOrReplaceTempView(view_name)
        views[tbl] = df
    return views


def load_silver_incremental_deltas(
    spark: SparkSession,
    *,
    catalog: str = "lake",
    silver_namespace: str = "silver_spark",
    from_snapshots: dict[str, int | None],
    to_snapshots: dict[str, int | None],
    tables: tuple[str, ...] = SILVER_INPUT_TABLES,
) -> dict[str, DataFrame]:
    """Read only newly appended Silver rows between ``from_snapshots`` and ``to_snapshots``.

    Tables whose snapshot_id did not advance are omitted from the returned dict.
    """
    deltas: dict[str, DataFrame] = {}
    for tbl in tables:
        start_snap = from_snapshots.get(tbl)
        end_snap = to_snapshots.get(tbl)
        if end_snap is None or start_snap == end_snap:
            continue
        full_name = f"{catalog}.{silver_namespace}.{tbl}"
        reader = spark.read.format("iceberg")
        if start_snap is not None:
            reader = reader.option("start-snapshot-id", str(start_snap)).option(
                "end-snapshot-id", str(end_snap)
            )
        else:
            reader = reader.option("snapshot-id", str(end_snap))
        deltas[tbl] = reader.load(full_name)
    return deltas
