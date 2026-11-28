"""Apache Iceberg table maintenance on Spark 4.0 (API + CLI).

Wraps Iceberg's official Spark SQL stored procedures in a safe, Dagster-ready API:
  1. ``rewrite_data_files``  — binpack small Parquet files per partition (~128 MB target)
  2. ``rewrite_manifests``   — compact Avro manifest lists for faster query planning
  3. ``expire_snapshots``    — prune old snapshots & superseded data files
  4. ``remove_orphan_files`` — clean unreferenced S3 files older than a safe retention window

CLI usage::

    python -m procurement_spark.maintenance --namespace silver_spark --dry-run-orphans
    python -m procurement_spark.maintenance --namespace silver_spark --tables notice_revision,award_lot
"""

from __future__ import annotations

import argparse
import gc
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import SparkSession

from procurement_spark.cli_common import (
    add_spark_runtime_args,
    render_maintenance_summary,
    settings_from_cli_args,
)
from procurement_spark.config import SparkSilverSettings
from procurement_spark.spark_session import build_spark_session

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, label: str) -> str:
    if not value or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid SQL identifier for {label}: {value!r}")
    return value


def _format_sql_timestamp(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@dataclass(frozen=True)
class MaintenanceOptions:
    """Configuration for Iceberg maintenance procedures."""

    target_file_size_bytes: int = 128 * 1024 * 1024
    min_input_files: int = 2
    min_file_size_bytes: int = 32 * 1024 * 1024
    retain_last_snapshots: int = 5
    expire_older_than_days: int = 7
    orphan_older_than_days: int = 3
    run_rewrite_data_files: bool = True
    run_rewrite_manifests: bool = True
    run_expire_snapshots: bool = True
    run_remove_orphan_files: bool = True
    dry_run_orphans: bool = False

    def __post_init__(self) -> None:
        if self.target_file_size_bytes <= 0:
            raise ValueError("target_file_size_bytes must be > 0")
        if self.min_input_files < 1:
            raise ValueError("min_input_files must be >= 1")
        if self.retain_last_snapshots < 1:
            raise ValueError("retain_last_snapshots must be >= 1")
        if self.expire_older_than_days < 0:
            raise ValueError("expire_older_than_days must be >= 0")
        if self.orphan_older_than_days < 1:
            raise ValueError(
                "orphan_older_than_days must be >= 1 to protect concurrent in-flight writes"
            )


@dataclass
class TableMaintenanceResult:
    """Per-table execution summary returned by maintain_iceberg_table."""

    table: str
    status: str
    rewritten_data_files_count: int = 0
    added_data_files_count: int = 0
    rewritten_bytes_count: int = 0
    rewritten_manifests_count: int = 0
    added_manifests_count: int = 0
    deleted_data_files_count: int = 0
    deleted_manifest_files_count: int = 0
    orphan_files_count: int = 0
    orphan_dry_run: bool = False
    wall_s: float = 0.0
    error: str | None = None


def discover_namespace_tables(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
) -> list[str]:
    """List all Iceberg tables in ``{catalog}.{namespace}`` sorted alphabetically."""
    _validate_identifier(catalog, "catalog")
    _validate_identifier(namespace, "namespace")
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
    return sorted(str(row["tableName"]) for row in rows if row["tableName"])


def _row_int(row: Any, field: str) -> int:
    if row is None:
        return 0
    as_dict = row.asDict(recursive=False) if hasattr(row, "asDict") else dict(row)
    val = as_dict.get(field)
    return int(val) if val is not None else 0


def maintain_iceberg_table(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    table_name: str,
    options: MaintenanceOptions | None = None,
    now_utc: datetime | None = None,
) -> TableMaintenanceResult:
    """Run configured Iceberg maintenance procedures on a single table."""
    _validate_identifier(catalog, "catalog")
    _validate_identifier(namespace, "namespace")
    _validate_identifier(table_name, "table_name")

    opts = options or MaintenanceOptions()
    now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    table_ref = f"{namespace}.{table_name}"
    t0 = time.perf_counter()

    result = TableMaintenanceResult(
        table=table_name,
        status="OK",
        orphan_dry_run=opts.dry_run_orphans,
    )

    try:
        if opts.run_rewrite_data_files:
            rows = spark.sql(
                f"CALL {catalog}.system.rewrite_data_files("
                f"table => '{table_ref}', "
                f"strategy => 'binpack', "
                f"options => map("
                f"'target-file-size-bytes', '{opts.target_file_size_bytes}', "
                f"'min-input-files', '{opts.min_input_files}', "
                f"'min-file-size-bytes', '{opts.min_file_size_bytes}'"
                f"))"
            ).collect()
            if rows:
                result.rewritten_data_files_count = _row_int(
                    rows[0], "rewritten_data_files_count"
                )
                result.added_data_files_count = _row_int(rows[0], "added_data_files_count")
                result.rewritten_bytes_count = _row_int(rows[0], "rewritten_bytes_count")

        if opts.run_rewrite_manifests:
            rows = spark.sql(
                f"CALL {catalog}.system.rewrite_manifests(table => '{table_ref}')"
            ).collect()
            if rows:
                result.rewritten_manifests_count = _row_int(
                    rows[0], "rewritten_manifests_count"
                )
                result.added_manifests_count = _row_int(rows[0], "added_manifests_count")

        if opts.run_expire_snapshots:
            expire_cutoff = _format_sql_timestamp(
                now - timedelta(days=opts.expire_older_than_days)
            )
            rows = spark.sql(
                f"CALL {catalog}.system.expire_snapshots("
                f"table => '{table_ref}', "
                f"older_than => TIMESTAMP '{expire_cutoff}', "
                f"retain_last => {opts.retain_last_snapshots}"
                f")"
            ).collect()
            if rows:
                result.deleted_data_files_count = _row_int(rows[0], "deleted_data_files_count")
                result.deleted_manifest_files_count = _row_int(
                    rows[0], "deleted_manifest_files_count"
                )

        if opts.run_remove_orphan_files:
            orphan_cutoff = _format_sql_timestamp(
                now - timedelta(days=opts.orphan_older_than_days)
            )
            dry_run_literal = "true" if opts.dry_run_orphans else "false"
            rows = spark.sql(
                f"CALL {catalog}.system.remove_orphan_files("
                f"table => '{table_ref}', "
                f"older_than => TIMESTAMP '{orphan_cutoff}', "
                f"dry_run => {dry_run_literal}"
                f")"
            ).collect()
            result.orphan_files_count = len(rows)
    except Exception as exc:
        result.status = "FAILED"
        result.error = f"{type(exc).__name__}: {exc}"
    finally:
        result.wall_s = round(time.perf_counter() - t0, 2)
        try:
            spark.catalog.clearCache()
        except Exception:
            pass
        gc.collect()

    return result


def run_iceberg_maintenance(
    *,
    namespace: str | None = None,
    tables: list[str] | tuple[str, ...] | None = None,
    options: MaintenanceOptions | None = None,
    settings: SparkSilverSettings | None = None,
    spark: SparkSession | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Run Iceberg maintenance across all (or selected) tables in a namespace.

    Designed for both CLI invocation and Dagster ops/assets.
    """
    cfg = settings or SparkSilverSettings.from_environment(
        namespace=namespace,
        load_dotenv_file=True,
    )
    target_namespace = namespace or cfg.iceberg_namespace
    _validate_identifier(cfg.iceberg_catalog_name, "catalog")
    _validate_identifier(target_namespace, "namespace")

    opts = options or MaintenanceOptions()
    owns_spark = spark is None
    t_total = time.perf_counter()

    active_spark = spark or build_spark_session(cfg)
    try:
        target_tables = (
            list(tables)
            if tables is not None
            else discover_namespace_tables(
                active_spark,
                catalog=cfg.iceberg_catalog_name,
                namespace=target_namespace,
            )
        )
        results: list[TableMaintenanceResult] = []
        for tbl in target_tables:
            res = maintain_iceberg_table(
                active_spark,
                catalog=cfg.iceberg_catalog_name,
                namespace=target_namespace,
                table_name=tbl,
                options=opts,
                now_utc=now_utc,
            )
            results.append(res)
    finally:
        if owns_spark:
            active_spark.stop()

    failed = [r.table for r in results if r.status != "OK"]
    return {
        "catalog": cfg.iceberg_catalog_name,
        "namespace": target_namespace,
        "status": "FAILED" if failed else "OK",
        "tables_total": len(results),
        "tables_failed": failed,
        "totals": {
            "rewritten_data_files_count": sum(r.rewritten_data_files_count for r in results),
            "added_data_files_count": sum(r.added_data_files_count for r in results),
            "rewritten_bytes_count": sum(r.rewritten_bytes_count for r in results),
            "rewritten_manifests_count": sum(r.rewritten_manifests_count for r in results),
            "added_manifests_count": sum(r.added_manifests_count for r in results),
            "deleted_data_files_count": sum(r.deleted_data_files_count for r in results),
            "deleted_manifest_files_count": sum(r.deleted_manifest_files_count for r in results),
            "orphan_files_count": sum(r.orphan_files_count for r in results),
        },
        "options": asdict(opts),
        "tables": [asdict(r) for r in results],
        "wall_s": round(time.perf_counter() - t_total, 2),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m procurement_spark.maintenance",
        description="Run Apache Iceberg maintenance (compaction, manifests, expire snapshots, orphan cleanup).",
    )
    parser.add_argument(
        "--namespace",
        default=None,
        help="Iceberg namespace (default: SPARK_SILVER_NAMESPACE or silver_spark).",
    )
    parser.add_argument(
        "--tables",
        default="all",
        help="Comma-separated table names or 'all' (default: all).",
    )
    parser.add_argument(
        "--target-file-size-mb",
        type=int,
        default=128,
        help="Target Parquet file size in MB for rewrite_data_files (default: 128).",
    )
    parser.add_argument(
        "--min-input-files",
        type=int,
        default=2,
        help="Minimum small files in a partition before compacting (default: 2).",
    )
    parser.add_argument(
        "--min-file-size-mb",
        type=int,
        default=32,
        help="Files smaller than this threshold (MB) are candidates for compaction (default: 32).",
    )
    parser.add_argument(
        "--retain-last",
        type=int,
        default=5,
        help="Minimum number of recent snapshots to keep (default: 5).",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=7,
        help="Expire snapshots older than N days (default: 7).",
    )
    parser.add_argument(
        "--orphan-older-than-days",
        type=int,
        default=3,
        help="Remove orphan S3 files older than N days (min: 1, default: 3).",
    )
    parser.add_argument(
        "--dry-run-orphans",
        action="store_true",
        help="Preview orphan files without deleting them.",
    )
    parser.add_argument("--skip-rewrite-data", action="store_true")
    parser.add_argument("--skip-rewrite-manifests", action="store_true")
    parser.add_argument("--skip-expire-snapshots", action="store_true")
    parser.add_argument("--skip-orphans", action="store_true")
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit full summary as JSON.",
    )
    add_spark_runtime_args(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    tables = (
        None
        if args.tables.strip().lower() == "all"
        else [t.strip() for t in args.tables.split(",") if t.strip()]
    )
    cfg = settings_from_cli_args(args, namespace=args.namespace)
    opts = MaintenanceOptions(
        target_file_size_bytes=args.target_file_size_mb * 1024 * 1024,
        min_input_files=args.min_input_files,
        min_file_size_bytes=args.min_file_size_mb * 1024 * 1024,
        retain_last_snapshots=args.retain_last,
        expire_older_than_days=args.older_than_days,
        orphan_older_than_days=args.orphan_older_than_days,
        run_rewrite_data_files=not args.skip_rewrite_data,
        run_rewrite_manifests=not args.skip_rewrite_manifests,
        run_expire_snapshots=not args.skip_expire_snapshots,
        run_remove_orphan_files=not args.skip_orphans,
        dry_run_orphans=args.dry_run_orphans,
    )
    summary = run_iceberg_maintenance(
        namespace=cfg.iceberg_namespace,
        tables=tables,
        options=opts,
        settings=cfg,
    )
    if args.as_json:
        print(json.dumps(summary, indent=2))
    else:
        render_maintenance_summary(summary, master=cfg.spark_master)
    return 0 if summary["status"] == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
