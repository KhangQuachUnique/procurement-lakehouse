"""Unified Spark Silver runner for both Daily Incremental and Monthly Backfill."""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url

from procurement_spark.config import SparkSilverSettings
from procurement_spark.control import (
    PlannedBatch,
    ReceiptStore,
    SilverReceipt,
    plan_batches,
    resolve_bronze_date_bounds,
    resolve_bronze_manifests,
)
from procurement_spark.io import (
    abort_wap_branch,
    ensure_namespace_and_tables,
    publish_wap_branch,
    read_bronze_tables_by_name,
    stage_to_wap_branch,
)
from procurement_spark.quality import (
    SHARED_DQ_TABLE,
    SHARED_QUARANTINE_TABLE,
    evaluate_inline_rules,
    run_post_load_gate,
)
from procurement_spark.resources import SPARK_RESOURCES, get_spark_resource
from procurement_spark.spark_session import build_spark_session


@dataclass(frozen=True)
class BatchRunSummary:
    batch_id: str
    label: str
    status: str
    days_count: int
    raw_input_rows: int
    valid_rows: int
    quarantined_rows: int
    warn_rows: int
    wall_s: float
    snapshots: dict[str, int]
    error: str | None = None


def _run_one_batch(
    spark: SparkSession,
    *,
    settings: SparkSilverSettings,
    receipts: ReceiptStore,
    batch: PlannedBatch,
) -> BatchRunSummary:
    spec = get_spark_resource(batch.resource)
    branch_name = f"wap_{batch.batch_id}"
    t0 = time.perf_counter()

    # Handle all-empty committed batch without invoking Spark Parquet read
    if not batch.s3a_uris:
        empty_receipts = [
            SilverReceipt(
                resource=batch.resource,
                source_date=d.source_date,
                namespace=settings.iceberg_namespace,
                input_commit_id=d.commit_id,
                transform_version=spec.transform_version,
                batch_id=batch.batch_id,
                status="PUBLISHED",
                snapshot_map={},
                metrics={"raw_input_rows": 0, "valid_rows": 0, "quarantined_rows": 0},
            )
            for d in batch.days
        ]
        receipts.upsert_many(empty_receipts)
        return BatchRunSummary(
            batch_id=batch.batch_id,
            label=batch.label,
            status="PUBLISHED",
            days_count=len(batch.days),
            raw_input_rows=0,
            valid_rows=0,
            quarantined_rows=0,
            warn_rows=0,
            wall_s=round(time.perf_counter() - t0, 2),
            snapshots={},
        )

    # Mark batch days RUNNING in PostgreSQL control plane
    receipts.upsert_many(
        [
            SilverReceipt(
                resource=batch.resource,
                source_date=d.source_date,
                namespace=settings.iceberg_namespace,
                input_commit_id=d.commit_id,
                transform_version=spec.transform_version,
                batch_id=batch.batch_id,
                status="RUNNING",
                snapshot_map={},
                metrics={},
            )
            for d in batch.days
        ]
    )

    bronze_by_table = {
        tbl: df.persist(StorageLevel.DISK_ONLY)
        for tbl, df in read_bronze_tables_by_name(
            spark, batch.uris_by_table, spec.bronze_tables
        ).items()
    }
    target_dates = [d.source_date for d in batch.days]
    try:
        output = spec.transform_fn(bronze_by_table)

        # 1. Stage all output tables onto isolated Iceberg WAP branch
        stage_to_wap_branch(
            spark,
            catalog=settings.iceberg_catalog_name,
            namespace=settings.iceberg_namespace,
            branch_name=branch_name,
            tables_df=output.tables,
            target_dates=target_dates,
            resource=batch.resource,
            resource_scoped_tables={SHARED_DQ_TABLE, SHARED_QUARANTINE_TABLE},
        )

        # 2. Collect accurate per-day inline quality metrics
        day_metric_rows = output.per_day_metrics.collect()
        per_day_metrics: dict[date, dict[str, Any]] = {}
        totals: dict[str, int] = {
            "raw_input_rows": 0,
            "valid_rows": 0,
            "quarantined_rows": 0,
            "warn_rows": 0,
            "invalid_money_rows": 0,
            "unknown_currency_rows": 0,
            "invalid_date_order_rows": 0,
        }
        for row in day_metric_rows:
            d_dict = row.asDict(recursive=True)
            src_d = d_dict.pop("source_date")
            per_day_metrics[src_d] = {k: int(v or 0) for k, v in d_dict.items()}
            for k, v in per_day_metrics[src_d].items():
                totals[k] = totals.get(k, 0) + v

        # 3. Evaluate inline rules + Post-load SQL Quality Gate on the WAP branch
        inline_ok, inline_evals = evaluate_inline_rules(spec.quality_rules, totals)
        gate_ok, gate_checks = run_post_load_gate(
            spark,
            catalog=settings.iceberg_catalog_name,
            namespace=settings.iceberg_namespace,
            resource=batch.resource,
            primary_tables=spec.primary_tables,
            quarantine_table=spec.quarantine_table,
            branch_name=branch_name,
            target_dates=target_dates,
            expected_raw_rows=totals["raw_input_rows"],
        )

        if not (inline_ok and gate_ok):
            failed_details = [
                f"inline:{e.rule.name}(count={e.observed_count},rate={e.observed_rate})"
                for e in inline_evals
                if not e.passed and e.rule.severity == "block"
            ] + [f"gate:{c.check_name}({c.detail})" for c in gate_checks if not c.passed]
            err_msg = "Quality gate failed: " + "; ".join(failed_details)
            abort_wap_branch(
                spark,
                catalog=settings.iceberg_catalog_name,
                namespace=settings.iceberg_namespace,
                branch_name=branch_name,
                table_names=list(output.tables.keys()),
            )
            receipts.upsert_many(
                [
                    SilverReceipt(
                        resource=batch.resource,
                        source_date=d.source_date,
                        namespace=settings.iceberg_namespace,
                        input_commit_id=d.commit_id,
                        transform_version=spec.transform_version,
                        batch_id=batch.batch_id,
                        status="FAILED",
                        snapshot_map={},
                        metrics=per_day_metrics.get(d.source_date, {"raw_input_rows": 0}),
                        error_message=err_msg,
                    )
                    for d in batch.days
                ]
            )
            return BatchRunSummary(
                batch_id=batch.batch_id,
                label=batch.label,
                status="FAILED",
                days_count=len(batch.days),
                raw_input_rows=totals["raw_input_rows"],
                valid_rows=totals["valid_rows"],
                quarantined_rows=totals["quarantined_rows"],
                warn_rows=totals["warn_rows"],
                wall_s=round(time.perf_counter() - t0, 2),
                snapshots={},
                error=err_msg,
            )

        # 4. Publish WAP branch atomically to main & record per-day receipts
        snapshots = publish_wap_branch(
            spark,
            catalog=settings.iceberg_catalog_name,
            namespace=settings.iceberg_namespace,
            branch_name=branch_name,
            table_names=list(output.tables.keys()),
        )
        receipts.upsert_many(
            [
                SilverReceipt(
                    resource=batch.resource,
                    source_date=d.source_date,
                    namespace=settings.iceberg_namespace,
                    input_commit_id=d.commit_id,
                    transform_version=spec.transform_version,
                    batch_id=batch.batch_id,
                    status="PUBLISHED",
                    snapshot_map=snapshots,
                    metrics=per_day_metrics.get(d.source_date, {"raw_input_rows": 0}),
                )
                for d in batch.days
            ]
        )
        return BatchRunSummary(
            batch_id=batch.batch_id,
            label=batch.label,
            status="PUBLISHED",
            days_count=len(batch.days),
            raw_input_rows=totals["raw_input_rows"],
            valid_rows=totals["valid_rows"],
            quarantined_rows=totals["quarantined_rows"],
            warn_rows=totals["warn_rows"],
            wall_s=round(time.perf_counter() - t0, 2),
            snapshots=snapshots,
        )
    except Exception as exc:
        try:
            abort_wap_branch(
                spark,
                catalog=settings.iceberg_catalog_name,
                namespace=settings.iceberg_namespace,
                branch_name=branch_name,
                table_names=list(spec.iceberg_ddls.keys()),
            )
        except Exception:
            pass
        receipts.upsert_many(
            [
                SilverReceipt(
                    resource=batch.resource,
                    source_date=d.source_date,
                    namespace=settings.iceberg_namespace,
                    input_commit_id=d.commit_id,
                    transform_version=spec.transform_version,
                    batch_id=batch.batch_id,
                    status="FAILED",
                    snapshot_map={},
                    metrics={},
                    error_message=str(exc),
                )
                for d in batch.days
            ]
        )
        raise
    finally:
        for df in bronze_by_table.values():
            try:
                df.unpersist()
            except Exception:
                pass


def _create_pg_engine(db_url: str) -> Engine:
    """Create SQLAlchemy PostgreSQL engine using whichever driver (psycopg v3 or psycopg2) is installed."""
    url = make_url(db_url)
    if url.drivername in {"postgresql", "postgresql+psycopg", "postgresql+psycopg2"}:
        try:
            import psycopg  # noqa: F401

            url = url.set(drivername="postgresql+psycopg")
        except ImportError:
            url = url.set(drivername="postgresql+psycopg2")
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        hide_parameters=True,
        connect_args={"connect_timeout": 10},
    )


def run_spark_silver(
    resource: str,
    start_date: date,
    end_date: date,
    *,
    mode: Literal["daily", "backfill"] = "backfill",
    force: bool = False,
    settings: SparkSilverSettings | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Run Silver Spark pipeline for a resource over [start_date, end_date].

    Reuses a single SparkSession across all planned batches (months in backfill, days in daily).
    """
    cfg = settings or SparkSilverSettings.from_environment(load_dotenv_file=True)
    spec = get_spark_resource(resource)
    engine = _create_pg_engine(cfg.app_database_url)
    owns_spark = spark is None
    t_total = time.perf_counter()

    try:
        receipt_store = ReceiptStore(engine)
        receipt_store.ensure_schema()

        manifests = resolve_bronze_manifests(
            engine,
            spec.bronze_resource,
            start_date,
            end_date,
            table_names=spec.bronze_tables,
        )
        published = receipt_store.list_published(
            cfg.iceberg_namespace,
            spec.name,
            start_date,
            end_date,
        )
        batches, missing_dates = plan_batches(
            resource=spec.name,
            transform_version=spec.transform_version,
            manifests=manifests,
            published_receipts=published,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            force=force,
        )

        if not batches:
            return {
                "resource": spec.name,
                "namespace": cfg.iceberg_namespace,
                "mode": mode,
                "batches": [],
                "missing_bronze_dates": [d.isoformat() for d in missing_dates],
                "wall_s": round(time.perf_counter() - t_total, 2),
                "status": "NO_OP",
            }

        active_spark = spark or build_spark_session(cfg)
        try:
            ensure_namespace_and_tables(
                active_spark,
                catalog=cfg.iceberg_catalog_name,
                namespace=cfg.iceberg_namespace,
                table_ddls=spec.iceberg_ddls,
            )
            summaries: list[BatchRunSummary] = []
            for batch in batches:
                summary = _run_one_batch(
                    active_spark,
                    settings=cfg,
                    receipts=receipt_store,
                    batch=batch,
                )
                summaries.append(summary)
        finally:
            if owns_spark:
                active_spark.stop()

        overall = "FAILED" if any(s.status != "PUBLISHED" for s in summaries) else "PUBLISHED"
        return {
            "resource": spec.name,
            "namespace": cfg.iceberg_namespace,
            "mode": mode,
            "status": overall,
            "batches": [s.__dict__ for s in summaries],
            "missing_bronze_dates": [d.isoformat() for d in missing_dates],
            "wall_s": round(time.perf_counter() - t_total, 2),
        }
    finally:
        engine.dispose()


def run_spark_silver_incremental(
    resource: str,
    *,
    since_date: date | None = None,
    until_date: date | None = None,
    batch_mode: Literal["daily", "backfill"] | None = None,
    force: bool = False,
    settings: SparkSilverSettings | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Auto-discover committed Bronze date bounds and process any unpublished or updated days.

    Returns ``status='NO_OP'`` immediately without starting Spark if all committed Bronze
    days already have matching PUBLISHED receipts.
    """
    cfg = settings or SparkSilverSettings.from_environment(load_dotenv_file=True)
    spec = get_spark_resource(resource)
    t_total = time.perf_counter()

    engine = _create_pg_engine(cfg.app_database_url)
    try:
        bounds = resolve_bronze_date_bounds(
            engine,
            spec.bronze_resource,
            since_date=since_date,
            until_date=until_date,
        )
    finally:
        engine.dispose()

    if bounds is None:
        return {
            "resource": spec.name,
            "namespace": cfg.iceberg_namespace,
            "mode": "incremental",
            "status": "NO_OP",
            "batches": [],
            "missing_bronze_dates": [],
            "wall_s": round(time.perf_counter() - t_total, 2),
        }

    start_date, end_date = bounds
    resolved_mode: Literal["daily", "backfill"] = batch_mode or (
        "backfill" if (end_date - start_date).days > 31 else "daily"
    )
    result = run_spark_silver(
        resource=spec.name,
        start_date=start_date,
        end_date=end_date,
        mode=resolved_mode,
        force=force,
        settings=cfg,
        spark=spark,
    )
    result["discovered_start_date"] = start_date.isoformat()
    result["discovered_end_date"] = end_date.isoformat()
    result["requested_mode"] = "incremental"
    return result


def run_spark_silver_all(
    resources: list[str] | tuple[str, ...] | None = None,
    *,
    mode: Literal["incremental", "daily", "backfill"] = "incremental",
    start_date: date | None = None,
    end_date: date | None = None,
    force: bool = False,
    settings: SparkSilverSettings | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Run multiple (or all 6) Silver resources sharing a single SparkSession when work exists."""
    cfg = settings or SparkSilverSettings.from_environment(load_dotenv_file=True)
    target_resources = list(resources) if resources is not None else list(SPARK_RESOURCES.keys())
    for res in target_resources:
        get_spark_resource(res)

    if mode in {"daily", "backfill"} and (start_date is None or end_date is None):
        raise ValueError(f"--start-date and --end-date are required when mode={mode!r}")

    t_total = time.perf_counter()
    owns_spark = spark is None
    active_spark: SparkSession | None = spark
    results: list[dict[str, Any]] = []

    try:
        for res in target_resources:
            if mode == "incremental":
                # Check if work exists before spinning up a SparkSession on an empty run
                if active_spark is None:
                    engine = _create_pg_engine(cfg.app_database_url)
                    try:
                        spec = get_spark_resource(res)
                        bounds = resolve_bronze_date_bounds(
                            engine,
                            spec.bronze_resource,
                            since_date=start_date,
                            until_date=end_date,
                        )
                        if bounds is None:
                            results.append(
                                {
                                    "resource": spec.name,
                                    "namespace": cfg.iceberg_namespace,
                                    "mode": "incremental",
                                    "status": "NO_OP",
                                    "batches": [],
                                    "missing_bronze_dates": [],
                                    "wall_s": 0.0,
                                }
                            )
                            continue
                        receipt_store = ReceiptStore(engine)
                        receipt_store.ensure_schema()
                        manifests = resolve_bronze_manifests(
                            engine,
                            spec.bronze_resource,
                            bounds[0],
                            bounds[1],
                            table_names=spec.bronze_tables,
                        )
                        published = receipt_store.list_published(
                            cfg.iceberg_namespace,
                            spec.name,
                            bounds[0],
                            bounds[1],
                        )
                        batches, missing_dates = plan_batches(
                            resource=spec.name,
                            transform_version=spec.transform_version,
                            manifests=manifests,
                            published_receipts=published,
                            start_date=bounds[0],
                            end_date=bounds[1],
                            mode="daily",
                            force=force,
                        )
                        if not batches:
                            results.append(
                                {
                                    "resource": spec.name,
                                    "namespace": cfg.iceberg_namespace,
                                    "mode": "incremental",
                                    "status": "NO_OP",
                                    "batches": [],
                                    "missing_bronze_dates": [d.isoformat() for d in missing_dates],
                                    "wall_s": 0.0,
                                }
                            )
                            continue
                    finally:
                        engine.dispose()
                    active_spark = build_spark_session(cfg)

                res_summary = run_spark_silver_incremental(
                    res,
                    since_date=start_date,
                    until_date=end_date,
                    force=force,
                    settings=cfg,
                    spark=active_spark,
                )
            else:
                if active_spark is None:
                    active_spark = build_spark_session(cfg)
                assert start_date is not None and end_date is not None
                res_summary = run_spark_silver(
                    res,
                    start_date=start_date,
                    end_date=end_date,
                    mode=mode,
                    force=force,
                    settings=cfg,
                    spark=active_spark,
                )

            results.append(res_summary)
            if active_spark is not None:
                try:
                    active_spark.catalog.clearCache()
                except Exception:
                    pass
                gc.collect()
    finally:
        if owns_spark and active_spark is not None:
            active_spark.stop()

    failed = [r["resource"] for r in results if r["status"] == "FAILED"]
    overall = (
        "FAILED"
        if failed
        else ("NO_OP" if all(r["status"] == "NO_OP" for r in results) else "PUBLISHED")
    )
    return {
        "namespace": cfg.iceberg_namespace,
        "mode": mode,
        "status": overall,
        "resources_failed": failed,
        "results": results,
        "wall_s": round(time.perf_counter() - t_total, 2),
    }


def _parse_iso_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m procurement_spark.runner",
        description="Run Spark Silver pipeline (incremental, daily, or monthly backfill).",
    )
    parser.add_argument(
        "--resource",
        default="all",
        help="Comma-separated resource names or 'all' (default: all).",
    )
    parser.add_argument(
        "--mode",
        choices=("incremental", "daily", "backfill"),
        default="incremental",
        help="Execution mode (default: incremental).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date YYYY-MM-DD (required for daily/backfill; optional lower bound for incremental).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date YYYY-MM-DD (required for daily/backfill; optional upper bound for incremental).",
    )
    parser.add_argument(
        "--namespace",
        default=None,
        help="Override Iceberg namespace (default: SPARK_SILVER_NAMESPACE or silver_spark).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run dates even if already PUBLISHED with matching commit_id.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit full JSON summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    resources = (
        None
        if args.resource.strip().lower() == "all"
        else [r.strip() for r in args.resource.split(",") if r.strip()]
    )
    cfg = SparkSilverSettings.from_environment(
        namespace=args.namespace,
        load_dotenv_file=True,
    )
    summary = run_spark_silver_all(
        resources=resources,
        mode=args.mode,
        start_date=_parse_iso_date(args.start_date),
        end_date=_parse_iso_date(args.end_date),
        force=args.force,
        settings=cfg,
    )
    if args.as_json:
        print(json.dumps(summary, indent=2))
    else:
        print(
            f"[spark-silver] namespace={summary['namespace']} mode={summary['mode']} "
            f"status={summary['status']} wall_s={summary['wall_s']}"
        )
        for item in summary["results"]:
            print(
                f"  - {item['resource']}: status={item['status']} "
                f"batches={len(item['batches'])} missing_days={len(item['missing_bronze_dates'])} "
                f"wall_s={item['wall_s']}"
            )
        if summary["resources_failed"]:
            print(
                f"  FAILED resources: {', '.join(summary['resources_failed'])}",
                file=sys.stderr,
            )
    return 0 if summary["status"] != "FAILED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
