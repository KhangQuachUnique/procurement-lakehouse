"""Unified Spark Silver runner for both Daily Incremental and Monthly Backfill."""

from __future__ import annotations

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
from procurement_spark.resources import get_spark_resource
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
