"""Daily Silver standardization assets, dependent on committed Bronze assets."""

import os
from dataclasses import dataclass
from datetime import date
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
    Config,
    Failure,
    MaterializeResult,
    OpExecutionContext,
    RetryPolicy,
    asset,
    job,
    op,
)
from pydantic import Field

try:
    from dagster import AutomationCondition

    EAGER_AUTOMATION = AutomationCondition.eager()
except (ImportError, AttributeError):
    EAGER_AUTOMATION = None

from procurement.common.dates import today_vn, validate_closed_range
from procurement.common.settings import settings
from procurement.orchestration.bronze import BRONZE_PARTITIONS
from procurement.silver.service import run_resource


@dataclass(frozen=True)
class SilverAssetSpec:
    """Specification mapping a Silver asset to its upstream Bronze dependency."""

    name: str
    resource_name: str
    upstream_bronze_name: str
    description: str


SILVER_SPECS: tuple[SilverAssetSpec, ...] = (
    SilverAssetSpec(
        name="silver_project",
        resource_name="project",
        upstream_bronze_name="bronze_project",
        description="Standardized procurement project revisions and locations",
    ),
    SilverAssetSpec(
        name="silver_khlcnt",
        resource_name="khlcnt",
        upstream_bronze_name="bronze_khlcnt",
        description="Standardized procurement plans, bid packages, lots and locations",
    ),
    SilverAssetSpec(
        name="silver_notice",
        resource_name="notice",
        upstream_bronze_name="bronze_notify_contractor",
        description="Standardized tender notices (TBMT), lots, locations and reoffers",
    ),
    SilverAssetSpec(
        name="silver_contractor_result",
        resource_name="contractor_result",
        upstream_bronze_name="bronze_contractor_result",
        description="Standardized contractor selection results (KQLCNT) and award details",
    ),
)


class SilverConfig(Config):
    """Runtime configuration for Silver materialization."""

    publish_iceberg: bool = Field(
        default=True,
        description="Atomically publish materialized data into Apache Iceberg Format v2",
    )
    skip_existing: bool = Field(
        default=False,
        description="Skip execution if Silver output partitions already exist",
    )
    family: str | None = Field(
        default=None,
        description="Optional family sub-filter for the resource",
    )


def build_silver_asset(spec: SilverAssetSpec):
    """Build a partitioned Dagster asset for a Silver resource contract."""
    checks = [
        AssetCheckSpec(
            "execution_status",
            asset=spec.name,
            blocking=True,
            description="Verify successful execution without unhandled errors",
        ),
        AssetCheckSpec(
            "quarantine_threshold",
            asset=spec.name,
            blocking=False,
            description="Warning if quarantine rate exceeds 20% of processed records",
        ),
        AssetCheckSpec(
            "iceberg_publication",
            asset=spec.name,
            blocking=False,
            description="Verify table partitions were staged/committed into Iceberg catalog",
        ),
    ]

    @asset(
        name=spec.name,
        group_name="silver",
        partitions_def=BRONZE_PARTITIONS,
        deps=[AssetKey(spec.upstream_bronze_name)],
        required_resource_keys={"object_storage"},
        backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
        retry_policy=RetryPolicy(max_retries=0),
        check_specs=checks,
        automation_condition=EAGER_AUTOMATION,
        description=spec.description,
    )
    def silver_asset(context: AssetExecutionContext, config: SilverConfig):
        source_date = date.fromisoformat(context.partition_key)
        validate_closed_range(source_date, source_date, today=today_vn())

        fs = context.resources.object_storage
        bucket = settings.OBJECT_STORAGE_BUCKET

        report = run_resource(
            fs=fs,
            bucket=bucket,
            resource=spec.resource_name,
            start=source_date,
            end=source_date,
            family=config.family,
            skip_existing=config.skip_existing,
            iceberg=config.publish_iceberg,
        )

        days = report.get("days", [])
        if not days:
            yield AssetCheckResult(
                check_name="execution_status",
                passed=False,
                metadata={"error": "Empty execution report returned"},
            )
            raise Failure(
                f"Silver materialization failed: no report for {spec.name}/{source_date}",
                allow_retries=False,
            )

        entry = days[0]
        status = entry.get("status", "unknown")

        if status == "failed":
            error_msg = entry.get("error", "Unknown processing error")
            yield AssetCheckResult(
                check_name="execution_status",
                passed=False,
                metadata={"error": error_msg, "error_type": entry.get("error_type", "")},
            )
            raise Failure(
                f"Silver materialization failed for {spec.name}/{source_date}: {error_msg}",
                allow_retries=False,
            )

        # 1. execution_status check
        yield AssetCheckResult(
            check_name="execution_status",
            passed=True,
            metadata={"status": status},
        )

        # 2. quarantine_threshold check (< 20% quarantine rate)
        quality = entry.get("quality", {})
        acc = quality.get("accepted", 0)
        quar = quality.get("quarantined", 0)
        issues = quality.get("issues", 0)
        blocking_issues = quality.get("blocking_issues", 0)
        total_recs = acc + quar
        quar_rate = (quar / total_recs) if total_recs > 0 else 0.0
        quar_passed = quar_rate <= 0.20

        yield AssetCheckResult(
            check_name="quarantine_threshold",
            passed=quar_passed,
            metadata={
                "accepted": acc,
                "quarantined": quar,
                "quarantine_rate": f"{quar_rate * 100:.2f}%",
                "issues": issues,
                "blocking_issues": blocking_issues,
            },
        )

        # 3. iceberg_publication check
        iceberg_staged = entry.get("iceberg", [])
        iceberg_passed = len(iceberg_staged) > 0 if config.publish_iceberg and acc > 0 else True
        yield AssetCheckResult(
            check_name="iceberg_publication",
            passed=iceberg_passed,
            metadata={
                "enabled": config.publish_iceberg,
                "staged_tables": len(iceberg_staged),
                "details": str(iceberg_staged) if iceberg_staged else "none",
            },
        )

        # 4. MaterializeResult
        yield MaterializeResult(
            metadata={
                "resource": spec.resource_name,
                "source_date": source_date.isoformat(),
                "status": status,
                "run_id": report.get("run_id", ""),
                "accepted": acc,
                "quarantined": quar,
                "issues": issues,
                "blocking_issues": blocking_issues,
                "output_files": len(entry.get("files", [])),
                "iceberg_tables_staged": len(iceberg_staged),
            }
        )

    return silver_asset


silver_assets = [build_silver_asset(spec) for spec in SILVER_SPECS]


class IcebergSyncConfig(Config):
    """Configuration for batch Iceberg synchronization job."""

    table: str | None = Field(
        default=None,
        description="Optional single table to synchronize (defaults to all tables)",
    )
    year: int | None = Field(
        default=None,
        description="Optional year to filter partitions (e.g. 2024)",
    )


@op(
    required_resource_keys={"object_storage"},
    description="Batch synchronize Silver Parquet data on S3 into Apache Iceberg Format v2",
)
def sync_silver_iceberg_op(context: OpExecutionContext, config: IcebergSyncConfig) -> dict[str, Any]:
    """Execute streaming chunked sync into Iceberg tables and refresh SIMD latest views."""
    from procurement.cli.silver import sync_iceberg

    # Execute sync_iceberg with clean streaming bounds
    sync_iceberg(table=config.table, resource=None, year=config.year)
    return {"status": "completed", "table": config.table or "all", "year": config.year or "all"}


@job(
    name="silver_sync_iceberg",
    description="Batch sync Silver Parquet files into Apache Iceberg Format v2",
)
def silver_sync_iceberg_job():
    sync_silver_iceberg_op()


__all__ = [
    "SILVER_SPECS",
    "SilverAssetSpec",
    "SilverConfig",
    "build_silver_asset",
    "silver_assets",
    "silver_sync_iceberg_job",
    "sync_silver_iceberg_op",
]
