"""Daily Silver standardization assets with table-level Dagster representations."""

from dataclasses import dataclass
from datetime import date
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    BackfillPolicy,
    Config,
    Failure,
    MaterializeResult,
    OpExecutionContext,
    RetryPolicy,
    job,
    multi_asset,
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
class SilverTableSpec:
    """Specification of an individual Silver domain table."""

    table: str
    description: str


@dataclass(frozen=True)
class SilverResourceGroup:
    """Specification of a Silver resource generating multiple physical tables."""

    resource_name: str
    upstream_bronze_name: str
    primary_revision_table: str
    tables: tuple[SilverTableSpec, ...]


SILVER_GROUPS: tuple[SilverResourceGroup, ...] = (
    SilverResourceGroup(
        resource_name="project",
        upstream_bronze_name="bronze_project",
        primary_revision_table="project_revision",
        tables=(
            SilverTableSpec("project_revision", "Standardized procurement project revisions and metadata"),
            SilverTableSpec("project_location", "Geographical execution locations of projects"),
        ),
    ),
    SilverResourceGroup(
        resource_name="khlcnt",
        upstream_bronze_name="bronze_khlcnt",
        primary_revision_table="plan_revision",
        tables=(
            SilverTableSpec("plan_revision", "Procurement plan revisions and estimated budgets"),
            SilverTableSpec("package_revision", "Bid package revisions, procurement methods and timelines"),
            SilverTableSpec("package_lot", "Procurement bid package lots and price items"),
            SilverTableSpec("package_location", "Geographical execution locations of bid packages"),
        ),
    ),
    SilverResourceGroup(
        resource_name="notice",
        upstream_bronze_name="bronze_notify_contractor",
        primary_revision_table="notice_revision",
        tables=(
            SilverTableSpec("notice_revision", "Tender notice (TBMT) revisions, deadlines and requirements"),
            SilverTableSpec("notice_lot", "Tender notice bidding lots and itemized prices"),
            SilverTableSpec("notice_location", "Geographical execution locations of tender notices"),
            SilverTableSpec("notice_reoffer_submission", "Tender reoffer submissions and price quotes"),
        ),
    ),
    SilverResourceGroup(
        resource_name="contractor_result",
        upstream_bronze_name="bronze_contractor_result",
        primary_revision_table="result_revision",
        tables=(
            SilverTableSpec("result_revision", "Contractor selection result (KQLCNT) decisions"),
            SilverTableSpec("award_lot", "Winning bid lots, awarded prices and contract durations"),
            SilverTableSpec("award_contractor", "Winning contractors, consortium details and tax codes"),
        ),
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


def build_silver_multi_asset(group: SilverResourceGroup):
    """Build a partitioned multi-asset representing all physical tables for a Silver resource."""
    specs = [
        AssetSpec(
            key=f"silver_{t.table}",
            group_name="silver",
            deps=[AssetKey(group.upstream_bronze_name)],
            description=t.description,
            automation_condition=EAGER_AUTOMATION,
        )
        for t in group.tables
    ]

    primary_key = f"silver_{group.primary_revision_table}"
    checks = [
        AssetCheckSpec(
            "execution_status",
            asset=primary_key,
            blocking=True,
            description="Verify successful execution without unhandled errors",
        ),
        AssetCheckSpec(
            "quarantine_threshold",
            asset=primary_key,
            blocking=False,
            description="Warning if quarantine rate exceeds 20% of processed records",
        ),
        AssetCheckSpec(
            "iceberg_publication",
            asset=primary_key,
            blocking=False,
            description="Verify table partitions were staged/committed into Iceberg catalog",
        ),
    ]

    @multi_asset(
        name=f"silver_{group.resource_name}",
        specs=specs,
        check_specs=checks,
        partitions_def=BRONZE_PARTITIONS,
        required_resource_keys={"object_storage"},
        backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
        retry_policy=RetryPolicy(max_retries=0),
    )
    def silver_multi_asset(context: AssetExecutionContext, config: SilverConfig):
        source_date = date.fromisoformat(context.partition_key)
        validate_closed_range(source_date, source_date, today=today_vn())

        fs = context.resources.object_storage
        bucket = settings.OBJECT_STORAGE_BUCKET

        report = run_resource(
            fs=fs,
            bucket=bucket,
            resource=group.resource_name,
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
                asset_key=AssetKey(primary_key),
                passed=False,
                metadata={"error": "Empty execution report returned"},
            )
            raise Failure(
                f"Silver materialization failed: no report for {group.resource_name}/{source_date}",
                allow_retries=False,
            )

        entry = days[0]
        status = entry.get("status", "unknown")

        if status == "failed":
            error_msg = entry.get("error", "Unknown processing error")
            yield AssetCheckResult(
                check_name="execution_status",
                asset_key=AssetKey(primary_key),
                passed=False,
                metadata={"error": error_msg, "error_type": entry.get("error_type", "")},
            )
            raise Failure(
                f"Silver materialization failed for {group.resource_name}/{source_date}: {error_msg}",
                allow_retries=False,
            )

        # 1. execution_status check
        yield AssetCheckResult(
            check_name="execution_status",
            asset_key=AssetKey(primary_key),
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
            asset_key=AssetKey(primary_key),
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
        iceberg_staged = entry.get("iceberg", {})
        iceberg_passed = len(iceberg_staged) > 0 if config.publish_iceberg and acc > 0 else True
        yield AssetCheckResult(
            check_name="iceberg_publication",
            asset_key=AssetKey(primary_key),
            passed=iceberg_passed,
            metadata={
                "enabled": config.publish_iceberg,
                "staged_tables": len(iceberg_staged),
                "details": str(iceberg_staged) if iceberg_staged else "none",
            },
        )

        files = entry.get("files", {})

        # 4. MaterializeResult for each individual table in the group
        for t in group.tables:
            tbl_name = t.table
            tbl_key = f"silver_{tbl_name}"
            tbl_iceberg = iceberg_staged.get(tbl_name, {}) if isinstance(iceberg_staged, dict) else {}
            parquet_file = files.get(tbl_name, "") if isinstance(files, dict) else ""

            meta: dict[str, Any] = {
                "table": tbl_name,
                "resource": group.resource_name,
                "source_date": source_date.isoformat(),
                "status": status,
                "parquet_path": parquet_file,
            }
            if tbl_name == group.primary_revision_table:
                meta.update({
                    "run_id": report.get("run_id", ""),
                    "accepted": acc,
                    "quarantined": quar,
                    "issues": issues,
                })
            if tbl_iceberg:
                meta["iceberg_snapshot_id"] = tbl_iceberg.get("snapshot_id")
                meta["iceberg_total_rows"] = tbl_iceberg.get("row_count")

            yield MaterializeResult(
                asset_key=AssetKey(tbl_key),
                metadata=meta,
            )

    return silver_multi_asset


silver_assets = [build_silver_multi_asset(group) for group in SILVER_GROUPS]


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

    sync_iceberg(table=config.table, resource=None, year=config.year)
    return {"status": "completed", "table": config.table or "all", "year": config.year or "all"}


@job(
    name="silver_sync_iceberg",
    description="Batch sync Silver Parquet files into Apache Iceberg Format v2",
)
def silver_sync_iceberg_job():
    sync_silver_iceberg_op()


__all__ = [
    "SILVER_GROUPS",
    "SilverConfig",
    "SilverResourceGroup",
    "SilverTableSpec",
    "build_silver_multi_asset",
    "silver_assets",
    "silver_sync_iceberg_job",
    "sync_silver_iceberg_op",
]
