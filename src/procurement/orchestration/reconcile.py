"""Reconcile existing Lakehouse storage partitions (Bronze & Silver) into Dagster state."""

import time
from typing import Any

from dagster import (
    AssetKey,
    AssetMaterialization,
    Config,
    DagsterInstance,
    OpExecutionContext,
    job,
    op,
)
from pydantic import Field

from procurement.common.settings import settings
from procurement.orchestration.resources import create_s3_filesystem
from procurement.orchestration.silver import SILVER_GROUPS

BRONZE_FOLDER_MAP: dict[str, str] = {
    "bronze_project": "project_detail",
    "bronze_khlcnt": "khlcnt_plan_detail",
    "bronze_notify_contractor": "notify_contractor_standard_detail",
    "bronze_contractor_result": "contractor_result_detail",
}


def discover_s3_partitions(fs, bucket: str, prefix: str, year: int | None = None) -> list[str]:
    """Discover date partitions on S3 matching source_date=YYYY-MM-DD."""
    pattern = f"{bucket}/{prefix}/source_date={year}-*-*" if year else f"{bucket}/{prefix}/source_date=*-*-*"
    paths = fs.glob(pattern)
    dates = []
    for p in paths:
        base = p.rsplit("/", 1)[-1]
        if base.startswith("source_date="):
            d_str = base[len("source_date=") : len("source_date=") + 10]
            if len(d_str) == 10 and d_str[4] == "-" and d_str[7] == "-":
                dates.append(d_str)
    return sorted(set(dates))


def reconcile_lakehouse_to_dagster(
    instance: DagsterInstance | None = None,
    fs=None,
    bucket: str | None = None,
    year: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Scan existing S3 Bronze & Silver partitions and register missing materializations in Dagster."""
    inst = instance or DagsterInstance.get()
    storage_fs = fs or create_s3_filesystem()
    target_bucket = bucket or settings.OBJECT_STORAGE_BUCKET

    results: dict[str, int] = {}

    # 1. Reconcile Bronze Assets
    for asset_name, folder in BRONZE_FOLDER_MAP.items():
        key = AssetKey(asset_name)
        existing_parts = set(inst.get_materialized_partitions(key))
        s3_dates = discover_s3_partitions(
            storage_fs, target_bucket, f"bronze/muasamcong/{folder}", year=year
        )
        missing_dates = [d for d in s3_dates if d not in existing_parts]

        if not dry_run:
            for d in missing_dates:
                mat = AssetMaterialization(
                    asset_key=key,
                    partition=d,
                    description="Reconciled from existing Bronze S3 Parquet partition",
                )
                inst.report_runless_asset_event(mat)

        results[asset_name] = len(missing_dates)

    # 2. Reconcile Silver Table Assets
    for group in SILVER_GROUPS:
        for t in group.tables:
            asset_name = f"silver_{t.table}"
            key = AssetKey(asset_name)
            existing_parts = set(inst.get_materialized_partitions(key))
            s3_dates = discover_s3_partitions(
                storage_fs, target_bucket, f"silver/{t.table}", year=year
            )
            missing_dates = [d for d in s3_dates if d not in existing_parts]

            if not dry_run:
                for d in missing_dates:
                    mat = AssetMaterialization(
                        asset_key=key,
                        partition=d,
                        description="Reconciled from existing Silver S3 Parquet partition",
                    )
                    inst.report_runless_asset_event(mat)

            results[asset_name] = len(missing_dates)

    return results


class ReconcileConfig(Config):
    """Configuration for state reconciliation job."""

    year: int | None = Field(
        default=None,
        description="Optional filter year (e.g. 2024), default is all years",
    )
    dry_run: bool = Field(
        default=False,
        description="Scan and report missing partition count without writing events",
    )


@op(
    required_resource_keys={"object_storage"},
    description="Scan S3 storage and synchronize historical partitions into Dagster catalog",
)
def reconcile_lakehouse_op(context: OpExecutionContext, config: ReconcileConfig) -> dict[str, Any]:
    """Execute state reconciliation and emit progress logs."""
    context.log.info(f"Starting Lakehouse state reconciliation (year={config.year}, dry_run={config.dry_run})...")
    t0 = time.time()
    fs = context.resources.object_storage

    added_by_asset = reconcile_lakehouse_to_dagster(
        instance=context.instance,
        fs=fs,
        bucket=settings.OBJECT_STORAGE_BUCKET,
        year=config.year,
        dry_run=config.dry_run,
    )
    duration = time.time() - t0
    total_added = sum(added_by_asset.values())

    context.log.info(
        f"Reconciliation completed in {duration:.2f}s: {total_added} partitions registered across {len(added_by_asset)} assets."
    )
    return {"status": "completed", "total_added": total_added, "by_asset": added_by_asset}


@job(
    name="reconcile_lakehouse_to_dagster",
    description="Synchronize existing S3 Bronze and Silver partitions into Dagster Materializations",
)
def reconcile_lakehouse_job():
    reconcile_lakehouse_op()


if __name__ == "__main__":

    print("Scanning S3 Lakehouse and synchronizing Dagster state...")
    res = reconcile_lakehouse_to_dagster()
    print("-----------------------------------------------------------------")
    print("Asset Name                      | Reconciled Partitions")
    print("-----------------------------------------------------------------")
    for asset, count in sorted(res.items()):
        print(f"{asset:32}| {count:5} partitions")
    print("-----------------------------------------------------------------")
    print(f"Total partitions synchronized: {sum(res.values()):,}")
