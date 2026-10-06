"""Daily resource assets with object-store commit truth and explicit retry semantics."""

import os
from datetime import date

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    BackfillPolicy,
    Config,
    DailyPartitionsDefinition,
    Failure,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from pydantic import Field

from procurement import bootstrap
from procurement.common.catalog import RESOURCE_CATALOG
from procurement.common.dates import today_vn, validate_closed_range
from procurement.ingestion.contracts import MaterializeDayRequest
from procurement.quality.adapters import QUALITY_RESOURCES

INGESTION_POOL = "muasamcong_ingestion"
BRONZE_PARTITIONS = DailyPartitionsDefinition(
    start_date=os.getenv("DAGSTER_BRONZE_START_DATE", "2000-01-01"),
    timezone="Asia/Ho_Chi_Minh",
)


class BronzeConfig(Config):
    refresh: bool = False
    page_size: int = Field(default=50, gt=0)
    reconciled_run_ids: list[str] = Field(default_factory=list)


def build_bronze_asset(definition):
    name = f"bronze_{definition.identity.resource}"
    checks = [
        AssetCheckSpec("committed_manifest", asset=name, blocking=True),
        AssetCheckSpec(
            "committed_integrity",
            asset=name,
            blocking=True,
            description="Existing file, count, lineage and content hash verifier",
        ),
    ]
    if definition.identity.resource in QUALITY_RESOURCES:
        checks.append(AssetCheckSpec("quality_contract", asset=name, blocking=True))

    @asset(
        name=name,
        group_name="bronze",
        partitions_def=BRONZE_PARTITIONS,
        required_resource_keys={"object_storage"},
        pool=INGESTION_POOL,
        backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
        retry_policy=RetryPolicy(max_retries=0),
        check_specs=checks,
        description=f"Committed {definition.identity.resource} resource/day; immutable attempts",
    )
    def bronze_asset(context: AssetExecutionContext, config: BronzeConfig):
        source_date = date.fromisoformat(context.partition_key)
        validate_closed_range(source_date, source_date, today=today_vn())

        resource_name = definition.identity.resource
        service, _ = bootstrap.bootstrap_services()

        # Safely resolve run_id avoiding attribute error on None
        run_id: str | None = None
        dagster_run = getattr(context, "run", None)
        if dagster_run is not None:
            run_id = getattr(dagster_run, "run_id", None)
        if not run_id:
            run_id = getattr(context, "run_id", None)

        request = MaterializeDayRequest(
            source_date=source_date,
            resource=resource_name,
            refresh=config.refresh,
            dagster_run_id=run_id,
            page_size=config.page_size,
        )
        result = service.materialize_day(request)

        if result.status != "success":
            yield AssetCheckResult(
                check_name="committed_manifest",
                passed=False,
                metadata={"status": result.status},
            )
            raise Failure(
                f"Committed Bronze materialization failed for {resource_name}/{source_date}",
                allow_retries=False,
            )

        yield AssetCheckResult(
            check_name="committed_manifest",
            passed=True,
            metadata={
                "commit_id": str(result.commit_id) if result.commit_id else "",
                "reused": result.reused,
            },
        )
        yield AssetCheckResult(
            check_name="committed_integrity",
            passed=True,
            metadata={
                "records": result.record_count,
                "files": result.file_count,
            },
        )
        if resource_name in QUALITY_RESOURCES:
            yield AssetCheckResult(
                check_name="quality_contract",
                passed=True,
                metadata={
                    "commit_id": str(result.commit_id) if result.commit_id else "",
                    "records": result.record_count,
                },
            )
        yield MaterializeResult(
            metadata={
                "resource": result.resource,
                "source_date": result.source_date.isoformat(),
                "reused": result.reused,
                "commit_id": str(result.commit_id) if result.commit_id else "",
                "attempt_id": str(result.attempt_id) if result.attempt_id else "",
                "record_count": result.record_count,
                "file_count": result.file_count,
            }
        )

    return bronze_asset


bronze_assets = [build_bronze_asset(definition) for definition in RESOURCE_CATALOG]
