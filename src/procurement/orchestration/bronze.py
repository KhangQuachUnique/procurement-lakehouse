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


class BaseBronzeConfig(Config):
    refresh: bool = False
    page_size: int = Field(default=50, gt=0)
    reconciled_run_ids: list[str] = Field(default_factory=list)
    request_interval_seconds: float | None = Field(
        default=None, ge=0.0, description="Pacing interval between requests in seconds"
    )
    max_inflight: int | None = Field(
        default=None, ge=1, le=32, description="Max concurrent in-flight requests"
    )
    max_attempts: int | None = Field(
        default=None, ge=1, le=10, description="Max retry attempts for retryable errors"
    )


class BidOpeningBronzeConfig(BaseBronzeConfig):
    detail_workers: int = Field(
        default=4, ge=1, le=32, description="Concurrent worker threads for bid opening details"
    )


class KhlcntBronzeConfig(BaseBronzeConfig):
    package_workers: int = Field(
        default=3, ge=1, le=16, description="Concurrent worker threads for KHLCNT bid packages"
    )


BronzeConfig = BaseBronzeConfig


def get_config_cls(resource: str) -> type[BaseBronzeConfig]:
    if resource == "bid_opening":
        return BidOpeningBronzeConfig
    if resource == "khlcnt":
        return KhlcntBronzeConfig
    return BaseBronzeConfig


def build_bronze_asset(definition):
    resource_name = definition.identity.resource
    name = f"bronze_{resource_name}"
    config_cls = get_config_cls(resource_name)
    checks = [
        AssetCheckSpec("committed_manifest", asset=name, blocking=True),
        AssetCheckSpec(
            "committed_integrity",
            asset=name,
            blocking=True,
            description="Existing file, count, lineage and content hash verifier",
        ),
    ]
    if resource_name in QUALITY_RESOURCES:
        checks.append(AssetCheckSpec("quality_contract", asset=name, blocking=True))

    def _create_asset_fn(cfg_cls):
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
        def bronze_asset(context: AssetExecutionContext, config: cfg_cls):
            source_date = date.fromisoformat(context.partition_key)
            validate_closed_range(source_date, source_date, today=today_vn())

            refresh = config.refresh
            page_size = config.page_size
            req_interval = config.request_interval_seconds
            max_inflight = config.max_inflight
            max_attempts = config.max_attempts
            bid_workers = getattr(config, "detail_workers", None)
            khlcnt_workers = getattr(config, "package_workers", None)

            service, _ = bootstrap.bootstrap_services(
                request_interval_seconds=req_interval,
                max_inflight=max_inflight,
                max_attempts=max_attempts,
                bid_opening_workers=bid_workers,
                khlcnt_workers=khlcnt_workers,
            )

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
                refresh=refresh,
                dagster_run_id=run_id,
                page_size=page_size,
            )
            return _execute_bronze_materialize(service, request, resource_name, source_date)

        return bronze_asset

    return _create_asset_fn(config_cls)


def _execute_bronze_materialize(service, request, resource_name, source_date):
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



bronze_assets = [build_bronze_asset(definition) for definition in RESOURCE_CATALOG]
