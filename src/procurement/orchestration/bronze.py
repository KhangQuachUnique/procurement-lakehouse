"""Daily resource assets with object-store commit truth and explicit retry semantics."""

import os
from collections import Counter
from datetime import date
from pathlib import Path
from time import perf_counter

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    BackfillPolicy,
    Config,
    DailyPartitionsDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from pydantic import Field

from procurement.common.catalog import RESOURCE_CATALOG
from procurement.common.dates import today_vn, validate_closed_range
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.jobs.lock import execution_lock
from procurement.jobs.runner import run_resource_day
from procurement.models.control import DayStatus
from procurement.quality.adapters import QUALITY_RESOURCES
from procurement.quality.audit import audit_day
from procurement.quality.contracts import load_config
from procurement.storage.committed import select_committed_days, verify_committed
from procurement.storage.control import DayCommitUncertainError, read_day_manifest

INGESTION_POOL = "muasamcong_ingestion"
MANIFEST_READ_WORKERS = 8
BRONZE_PARTITIONS = DailyPartitionsDefinition(
    start_date=os.getenv("DAGSTER_BRONZE_START_DATE", "2000-01-01"),
    timezone="Asia/Ho_Chi_Minh",
)


class BronzeConfig(Config):
    refresh: bool = False
    page_size: int = Field(default=50, gt=0)
    reconciled_run_ids: list[str] = Field(default_factory=list)


def _committed_attempt(fs, definition, source_date, config):
    """Caller holds the host lock through both write and verification."""
    coverage = read_coverage(
        fs,
        definition.identity,
        source_date,
        source_date,
        workers=MANIFEST_READ_WORKERS,
    )[0]
    unresolved = set(coverage.active_run_ids) - set(config.reconciled_run_ids)
    if unresolved:
        raise Failure(
            "An active or unresolved attempt requires reconciliation before another write",
            metadata={"run_ids": MetadataValue.json(sorted(unresolved))},
            allow_retries=False,
        )
    reused = coverage.effective is not None and not config.refresh
    run_id = (
        coverage.effective.run_id
        if reused
        else run_resource_day(
            definition.identity.resource,
            source_date,
            source=definition.identity.source,
            page_size=config.page_size,
        )
    )
    manifest = read_day_manifest(fs, definition.identity, run_id, source_date)
    if manifest is None or manifest.status is not DayStatus.SUCCESS:
        raise Failure(
            "The exact attempt has no SUCCESS DayManifest; inspect storage before retrying",
            metadata={
                "run_id": run_id,
                "source_date": str(source_date),
                "status": manifest.status.value if manifest else "missing",
            },
            allow_retries=False,
        )
    if (
        manifest.run_id != run_id
        or manifest.source_date != source_date
        or manifest.source != definition.identity.source
        or manifest.resource != definition.identity.resource
    ):
        raise Failure("DayManifest identity mismatch", allow_retries=False)
    selection = select_committed_days(
        fs,
        definition,
        source_date,
        source_date,
        dataset=definition.identity.source,
        workers=MANIFEST_READ_WORKERS,
    )
    if len(selection) != 1 or selection[0].run_id != run_id:
        raise Failure("Committed selection changed during execution", allow_retries=False)
    return manifest, selection, reused


def _quality_result(fs, definition, day):
    config = load_config(resource=definition.identity.resource)
    report = audit_day(
        fs,
        {
            "resource": definition.identity.resource,
            "date": str(day.source_date),
            "run_id": day.run_id,
            "expected_records": day.expected_records,
            "files": [list(item) for item in day.files],
        },
        config,
        [],
    )
    counts = Counter(row["result"]["status"] for row in report["rows"])
    passed = report["status"] == "complete" and not (counts["fail"] or counts["unresolved"])
    return AssetCheckResult(
        check_name="quality_contract",
        passed=passed,
        metadata={
            "run_id": day.run_id,
            "quality_config_hash": config.fingerprint,
            "status": report["status"],
            "results": MetadataValue.json(dict(counts)),
        },
    )


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

        if definition.identity.resource == "project":
            from procurement.bootstrap import bootstrap_services
            from procurement.ingestion.contracts import MaterializeDayRequest

            service, _ = bootstrap_services()
            run_id = (
                context.run.run_id
                if hasattr(context, "run") and context.run
                else getattr(context, "run_id", None)
            )
            request = MaterializeDayRequest(
                source_date=source_date,
                resource="project",
                refresh=config.refresh,
                dagster_run_id=run_id,
                page_size=config.page_size,
            )
            result = service.materialize_day(request)
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
            return

        fs = context.resources.object_storage
        materialization_started = perf_counter()
        timings = {}
        try:
            with execution_lock(Path(settings.INGESTION_LOCK_DIR)):
                try:
                    phase_started = perf_counter()
                    manifest, selection, reused = _committed_attempt(
                        fs,
                        definition,
                        source_date,
                        config,
                    )
                    timings["manifest_selection_seconds"] = perf_counter() - phase_started
                    context.log.info(
                        "Committed selection: %.3fs (reused=%s)",
                        timings["manifest_selection_seconds"],
                        reused,
                    )
                except Exception as exc:
                    yield AssetCheckResult(
                        check_name="committed_manifest",
                        passed=False,
                        metadata={"error_type": type(exc).__name__},
                    )
                    raise
                yield AssetCheckResult(
                    check_name="committed_manifest",
                    passed=True,
                    metadata={"run_id": manifest.run_id},
                )
                try:
                    phase_started = perf_counter()
                    verified = verify_committed(fs, selection)
                    timings["integrity_seconds"] = perf_counter() - phase_started
                except Exception as exc:
                    yield AssetCheckResult(
                        check_name="committed_integrity",
                        passed=False,
                        metadata={"run_id": manifest.run_id, "error_type": type(exc).__name__},
                    )
                    raise Failure(
                        "Committed Bronze verification failed", allow_retries=False
                    ) from exc
                yield AssetCheckResult(
                    check_name="committed_integrity",
                    passed=True,
                    metadata={"run_id": manifest.run_id, **verified},
                )
                if definition.identity.resource in QUALITY_RESOURCES:
                    phase_started = perf_counter()
                    quality = _quality_result(fs, definition, selection[0])
                    timings["quality_seconds"] = perf_counter() - phase_started
                    yield quality
                    if not quality.passed:
                        raise Failure(
                            "Committed Bronze requires quality review", allow_retries=False
                        )
                metadata = manifest.model_dump(mode="json")
                metadata.update(
                    **timings,
                    materialization_seconds=perf_counter() - materialization_started,
                    reused_attempt=reused,
                    files=verified["files"],
                    table_names=MetadataValue.json(list(definition.tables)),
                    reconciled_run_ids=MetadataValue.json(config.reconciled_run_ids),
                )
                if manifest.completed_at:
                    metadata["duration_seconds"] = (
                        manifest.completed_at - manifest.started_at
                    ).total_seconds()
                yield MaterializeResult(metadata=metadata)
        except DayCommitUncertainError as exc:
            raise Failure(
                "Commit acknowledgement is uncertain; reconcile manifests before retrying",
                allow_retries=False,
            ) from exc

    return bronze_asset


bronze_assets = [build_bronze_asset(definition) for definition in RESOURCE_CATALOG]
