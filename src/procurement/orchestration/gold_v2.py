"""Gold scheduler adapter. All computation/lifecycle stays in the shared runtime."""

from dagster import Config, Definitions, MetadataValue, OpExecutionContext, job, op
from pydantic import Field


class GoldRunConfig(Config):
    run_id: str
    root: str
    allow_isolated_publication: bool = False


@op
def execute_gold_v2(context: OpExecutionContext, config: GoldRunConfig) -> dict:
    from procurement.gold.application.runtime import execute_run

    result = execute_run(
        config.run_id,
        config.root,
        allow_isolated_publication=config.allow_isolated_publication,
        on_phase=lambda phase: context.log.info("phase=" + phase.value),
    )
    context.add_output_metadata({"run_id": config.run_id, "result": MetadataValue.json(result)})
    return result


@job
def gold_v2_run_job():
    execute_gold_v2()


# Explicitly separate code location during acceptance. No schedule is enabled;
# production defs/legacy entrypoint remain intact until cutover authorization.
defs = Definitions(jobs=[gold_v2_run_job])


# Daily adapter for the future replacement code location. Inputs are sealed only
# after required Silver resources have materialized in THIS run.
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


class GoldDailyV2Config(Config):
    root: str
    target_namespace: str
    source_start_date: str
    source_namespace: str = "silver"
    prior_reports: list[str] = Field(default_factory=list)
    event_start_date: str = "2022-01-01"
    event_end_date: str = "2025-12-31"
    references_json: str = "{}"
    memory_limit: str = "2GB"
    batch_rows: int = 50000
    change_journal: str | None = None
    allow_isolated_publication: bool = False


def same_run_reports(context):
    wanted = {"silver_plan_revision", "silver_notice_revision", "silver_result_revision"}
    result = {}
    for record in context.instance.all_logs(context.run_id):
        event = record.dagster_event
        if event and event.is_step_materialization:
            materialization = event.event_specific_data.materialization
            name = materialization.asset_key.to_user_string()
            if name in wanted:
                metadata = materialization.metadata
                report = metadata.get("gold_readiness_report")
                if report is not None:
                    result[name] = report.value
    if set(result) != wanted:
        raise ValueError("Gold v2 requires complete current-run Silver readiness reports")
    return list(result.values())


def load_prior_reports(path, namespace):
    """Accept plain writer report, published ACK, or sealed baseline bundle.

    Bundle evidence is expanded to original per-day reports, never flattened into
    false clean quality. The source barrier rechecks actual pins/ancestry/proof.
    """
    import json

    value = json.loads(path.read_text())
    if "receipt" in value and "evidence" in value:
        from procurement.gold.domain.models import SilverReadyInput

        ready = SilverReadyInput.model_validate(value["receipt"])
        if ready.namespace != namespace:
            raise ValueError("Baseline namespace differs from configured Silver source")
        return [
            {
                "run_id": item["run_id"],
                "resource": item["resource"],
                "mapping_version": item["mapping_version"],
                "family": "all",
                "iceberg_enabled": True,
                "iceberg_namespace": namespace,
                "days": [item["entry"]],
            }
            for item in value["evidence"]
        ]
    if "report" in value and "identity" in value:
        value = value["report"]
    if value.get("resource") not in {"khlcnt", "notify_contractor", "contractor_result"}:
        raise ValueError("Unsupported baseline report shape")
    return [value]


def plan_after_silver(context, config):
    from datetime import date

    from procurement.common.dates import today_vn
    from procurement.common.settings import Settings
    from procurement.gold.application.state import StateStore
    from procurement.gold.domain.models import RunPlan, SilverReadyInput, logic_version
    from procurement.gold.domain.policies import ReferencePolicy
    from procurement.readiness_with_exception import seal_with_null_policy as seal_readiness
    from procurement.silver.application.readiness import save_bundle
    from procurement.storage.iceberg import RestCatalog, connect, identifier

    run_id = "dagster_" + context.run_id.replace("-", "_")
    root = Path(config.root)
    # Retry continues immutable old input vector, not newly materialized snapshots.
    if (root / "state.duckdb").exists():
        with StateStore(root) as state:
            exists = state.db.execute(
                "SELECT count(*) FROM control.runs WHERE run_id=?", [run_id]
            ).fetchone()[0]
            if exists:
                return state.plan(run_id)
    current = same_run_reports(context)
    prior = []
    for path in config.prior_reports:
        prior.extend(load_prior_reports(Path(path), config.source_namespace))
    policy = ReferencePolicy.model_validate_json(config.references_json)
    start = date.fromisoformat(config.source_start_date)
    cutoff = date.fromisoformat(context.partition_key)
    source_namespace = identifier(config.source_namespace)
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg, max_memory="512MB", threads=1) as remote, RestCatalog(cfg) as catalog:
        bundle = seal_readiness(
            prior + current,
            start=start,
            end=cutoff,
            namespace=source_namespace,
            catalog=catalog,
            count_rows=lambda table, snap: (
                remote.execute(
                    f"SELECT count(*) FROM lake.{source_namespace}.{table} AT (VERSION => {snap})"
                ).fetchone()[0]
                if snap is not None
                else 0
            ),
        )
    save_bundle(bundle, root / "readiness" / f"{run_id}.json")
    with StateStore(root) as state:
        base = state.db.execute(
            "SELECT release_id FROM control.accepted WHERE target=?", [config.target_namespace]
        ).fetchone()
        request = RunPlan(
            run_id=run_id,
            mode="incremental" if base else "backfill",
            logic_version=logic_version(),
            reference_version=policy.version,
            references_json=policy.model_dump_json(),
            target_namespace=config.target_namespace,
            base_release=base[0] if base else None,
            source=SilverReadyInput.model_validate(bundle["receipt"]),
            source_cutoff=cutoff,
            event_start=date.fromisoformat(config.event_start_date),
            event_end=date.fromisoformat(config.event_end_date),
            observation_day=today_vn(),
            change_journal=config.change_journal,
            budget={
                "memory_limit": config.memory_limit,
                "threads": 1,
                "batch_rows": config.batch_rows,
            },
        )
        state.create(request)
    return request


from procurement.orchestration.bronze import BRONZE_PARTITIONS


@asset(
    name="gold_v2_active_release",
    group_name="gold",
    deps=[
        AssetKey(n)
        for n in ("silver_plan_revision", "silver_notice_revision", "silver_result_revision")
    ],
    partitions_def=BRONZE_PARTITIONS,
)
def gold_v2_active_release(context: AssetExecutionContext, config: GoldDailyV2Config):
    from procurement.gold.application.runtime import execute_run

    if not config.allow_isolated_publication:
        raise ValueError("Gold v2 isolated publication requires explicit opt-in")
    request = plan_after_silver(context, config)
    result = execute_run(
        request.run_id,
        config.root,
        allow_isolated_publication=True,
        on_phase=lambda phase: context.log.info("phase=" + phase.value),
    )
    return MaterializeResult(
        metadata={"run_id": request.run_id, "result": MetadataValue.json(result)}
    )
