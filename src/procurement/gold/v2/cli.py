"""Isolated contract/plan commands; no implicit catalog connection or publication."""

import json
from pathlib import Path

import typer

from .contracts import RunPlan, canonical, logic_version
from .schema import TABLES
from .state import StateStore

app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command("schema")
def schema():
    """Print the exact approved 16-table contract."""
    typer.echo(
        json.dumps(
            {
                n: {
                    "columns": {c.name: c.sql_type for c in t.columns},
                    "key": t.key,
                    "grain": t.grain if n.startswith("fact_") else None,
                }
                for n, t in TABLES.items()
            },
            indent=2,
        )
    )


@app.command("version")
def version():
    typer.echo(logic_version())


@app.command("plan")
def plan(request: Path, root: Path):
    """Validate and persist an explicit frozen plan; never read/publish remote data."""
    model = RunPlan.model_validate_json(request.read_text())
    if model.logic_version != logic_version():
        raise ValueError("Plan logic differs from installed code")
    with StateStore(root) as state:
        state.create(model)
        typer.echo(canonical(state.status(model.run_id)))


@app.command("status")
def status(run_id: str, root: Path):
    """Inspect a local run without opening the warehouse."""
    if not (root / "state.duckdb").is_file():
        raise ValueError("No existing V2 state")
    with StateStore(root) as state:
        typer.echo(canonical(state.status(run_id)))


@app.command("resume")
def resume(run_id: str, root: Path, allow_isolated_publication: bool = False):
    """Resume frozen run; explicit opt-in writes ONLY its isolated v2 namespace."""
    from .runtime import execute_run

    result = execute_run(
        run_id,
        root,
        allow_isolated_publication=allow_isolated_publication,
        on_phase=lambda phase: typer.echo("phase=" + phase.value),
    )
    typer.echo(canonical(result))


@app.command("backup")
def backup(destination: Path, root: Path):
    """Back up locked local state only; never copies remote Iceberg files."""
    from .recovery import backup as execute

    typer.echo(canonical(execute(root, destination)))


@app.command("restore")
def restore(source: Path, destination: Path):
    """Restore to a NEW directory; remote reconciliation required before writes."""
    from .recovery import restore as execute

    typer.echo(canonical(execute(source, destination)))


@app.command("verify")
def verify(root: Path):
    """Full read-only remote snapshot comparison against accepted local state."""
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect

    from .adapters.iceberg import IcebergPublisher
    from .verification import verify as execute

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote, RestCatalog(config) as catalog:
        typer.echo(canonical(execute(root, IcebergPublisher(remote, catalog))))


@app.command("retirement-audit")
def retirement_audit(repository: Path, archive_path: Path | None = None):
    """Inventory legacy import blockers; optional verified archive, NEVER delete."""
    from .retirement import archive, audit

    result = archive(repository, archive_path) if archive_path else audit(repository)
    typer.echo(canonical(result))


@app.command("materialize-history")
def materialize_history(
    source: Path,
    output: Path,
    allow_managed_publication: bool = False,
    profile_first_partition: bool = False,
):
    """Publish managed Gold 2022-2025 from already validated canonical Silver."""
    from procurement.tools.gold_main_history import run

    result = run(
        source,
        output,
        execute_managed=allow_managed_publication,
        profile_first_partition=profile_first_partition,
    )
    typer.echo(canonical(result))


@app.command("ensure-tables")
def ensure_tables(
    namespace: str = typer.Option("gold_v2", help="Target Iceberg namespace in catalog 'lake'"),
):
    """Provision all 16 tables in lake.{namespace} with monthly partitioning on facts."""
    from procurement.common.settings import Settings
    from procurement.gold.application.warehouse import ensure_gold_tables
    from procurement.storage.iceberg import RestCatalog, connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote, RestCatalog(config) as catalog:
        results = ensure_gold_tables(remote, catalog=catalog, namespace=namespace)
        typer.echo(f"Successfully provisioned {len(results)} tables in lake.{namespace}")


@app.command("seed-geography")
def seed_geo(
    namespace: str = typer.Option("gold_v2", help="Target Iceberg namespace in catalog 'lake'"),
):
    """Seed dim_geography with the authoritative 63 provinces dictionary."""
    from procurement.common.settings import Settings
    from procurement.gold.application.warehouse import seed_geography
    from procurement.storage.iceberg import connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote:
        count = seed_geography(remote, namespace=namespace)
        typer.echo(f"Seeded dim_geography in lake.{namespace}: {count} records")


@app.command("promote")
def promote(
    source_namespace: str = typer.Option("gold_v2", help="Source physical namespace in catalog 'lake'"),
    target_schema: str = typer.Option("gold", help="Target schema for zero-copy views"),
):
    """Promote physical lake.{source_namespace}.* tables to zero-copy views in {target_schema}.*."""
    from procurement.common.settings import Settings
    from procurement.gold.application.warehouse import promote_views
    from procurement.storage.iceberg import connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote:
        views = promote_views(remote, source_namespace=source_namespace, target_schema=target_schema)
        typer.echo(f"Promoted {len(views)} zero-copy views into {target_schema}.*")


@app.command("reconcile")
def reconcile(
    gold_schema: str = typer.Option("lake.gold_v2", help="Gold physical or view schema"),
    silver_schema: str = typer.Option("silver", help="Silver schema"),
):
    """Run automated 3-part financial and volume reconciliation between Gold and Silver."""
    from procurement.common.settings import Settings
    from procurement.gold.application.reconciliation import reconcile_gold
    from procurement.storage.iceberg import connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote:
        report = reconcile_gold(remote, gold_schema=gold_schema, silver_schema=silver_schema)
        typer.echo(
            json.dumps(
                {
                    "is_valid": report.is_valid,
                    "gold_planned_amount": str(report.gold_planned_amount),
                    "gold_award_amount": str(report.gold_award_amount),
                    "orphan_fks": report.orphan_fks,
                    "metrics": report.metrics,
                    "errors": report.errors,
                },
                indent=2,
            )
        )
        if not report.is_valid:
            raise typer.Exit(code=1)


def main():
    app()


if __name__ == "__main__":
    main()
