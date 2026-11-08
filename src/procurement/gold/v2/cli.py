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


def main():
    app()


if __name__ == "__main__":
    main()
