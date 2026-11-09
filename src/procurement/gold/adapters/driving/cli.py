"""Driving Typer CLI adapter for the Gold dimensional layer.

Hexagonal Driving Inbound Adapter.
"""

import json

import typer

from procurement.gold.application.reconciliation import reconcile_gold
from procurement.gold.application.warehouse import ensure_gold_tables, promote_views, seed_geography
from procurement.gold.v2.cli import app as v2_app

app = typer.Typer(pretty_exceptions_show_locals=False)

# Re-register all v2 commands
for cmd in v2_app.registered_commands:
    app.registered_commands.append(cmd)


@app.command("ensure-tables")
def cmd_ensure_tables(
    namespace: str = typer.Option("gold_v2", help="Target Iceberg namespace in catalog 'lake'"),
):
    """Provision all 16 tables in lake.{namespace} with monthly partitioning on facts."""
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote, RestCatalog(config) as catalog:
        results = ensure_gold_tables(remote, catalog=catalog, namespace=namespace)
        typer.echo(f"Successfully provisioned {len(results)} tables in lake.{namespace}")


@app.command("seed-geography")
def cmd_seed_geography(
    namespace: str = typer.Option("gold_v2", help="Target Iceberg namespace in catalog 'lake'"),
):
    """Seed dim_geography with the authoritative 63 provinces dictionary."""
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote:
        count = seed_geography(remote, namespace=namespace)
        typer.echo(f"Seeded dim_geography in lake.{namespace}: {count} records")


@app.command("promote")
def cmd_promote(
    source_namespace: str = typer.Option("gold_v2", help="Source physical namespace in catalog 'lake'"),
    target_schema: str = typer.Option("gold", help="Target schema for zero-copy views"),
):
    """Promote physical lake.{source_namespace}.* tables to zero-copy views in {target_schema}.*."""
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote:
        views = promote_views(remote, source_namespace=source_namespace, target_schema=target_schema)
        typer.echo(f"Promoted {len(views)} zero-copy views into {target_schema}.*")


@app.command("reconcile")
def cmd_reconcile(
    gold_schema: str = typer.Option("lake.gold_v2", help="Gold physical or view schema"),
    silver_schema: str = typer.Option("silver", help="Silver schema"),
):
    """Run automated 3-part financial and volume reconciliation between Gold and Silver."""
    from procurement.common.settings import Settings
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
