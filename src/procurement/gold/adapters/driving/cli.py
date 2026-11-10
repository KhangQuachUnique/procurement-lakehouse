"""Driving Typer CLI adapter for the Gold dimensional layer.

Hexagonal Driving Inbound Adapter.
"""

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from procurement.gold.application.reconciliation import reconcile_gold
from procurement.gold.application.state import StateStore
from procurement.gold.application.warehouse import ensure_gold_tables, promote_views, seed_geography
from procurement.gold.domain.models import RunPlan, canonical, compute_gold_logic_version, logic_version
from procurement.gold.domain.schema import TABLES

console = Console(highlight=False)
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
    """Print current gold pipeline logic version hash."""
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
        raise ValueError("No existing Gold state")
    with StateStore(root) as state:
        typer.echo(canonical(state.status(run_id)))


@app.command("resume")
def resume(run_id: str, root: Path, allow_isolated_publication: bool = False):
    """Resume frozen run; explicit opt-in writes ONLY its isolated namespace."""
    from procurement.gold.application.runtime import execute_run

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
    from procurement.gold.application.recovery import backup as execute

    typer.echo(canonical(execute(root, destination)))


@app.command("restore")
def restore(source: Path, destination: Path):
    """Restore to a NEW directory; remote reconciliation required before writes."""
    from procurement.gold.application.recovery import restore as execute

    typer.echo(canonical(execute(source, destination)))


@app.command("verify")
def verify(root: Path):
    """Full read-only remote snapshot comparison against accepted local state."""
    from procurement.common.settings import Settings
    from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
    from procurement.gold.application.verification import verify as execute
    from procurement.storage.iceberg import RestCatalog, connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote, RestCatalog(config) as catalog:
        typer.echo(canonical(execute(root, IcebergPublisher(remote, catalog))))


@app.command("retirement-audit")
def retirement_audit(repository: Path, archive_path: Path | None = None):
    """Inventory legacy import blockers; optional verified archive, NEVER delete."""
    from procurement.gold.application.retirement import archive, audit

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
def cmd_ensure_tables(
    namespace: str = typer.Option("gold", help="Target Iceberg namespace in catalog 'lake'"),
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
    namespace: str = typer.Option("gold", help="Target Iceberg namespace in catalog 'lake'"),
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


def _display_gold_summary(
    title: str,
    scope_label: str,
    total_input: int,
    total_facts: int,
    fact_totals: dict[str, int],
    extract_time: float,
    biz_time: float,
    proj_time: float,
    publish_time: float,
    total_time: float,
    silver_ns: str,
    gold_ns: str,
):
    summary_table = Table(title="Conformed Fact Tables (Star Schema)", show_header=True, header_style="bold gold1")
    summary_table.add_column("Fact Table", style="cyan")
    summary_table.add_column("Rows Materialized", justify="right", style="green")
    summary_table.add_column("Grain", style="dim")

    from procurement.gold.domain.schema import TABLES

    for fact_name in sorted(fact_totals.keys()):
        cnt = fact_totals[fact_name]
        grain_str = ", ".join(TABLES[fact_name].grain) if fact_name in TABLES else "-"
        summary_table.add_row(fact_name, f"{cnt:,}", grain_str)

    summary_table.add_section()
    summary_table.add_row("[bold white]Total Fact Records[/]", f"[bold green]{total_facts:,}[/]", "[bold green]7 Fact Cohorts[/]")

    timing_table = Table(title="Execution Latency Breakdown", show_header=True, header_style="bold sky_blue2")
    timing_table.add_column("Pipeline Stage", style="white")
    timing_table.add_column("Duration", justify="right", style="cyan")
    timing_table.add_column("Throughput / Details", justify="right", style="green")

    throughput = f"{total_facts / max(total_time, 0.001):,.0f} facts/sec"
    timing_table.add_row("1. Extract Silver Revisions", f"{extract_time:.2f}s", f"{total_input / max(extract_time, 0.001):,.0f} rows/s")
    timing_table.add_row("2. Business Logic & Clustering", f"{biz_time:.2f}s", "Entity Linking & Dedup")
    timing_table.add_row("3. Projection & Group Aggregations", f"{proj_time:.2f}s", "Conformed Star Schemas")
    timing_table.add_row("4. Iceberg Lakehouse Publish", f"{publish_time:.2f}s", f"{total_facts / max(publish_time, 0.001):,.0f} facts/s")
    timing_table.add_section()
    timing_table.add_row("[bold white]Total Wall Time[/]", f"[bold yellow]{total_time:.2f}s[/]", f"[bold green]{throughput}[/]")

    console.print()
    console.print(Panel(summary_table, title=f"[bold]Gold Star Schema Summary — {scope_label}[/]", border_style="gold1"))
    console.print(Panel(timing_table, title="[bold]Performance & Latency Breakdown[/]", border_style="sky_blue2"))
    console.rule(f"[bold green]✔ Gold Dimensional Pipeline Completed Successfully (lake.{gold_ns}.*)[/]")


@app.command("run")
def cmd_run(
    date: str | None = typer.Option(None, "--date", "-d", help="Single date (YYYY-MM-DD) to process"),
    month: str | None = typer.Option(None, "--month", "-m", help="Target month (YYYY-MM) to process"),
    start_date: str | None = typer.Option(None, "--start-date", help="Start date (YYYY-MM-DD) for range"),
    end_date: str | None = typer.Option(None, "--end-date", help="End date (YYYY-MM-DD) for range"),
    silver_namespace: str = typer.Option("silver", help="Source Silver namespace in catalog 'lake'"),
    gold_namespace: str = typer.Option("gold", help="Target Gold namespace in catalog 'lake'"),
    memory_limit: str = typer.Option("4GB", help="DuckDB memory limit"),
    threads: int = typer.Option(4, help="DuckDB execution threads"),
):
    """Execute Gold dimensional processing for a day, month, or historical date range."""
    import time
    from collections import defaultdict
    from datetime import date as dt_date
    from procurement.gold.application.processor import (
        process_gold_day,
        process_gold_month,
    )

    if not (date or month or (start_date and end_date)):
        console.print("[bold red]✘ Error:[/] Please provide --date, --month, or both --start-date and --end-date")
        raise typer.Exit(code=1)

    scope_label = date if date else month if month else f"{start_date} ➔ {end_date}"

    console.print(
        Panel(
            f"[bold]Batch Gold Dimensional Pipeline Execution[/]\n"
            f"Architecture: [bold cyan]Conformed Star Schema (10 Dimensions + 7 Facts)[/]\n"
            f"Source: [bold green]lake.{silver_namespace}.*[/] ➔ Target: [bold gold1]lake.{gold_namespace}.*[/]\n"
            f"Date Scope: [bold yellow]{scope_label}[/]\n"
            f"Engine: [bold sky_blue2]DuckDB Vectorized + Apache Iceberg Format v2[/]\n"
            f"Budget: [white]Memory: {memory_limit} | Threads: {threads}[/]",
            title="[bold gold1]Procurement Lakehouse — Gold Layer[/]",
            border_style="gold1",
        )
    )

    t0 = time.perf_counter()
    fact_totals: dict[str, int] = defaultdict(int)
    total_input = 0
    total_extract_time = 0.0
    total_biz_time = 0.0
    total_proj_time = 0.0
    total_publish_time = 0.0

    if date:
        with console.status(f"[bold gold1]Processing Gold for day {date}...[/]"):
            res = process_gold_day(
                date,
                silver_namespace=silver_namespace,
                gold_namespace=gold_namespace,
                memory_limit=memory_limit,
                threads=threads,
            )
        total_input = res.get("input_rows", 0)
        total_extract_time = res.get("extract_time", 0.0)
        total_biz_time = res.get("biz_time", 0.0)
        total_proj_time = res.get("proj_time", 0.0)
        total_publish_time = res.get("publish_time", 0.0)
        for f, cnt in res.get("facts", {}).items():
            fact_totals[f] += cnt

    elif month:
        with console.status(f"[bold gold1]Processing Gold for month {month}...[/]"):
            res = process_gold_month(
                month,
                silver_namespace=silver_namespace,
                gold_namespace=gold_namespace,
                memory_limit=memory_limit,
                threads=threads,
            )
        total_input = res.get("input_rows", 0)
        total_extract_time = res.get("extract_time", 0.0)
        total_biz_time = res.get("biz_time", 0.0)
        total_proj_time = res.get("proj_time", 0.0)
        total_publish_time = res.get("publish_time", 0.0)
        for f, cnt in res.get("facts", {}).items():
            fact_totals[f] += cnt

    elif start_date and end_date:
        months = []
        cur = dt_date.fromisoformat(start_date).replace(day=1)
        end_dt = dt_date.fromisoformat(end_date)
        while cur <= end_dt:
            months.append(cur.strftime("%Y-%m"))
            next_month = cur.month + 1 if cur.month < 12 else 1
            next_year = cur.year if cur.month < 12 else cur.year + 1
            cur = dt_date(next_year, next_month, 1)

        total_facts_acc = 0
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold gold1]{task.fields[name]}[/]"),
            BarColumn(bar_width=25),
            TaskProgressColumn(),
            TextColumn("[white]{task.completed}/{task.total} months[/]"),
            TextColumn("• [dim]facts=[/][bold green]{task.fields[facts]:,}[/]"),
            TextColumn("• [dim]cur=[/][bold cyan]{task.fields[cur]}[/]"),
            TimeElapsedColumn(),
            console=console,
        ) as progress_ui:
            task_id = progress_ui.add_task(
                "Gold Backfill",
                total=len(months),
                name="Gold Backfill",
                facts=0,
                cur=months[0] if months else "",
            )
            for ym in months:
                progress_ui.update(task_id, cur=ym)
                res = process_gold_month(
                    ym,
                    silver_namespace=silver_namespace,
                    gold_namespace=gold_namespace,
                    memory_limit=memory_limit,
                    threads=threads,
                )
                m_facts = res.get("fact_rows", 0)
                total_facts_acc += m_facts
                total_input += res.get("input_rows", 0)
                total_extract_time += res.get("extract_time", 0.0)
                total_biz_time += res.get("biz_time", 0.0)
                total_proj_time += res.get("proj_time", 0.0)
                total_publish_time += res.get("publish_time", 0.0)
                for f, cnt in res.get("facts", {}).items():
                    fact_totals[f] += cnt
                progress_ui.update(task_id, advance=1, facts=total_facts_acc, cur=ym)

    wall_time = time.perf_counter() - t0
    total_facts = sum(fact_totals.values())
    _display_gold_summary(
        title="Gold Star Schema",
        scope_label=scope_label,
        total_input=total_input,
        total_facts=total_facts,
        fact_totals=fact_totals,
        extract_time=total_extract_time,
        biz_time=total_biz_time,
        proj_time=total_proj_time,
        publish_time=total_publish_time,
        total_time=wall_time,
        silver_ns=silver_namespace,
        gold_ns=gold_namespace,
    )


@app.command("reconcile")
def cmd_reconcile(
    gold_schema: str = typer.Option("lake.gold", help="Gold physical or view schema"),
    silver_schema: str = typer.Option("lake.silver", help="Silver schema"),
):
    """Run automated 3-part financial and volume reconciliation between Gold and Silver."""
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote:
        with console.status(f"[bold gold1]Running 3-part reconciliation: {gold_schema} vs {silver_schema}...[/]"):
            report = reconcile_gold(remote, gold_schema=gold_schema, silver_schema=silver_schema)

        reconcile_table = Table(title="3-Part Financial & Referential Reconciliation Audit", show_header=True, header_style="bold gold1")
        reconcile_table.add_column("Audit Metric", style="cyan")
        reconcile_table.add_column("Value / Status", justify="right", style="green")

        status_text = "[bold green]PASS (100% MATCH)[/]" if report.is_valid else "[bold red]FAIL (MISMATCH)[/]"
        reconcile_table.add_row("Audit Status", status_text)
        reconcile_table.add_row("Gold Total Planned Amount", f"{report.gold_planned_amount:,.2f} VND")
        reconcile_table.add_row("Gold Total Award Amount", f"{report.gold_award_amount:,.2f} VND")
        orphan_text = "[bold green]0 (Perfect Referential Integrity)[/]" if not report.orphan_fks else f"[bold red]{len(report.orphan_fks)} violations[/]"
        reconcile_table.add_row("Orphan Foreign Keys", orphan_text)

        console.print()
        console.print(Panel(reconcile_table, title=f"[bold]Reconciliation Audit — {gold_schema}[/]", border_style="green" if report.is_valid else "red"))

        if not report.is_valid:
            console.print(f"[bold red]Errors detected:[/] {report.errors}")
            raise typer.Exit(code=1)
        else:
            console.rule("[bold green]✔ All Financial Balances & Foreign Key Constraints Verified![/]")


def main():
    app()


if __name__ == "__main__":
    main()
