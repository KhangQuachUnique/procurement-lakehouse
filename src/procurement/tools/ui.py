"""Rich terminal UI helpers for procurement Lakehouse CLI tools."""

import os
import sys
from collections import Counter
from typing import Any

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table

console = Console()


def is_rich_enabled(args: Any = None) -> bool:
    """Determine whether to use rich interactive terminal formatting."""
    if args and getattr(args, "json", False):
        return False
    if os.environ.get("PROCUREMENT_JSON_OUTPUT", "").lower() in ("1", "true", "yes"):
        return False
    # If explicitly forcing rich UI
    if os.environ.get("PROCUREMENT_RICH_UI", "").lower() in ("1", "true", "yes"):
        return True
    return sys.stdout.isatty()


def format_bytes(num_bytes: float | None) -> str:
    """Format byte count into human-readable representation."""
    if num_bytes is None or num_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(num_bytes)
    unit_idx = 0
    while val >= 1024.0 and unit_idx < len(units) - 1:
        val /= 1024.0
        unit_idx += 1
    return f"{val:.1f} {units[unit_idx]}" if unit_idx > 0 else f"{int(val)} B"


def create_progress_display() -> Progress:
    """Create a styled multi-task progress bar."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.fields[label]}[/bold cyan]"),
        TextColumn("[dim]({task.fields[sublabel]})[/dim]"),
        BarColumn(bar_width=25),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.fields[status]}"),
        console=console,
        transient=False,
    )


def render_transfer_summary(report_data: dict[str, Any], default_archive: str | None = None) -> None:
    """Render a modern Rich summary table and completion panel for Bronze transfers."""
    # Handle batch vs single archive report
    if report_data.get("mode") == "import_batch":
        reports_map = report_data.get("archives", {})
    else:
        # Single archive
        archive_name = default_archive or report_data.get("archive_name", f"bronze-{report_data.get('year', 'archive')}.zip")
        reports_map = {archive_name: report_data}

    table = Table(
        title="📦 Bronze Lakehouse Ingestion Summary",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Year", justify="center", style="bold yellow")
    table.add_column("Archive", style="dim")
    table.add_column("Resources Breakdown", style="magenta")
    table.add_column("Imported", justify="right", style="green")
    table.add_column("Skipped", justify="right", style="yellow")
    table.add_column("Objects", justify="right", style="cyan")
    table.add_column("Data Size", justify="right", style="white")
    table.add_column("Status", justify="center")

    total_imported = 0
    total_skipped = 0
    total_objects = 0
    total_bytes = 0
    all_success = True

    for arch_name, rep in sorted(reports_map.items()):
        is_error = "error" in rep
        if is_error:
            all_success = False
            table.add_row(
                "N/A",
                arch_name,
                f"[red]{rep.get('error', 'Error')}[/red]",
                "0",
                "0",
                "0",
                "0 B",
                "[bold red]✖ FAILED[/bold red]",
            )
            continue

        mode = rep.get("mode", "import")
        year = str(rep.get("year", "N/A"))
        imported_days = rep.get("imported_days", [])
        skipped_days = rep.get("skipped_days", [])

        if mode == "inspect":
            num_imported = rep.get("selected_days", rep.get("verified", {}).get("days", 0))
            num_skipped = 0
            uploaded_objs = rep.get("objects", rep.get("verified", {}).get("files", 0))
            uploaded_bytes = rep.get("bytes", 0)
            breakdown = ", ".join(rep.get("resources", []))
            status = "[bold green]✔ VALIDATED[/bold green]"
        else:
            num_imported = len(imported_days)
            num_skipped = len(skipped_days)
            uploaded_objs = rep.get("uploaded_objects", 0)
            uploaded_bytes = rep.get("uploaded_bytes", rep.get("bytes", 0))
            if imported_days:
                counter = Counter(item["resource"] for item in imported_days)
            else:
                counter = Counter(item["resource"] for item in skipped_days)
            breakdown = ", ".join(f"{res} ({cnt})" for res, cnt in sorted(counter.items())) or ", ".join(rep.get("resources", [])) or "none"
            status = "[bold green]✔ SUCCESS[/bold green]"

        total_imported += num_imported
        total_skipped += num_skipped
        total_objects += uploaded_objs
        total_bytes += uploaded_bytes

        table.add_row(
            year,
            arch_name,
            breakdown,
            f"{num_imported:,}",
            f"{num_skipped:,}",
            f"{uploaded_objs:,}",
            format_bytes(uploaded_bytes),
            status,
        )

    console.print()
    console.print(table)
    console.print()

    # Determine if any was inspect mode
    first_mode = next((r.get("mode") for r in reports_map.values() if isinstance(r, dict)), "import")

    if all_success:
        if first_mode == "inspect":
            summary_text = (
                f"[bold green]✨ Archive integrity verified offline successfully![/bold green]\n"
                f"[bold]Total Validated Days:[/] {total_imported:,}  │  "
                f"[bold]Objects Checked:[/] {total_objects:,}  │  "
                f"[bold]Archive Size:[/] {format_bytes(total_bytes)}"
            )
            console.print(Panel(summary_text, title="🎉 Verification Complete", border_style="green", box=box.ROUNDED))
        else:
            summary_text = (
                f"[bold green]✨ All Bronze archives imported successfully into Object Store![/bold green]\n"
                f"[bold]Total Imported Days:[/] {total_imported:,}  │  "
                f"[bold]Skipped (Already on S3):[/] {total_skipped:,}  │  "
                f"[bold]Objects Uploaded:[/] {total_objects:,}  │  "
                f"[bold]Total Ingested Size:[/] {format_bytes(total_bytes)}"
            )
            console.print(Panel(summary_text, title="🎉 Ingestion Complete", border_style="green", box=box.ROUNDED))
    else:
        summary_text = (
            f"[bold red]✖ Bronze transfer completed with errors.[/bold red]\n"
            f"Imported: {total_imported:,} days  │  Skipped: {total_skipped:,} days  │  "
            f"Please check error details above."
        )
        console.print(Panel(summary_text, title="⚠️ Ingestion Warning", border_style="red", box=box.ROUNDED))
    console.print()


def render_metadata_summary(reports: list[dict[str, Any]]) -> None:
    """Render a modern Rich summary table for PostgreSQL metadata migration."""
    table = Table(
        title="🗄️ PostgreSQL Metadata Sync Summary",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Resource", style="bold magenta")
    table.add_column("Manifests (S3)", justify="right", style="cyan")
    table.add_column("Imported to DB", justify="right", style="green")
    table.add_column("Skipped (Existing)", justify="right", style="yellow")
    table.add_column("Mismatches", justify="right", style="dim")
    table.add_column("Status", justify="center")

    total_manifests = 0
    total_imported = 0
    total_skipped = 0
    all_success = True

    for rep in reports:
        if "error" in rep:
            all_success = False
            table.add_row(
                rep.get("resource", "unknown"),
                "0",
                "0",
                "0",
                "0",
                f"[bold red]✖ {rep['error']}[/bold red]",
            )
            continue

        resource = rep.get("resource", "unknown")
        manifests = rep.get("total_manifest_days", rep.get("manifests_scanned", 0))
        imported = rep.get("imported", 0)
        skipped = rep.get("skipped", 0)
        mismatches = rep.get("mismatches", 0)
        is_dry = rep.get("dry_run", False)

        total_manifests += manifests
        total_imported += imported
        total_skipped += skipped

        status = "[yellow]DRY RUN[/yellow]" if is_dry else "[bold green]✔ SUCCESS[/bold green]"

        table.add_row(
            resource,
            f"{manifests:,}",
            f"{imported:,}",
            f"{skipped:,}",
            f"{mismatches:,}",
            status,
        )

    console.print()
    console.print(table)
    console.print()

    if all_success:
        summary_text = (
            f"[bold green]✨ S3 manifests successfully indexed into PostgreSQL![/bold green]\n"
            f"[bold]Total Manifests Scanned:[/] {total_manifests:,}  │  "
            f"[bold]Imported to DB:[/] {total_imported:,}  │  "
            f"[bold]Skipped (Already in DB):[/] {total_skipped:,}"
        )
        console.print(Panel(summary_text, title="🎉 Metadata Sync Complete", border_style="green", box=box.ROUNDED))
    else:
        summary_text = "[bold red]✖ Metadata sync encountered one or more errors.[/bold red]"
        console.print(Panel(summary_text, title="⚠️ Metadata Sync Warning", border_style="red", box=box.ROUNDED))
    console.print()


def render_record_counts(report: dict[str, Any], path: Any, cached: bool = False) -> None:
    """Render record counts table and summary panel."""
    title_prefix = "⚡ Cached Snapshot" if cached else "📊 Lakehouse Bronze Record Counts"
    start_str = report.get("start", "")
    end_str = report.get("end", "")
    range_str = f" ({start_str} to {end_str})" if start_str and end_str else ""

    table = Table(
        title=f"{title_prefix}{range_str}",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Year", justify="center", style="bold yellow")
    table.add_column("Resource", style="bold magenta")
    table.add_column("Bronze Records", justify="right", style="green")

    rows = report.get("rows", [])
    total_records = 0
    if rows:
        for r in rows:
            cnt = r.get("records", 0)
            total_records += cnt
            table.add_row(str(r.get("year", "")), r.get("resource", ""), f"{cnt:,}")
    else:
        for label, count in sorted(report.get("totals", {}).items()):
            total_records += count
            table.add_row("All", label, f"{count:,}")

    console.print()
    console.print(table)
    console.print()

    complete = report.get("complete", True)
    issues = report.get("issues", [])
    missing = report.get("missing_days", {})
    missing_items = [f"{k}: {v}" for k, v in missing.items() if v > 0]
    missing_str = ", ".join(missing_items) if missing_items else "None"

    if complete:
        status_line = "[bold green]✔ Complete committed dataset: 0 issues, 0 missing days.[/bold green]"
        border = "green"
    else:
        status_line = f"[bold yellow]⚠️ Incomplete: {len(issues)} issue(s), missing days: {missing_str}[/bold yellow]"
        border = "yellow"

    elapsed = report.get("elapsed_seconds")
    elapsed_str = f"  │  [bold]Elapsed:[/] {elapsed}s" if elapsed is not None else ""

    panel_text = (
        f"{status_line}\n"
        f"[bold]Total Records:[/] {total_records:,}  │  "
        f"[bold]Snapshot Time:[/] {report.get('counted_at', '')}{elapsed_str}\n"
        f"[dim]Report: {path}[/dim]"
    )
    console.print(Panel(panel_text, title="📋 Count Summary", border_style=border, box=box.ROUNDED))
    console.print()


def render_compaction_plan(result: dict[str, Any], plan_path: Any) -> None:
    """Render Bronze compaction plan details."""
    table = Table(
        title="📦 Bronze Compaction Plan",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Plan Parameter", style="bold white")
    table.add_column("Value", style="cyan")

    candidate_days = len(result.get("days", []))
    skipped_days = len(result.get("skipped", []))
    input_files = sum(len(d.get("files", [])) for d in result.get("days", []))
    input_bytes = sum(d.get("input_bytes", 0) for d in result.get("days", []))

    table.add_row("Plan ID", str(result.get("plan_id", "N/A")))
    table.add_row("Resource", str(result.get("resource", "all")))
    table.add_row("Candidate Days", f"{candidate_days:,}")
    table.add_row("Skipped Days", f"{skipped_days:,}")
    table.add_row("Input Files", f"{input_files:,}")
    table.add_row("Total Input Size", format_bytes(input_bytes))
    table.add_row("Target File Size", format_bytes(result.get("target_bytes", 128 * 1024 * 1024)))
    table.add_row("Plan File", str(plan_path))

    console.print()
    console.print(table)
    console.print()
    panel_text = (
        f"[bold green]✨ Compaction plan created successfully![/bold green]\n"
        f"To apply this plan under execution lock, run:\n"
        f"[bold cyan]uv run python -m procurement.tools.compact_bronze run --plan {plan_path}[/bold cyan]"
    )
    console.print(Panel(panel_text, title="🚀 Ready to Compact", border_style="green", box=box.ROUNDED))
    console.print()


def render_compaction_run(report: dict[str, Any], report_path: Any) -> None:
    """Render Bronze compaction run report."""
    is_complete = report.get("complete", False)
    days_count = len(report.get("days", []))

    status = "[bold green]✔ COMPLETE[/bold green]" if is_complete else "[bold red]✖ INCOMPLETE[/bold red]"
    border = "green" if is_complete else "red"

    panel_text = (
        f"Status: {status}\n"
        f"[bold]Compacted Days:[/] {days_count:,}\n"
        f"[dim]Report saved to: {report_path}[/dim]"
    )
    console.print()
    console.print(Panel(panel_text, title="📦 Bronze Compaction Results", border_style=border, box=box.ROUNDED))
    console.print()


def render_ingestion_summary(report: dict[str, Any]) -> None:
    """Render partition ingestion summary table and panel."""
    is_single = "results" not in report
    results = [report] if is_single else report.get("results", [])

    table = Table(
        title="📥 Bronze Partition Ingestion Summary",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Date", justify="center", style="yellow")
    table.add_column("Resource", style="magenta")
    table.add_column("Records", justify="right", style="green")
    table.add_column("Files", justify="right", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Commit ID", style="dim")

    success_count = 0
    total_records = 0
    total_files = 0

    for r in results:
        status_val = r.get("status", "unknown")
        is_ok = status_val == "success"
        if is_ok:
            success_count += 1
            if r.get("reused"):
                status_text = "[bold green]✔ REUSED[/bold green]"
            else:
                status_text = "[bold green]✔ SUCCESS[/bold green]"
        else:
            status_text = f"[bold red]✖ {status_val.upper()}[/bold red]"

        rec_cnt = r.get("record_count", 0)
        file_cnt = r.get("file_count", 0)
        total_records += rec_cnt
        total_files += file_cnt

        commit_id = str(r.get("commit_id") or "")[:8] + "..." if r.get("commit_id") else "-"
        table.add_row(
            r.get("source_date", "N/A"),
            r.get("resource", "N/A"),
            f"{rec_cnt:,}",
            f"{file_cnt:,}",
            status_text,
            commit_id,
        )

    console.print()
    console.print(table)
    console.print()

    all_ok = success_count == len(results)
    border = "green" if all_ok else "yellow"
    if all_ok:
        summary_msg = (
            f"[bold green]✨ Materialized {success_count}/{len(results)} partition days successfully![/bold green]\n"
            f"[bold]Total Bronze Records:[/] {total_records:,}  │  "
            f"[bold]Files Written:[/] {total_files:,}"
        )
    else:
        summary_msg = (
            f"[bold yellow]⚠️ Ingestion finished with failures: {len(results) - success_count} failed.[/bold yellow]\n"
            f"[bold]Processed Days:[/] {len(results)}  │  "
            f"[bold]Successful Days:[/] {success_count}"
        )

    title = "🎉 Ingestion Complete" if all_ok else "⚠️ Ingestion Warning"
    console.print(Panel(summary_msg, title=title, border_style=border, box=box.ROUNDED))
    console.print()


def render_explorer_startup(
    port: int,
    elapsed: float,
    bronze_views: list[str],
    silver_views: list[str],
    issues: list[Any],
) -> None:
    """Render DuckDB Lakehouse Explorer startup banner."""
    schemas = ["bronze_raw"]
    if silver_views:
        schemas.append("silver")

    content = (
        f"[bold green]🚀 Lakehouse DuckDB Explorer is live![/bold green] [dim]({elapsed:.2f}s startup)[/dim]\n\n"
        f"  🌐 [bold cyan]Web UI:[/] [link=http://localhost:{port}]http://localhost:{port}[/link]\n"
        f"  🗂️  [bold]Active Schemas:[/] {', '.join(schemas)}\n"
        f"  📦 [bold]Bronze Views ({len(bronze_views)}):[/] {', '.join(bronze_views)}\n"
    )
    if silver_views:
        content += f"  🥈 [bold]Silver Views ({len(silver_views)}):[/] {', '.join(silver_views)}\n"

    if issues:
        content += (
            f"\n  [bold yellow]⚠️ INCOMPLETE:[/] {len(issues)} day(s) missing files.\n"
            f"  [dim]Query: SELECT * FROM bronze_meta.selection_issues;[/dim]\n"
        )

    content += (
        "\n[dim]Tip: Use --union-by-name if Parquet files have schema variations.[/dim]\n"
        "[bold white]Press [red]Ctrl+C[/red] to stop the server.[/bold white]"
    )
    console.print()
    console.print(Panel(content, title="🦆 DuckDB Lakehouse Explorer", border_style="cyan", box=box.ROUNDED))
    console.print()


def render_watcher_summary(mode: str, result: dict[str, Any]) -> None:
    """Render Bid Opening Watcher summary."""
    table = Table(
        title=f"👀 Bid Opening Watcher ({mode.upper()})",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Property", style="bold white")
    table.add_column("Value", style="cyan")

    for k, v in result.items():
        if isinstance(v, (dict, list)):
            val_str = f"{len(v)} item(s)" if isinstance(v, list) else f"{len(v)} key(s)"
        else:
            val_str = str(v)
        table.add_row(k.replace("_", " ").title(), val_str)

    console.print()
    console.print(table)
    console.print()


def render_repair_summary(mode: str, data: dict[str, Any]) -> None:
    """Render Bronze Quality Repair summary."""
    table = Table(
        title=f"🔧 Bronze Quality Repair ({mode.upper()})",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold blue",
    )
    table.add_column("Metric", style="bold white")
    table.add_column("Value", style="cyan")

    for k, v in data.items():
        if isinstance(v, dict):
            val_str = ", ".join(f"{sub_k}: {sub_v}" for sub_k, sub_v in v.items())
        elif isinstance(v, list):
            val_str = f"{len(v)} item(s)"
        else:
            val_str = str(v)
        table.add_row(k.replace("_", " ").title(), val_str)

    console.print()
    console.print(table)
    console.print()

