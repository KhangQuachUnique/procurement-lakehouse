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
