"""Shared CLI flags (--k3s, --master, --driver-memory) and optional Rich rendering."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from procurement.spark.config import SparkSilverSettings


def add_spark_runtime_args(parser: argparse.ArgumentParser) -> None:
    """Attach standard Spark cluster/runtime flags to a CLI parser."""
    parser.add_argument(
        "--k3s",
        action="store_true",
        help="Run on the K3s Spark cluster (auto-detects LAN IP for master & driver host).",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Override Spark master URL (e.g. local[6] or spark://192.168.1.11:7077).",
    )
    parser.add_argument(
        "--driver-memory",
        default=None,
        help="Override Spark driver memory (e.g. 3g, 4g).",
    )


def settings_from_cli_args(
    args: argparse.Namespace,
    *,
    namespace: str | None = None,
) -> SparkSilverSettings:
    """Build SparkSilverSettings honoring --k3s, --master, --driver-memory, and --namespace."""
    return SparkSilverSettings.from_environment(
        load_dotenv_file=True,
        namespace=namespace,
        k3s=bool(getattr(args, "k3s", False)),
        spark_master=getattr(args, "master", None),
        spark_driver_memory=getattr(args, "driver_memory", None),
    )


def _status_style(status: str) -> str:
    upper = status.upper()
    if upper in {"OK", "PUBLISHED"}:
        return f"[bold green]{status}[/bold green]"
    if upper == "NO_OP":
        return f"[bold cyan]{status}[/bold cyan]"
    return f"[bold red]{status}[/bold red]"


def render_runner_summary(summary: dict[str, Any], *, master: str | None = None) -> None:
    """Render Spark Silver run summary using Rich if available, else clean plain text."""
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()
        title = (
            f"Spark Silver ({summary['namespace']} | mode={summary['mode']}"
            + (f" | master={master}" if master else "")
            + f") — {summary['status']} ({summary['wall_s']}s)"
        )
        table = Table(title=title, show_lines=False)
        table.add_column("Resource", style="bold")
        table.add_column("Status")
        table.add_column("Batches", justify="right")
        table.add_column("Missing Days", justify="right")
        table.add_column("Wall (s)", justify="right")

        for item in summary["results"]:
            table.add_row(
                str(item["resource"]),
                _status_style(str(item["status"])),
                str(len(item["batches"])),
                str(len(item["missing_bronze_dates"])),
                f"{item['wall_s']:.2f}",
            )
        console.print(table)
        if summary.get("resources_failed"):
            console.print(
                f"[bold red]FAILED resources:[/bold red] {', '.join(summary['resources_failed'])}"
            )
        return
    except ImportError:
        pass

    print(
        f"[spark-silver] namespace={summary['namespace']} mode={summary['mode']} "
        f"status={summary['status']} wall_s={summary['wall_s']}"
    )
    for item in summary["results"]:
        print(
            f"  - {item['resource']}: status={item['status']} "
            f"batches={len(item['batches'])} missing_days={len(item['missing_bronze_dates'])} "
            f"wall_s={item['wall_s']}"
        )
    if summary.get("resources_failed"):
        print(
            f"  FAILED resources: {', '.join(summary['resources_failed'])}",
            file=sys.stderr,
        )


def render_maintenance_summary(summary: dict[str, Any], *, master: str | None = None) -> None:
    """Render Iceberg maintenance summary using Rich if available, else clean plain text."""
    totals = summary["totals"]
    dry_run = bool(summary.get("options", {}).get("dry_run_orphans", False))
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()
        title = (
            f"Iceberg Maintenance ({summary['catalog']}.{summary['namespace']}"
            + (f" | master={master}" if master else "")
            + f") — {summary['status']} ({summary['wall_s']}s)"
        )
        table = Table(title=title, show_lines=False)
        table.add_column("Table", style="bold")
        table.add_column("Status")
        table.add_column("Data Files", justify="right")
        table.add_column("Manifests", justify="right")
        table.add_column("Expired", justify="right")
        table.add_column("Orphans" + (" (dry)" if dry_run else ""), justify="right")
        table.add_column("Wall (s)", justify="right")

        for tbl in summary["tables"]:
            table.add_row(
                str(tbl["table"]),
                _status_style(str(tbl["status"])),
                f"{tbl['rewritten_data_files_count']}→{tbl['added_data_files_count']}",
                f"{tbl['rewritten_manifests_count']}→{tbl['added_manifests_count']}",
                str(tbl["deleted_data_files_count"]),
                str(tbl["orphan_files_count"]),
                f"{tbl['wall_s']:.2f}",
            )
        table.add_section()
        table.add_row(
            f"TOTAL ({summary['tables_total']})",
            _status_style(str(summary["status"])),
            f"{totals['rewritten_data_files_count']}→{totals['added_data_files_count']}",
            f"{totals['rewritten_manifests_count']}→{totals['added_manifests_count']}",
            str(totals["deleted_data_files_count"]),
            str(totals["orphan_files_count"]),
            f"{summary['wall_s']:.2f}",
        )
        console.print(table)
        for tbl in summary["tables"]:
            if tbl["status"] != "OK":
                console.print(f"[bold red]FAILED {tbl['table']}:[/bold red] {tbl['error']}")
        return
    except ImportError:
        pass

    print(
        f"[iceberg-maintenance] {summary['catalog']}.{summary['namespace']} "
        f"status={summary['status']} tables={summary['tables_total']} "
        f"rewritten_files={totals['rewritten_data_files_count']}->{totals['added_data_files_count']} "
        f"rewritten_manifests={totals['rewritten_manifests_count']}->{totals['added_manifests_count']} "
        f"expired_data_files={totals['deleted_data_files_count']} "
        f"orphans={totals['orphan_files_count']}{' (dry-run)' if dry_run else ''} "
        f"wall_s={summary['wall_s']}"
    )
    for tbl in summary["tables"]:
        if tbl["status"] != "OK":
            print(f"  FAILED {tbl['table']}: {tbl['error']}", file=sys.stderr)
