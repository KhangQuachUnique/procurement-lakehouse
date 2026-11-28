"""Unified CLI entrypoint for Spark Gold (``gold-spark``).

Examples::

    gold-spark rebuild --k3s --start-date 2022-01-01 --end-date 2026-10-31
    gold-spark maintain --k3s --namespace gold_spark --dry-run-orphans
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any

from procurement.gold.runner import run_spark_gold_incremental, run_spark_gold_rebuild
from procurement.spark.cli_common import add_spark_runtime_args, settings_from_cli_args
from procurement.spark.maintenance import main as maintenance_main


def _render_gold_summary(summary: dict[str, Any], *, master: str | None = None) -> None:
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()
        title = (
            f"Spark Gold ({summary['gold_namespace']} <- {summary['silver_namespace']}"
            + (f" | master={master}" if master else "")
            + f") — {summary['status']} ({summary['wall_s']}s)"
        )
        table = Table(title=title, show_lines=False)
        table.add_column("Table", style="bold")
        table.add_column("Rows", justify="right")
        for tbl, cnt in sorted(summary["table_counts"].items()):
            table.add_row(tbl, f"{cnt:,}")
        console.print(table)
        console.print(
            f"[bold green]Award Amount Sum (reconciled):[/bold green] {summary['award_amount_sum']} VND"
        )
        return
    except ImportError:
        pass

    print(
        f"[gold-spark] {summary['gold_namespace']} <- {summary['silver_namespace']} "
        f"status={summary['status']} tables={len(summary['table_counts'])} "
        f"award_sum={summary['award_amount_sum']} wall_s={summary['wall_s']}"
    )
    for tbl, cnt in sorted(summary["table_counts"].items()):
        print(f"  - {tbl}: {cnt:,}")


def _build_rebuild_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gold rebuild",
        description="Full Two-Tier Gold rebuild on Spark 4.0 + Iceberg WAP.",
    )
    parser.add_argument("--start-date", default="2022-01-01", help="Event start YYYY-MM-DD.")
    parser.add_argument("--end-date", default="2026-10-31", help="Event end YYYY-MM-DD.")
    parser.add_argument(
        "--observation-day",
        default=None,
        help="Snapshot observation date YYYY-MM-DD (default: --end-date).",
    )
    parser.add_argument("--silver-namespace", default="silver_spark")
    parser.add_argument("--namespace", default="gold_spark", help="Target public Gold namespace.")
    parser.add_argument(
        "--core-namespace",
        default="gold_spark_core",
        help="Target Tier 1 Core Gold namespace.",
    )
    parser.add_argument(
        "--skip-core-persist",
        action="store_true",
        help="Skip persisting Tier 1 core tables to Iceberg (only write 17 public mart tables).",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    add_spark_runtime_args(parser)
    return parser


def _build_top_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gold",
        description="Apache Spark 4.0 + Iceberg Two-Tier Gold CLI.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="Incremental Two-Tier Gold sync (start-snapshot-id + MERGE INTO).", add_help=False)
    sub.add_parser("rebuild", help="Full Two-Tier Gold rebuild via Iceberg WAP.", add_help=False)
    sub.add_parser("maintain", help="Run Iceberg maintenance on Gold namespace.", add_help=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if not raw or raw[0] in {"-h", "--help"}:
        _build_top_parser().print_help()
        return 0

    cmd, rest = raw[0], raw[1:]
    if cmd == "run":
        args = _build_rebuild_parser().parse_args(rest)
        cfg = settings_from_cli_args(args, namespace=args.silver_namespace)
        summary = run_spark_gold_incremental(
            event_start=date.fromisoformat(args.start_date),
            event_end=date.fromisoformat(args.end_date),
            observation_day=date.fromisoformat(args.observation_day) if args.observation_day else None,
            silver_namespace=args.silver_namespace,
            gold_namespace=args.namespace,
            core_namespace=args.core_namespace,
            settings=cfg,
        )
        if args.as_json:
            print(json.dumps(summary, indent=2))
        else:
            _render_gold_summary(summary, master=cfg.spark_master)
        return 0 if summary["status"] in {"PUBLISHED", "NO_OP"} else 1

    if cmd == "rebuild":
        args = _build_rebuild_parser().parse_args(rest)
        cfg = settings_from_cli_args(args, namespace=args.silver_namespace)
        summary = run_spark_gold_rebuild(
            event_start=date.fromisoformat(args.start_date),
            event_end=date.fromisoformat(args.end_date),
            observation_day=date.fromisoformat(args.observation_day) if args.observation_day else None,
            silver_namespace=args.silver_namespace,
            gold_namespace=args.namespace,
            core_namespace=args.core_namespace,
            persist_core=not args.skip_core_persist,
            settings=cfg,
        )
        if args.as_json:
            print(json.dumps(summary, indent=2))
        else:
            _render_gold_summary(summary, master=cfg.spark_master)
        return 0 if summary["status"] == "PUBLISHED" else 1

    if cmd == "maintain":
        if "--namespace" not in rest:
            rest = ["--namespace", "gold_spark", *rest]
        return maintenance_main(rest)

    _build_top_parser().error(f"Unknown command: {cmd!r} (choose from: rebuild, maintain)")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
