"""Unified CLI entrypoint for Spark Silver (`silver-spark`).

Examples::

    silver-spark run --k3s
    silver-spark backfill --k3s --resource notice,khlcnt --start-date 2025-01-01 --end-date 2025-06-30
    silver-spark maintain --k3s --dry-run-orphans
"""

from __future__ import annotations

import argparse
import sys

from procurement_spark.platform.maintenance import main as maintenance_main
from procurement_spark.silver.runner import main as runner_main


def _build_top_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="silver-spark",
        description="Apache Spark 4.0 + Iceberg Silver Lakehouse CLI.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser(
        "run",
        help="Run incremental Silver sync (auto-discovers new/updated Bronze days).",
        add_help=False,
    )
    sub.add_parser(
        "backfill",
        help="Run monthly backfill over an explicit --start-date..--end-date range.",
        add_help=False,
    )
    sub.add_parser(
        "maintain",
        help="Run Iceberg maintenance (compaction, manifests, expire snapshots, orphan cleanup).",
        add_help=False,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if not raw_args or raw_args[0] in {"-h", "--help"}:
        _build_top_parser().print_help()
        return 0

    cmd, rest = raw_args[0], raw_args[1:]
    if cmd == "run":
        if "--mode" not in rest:
            rest = ["--mode", "incremental", *rest]
        return runner_main(rest)
    if cmd == "backfill":
        if "--mode" not in rest:
            rest = ["--mode", "backfill", *rest]
        return runner_main(rest)
    if cmd == "maintain":
        return maintenance_main(rest)

    _build_top_parser().error(f"Unknown command: {cmd!r} (choose from: run, backfill, maintain)")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
