"""CLI entrypoint for evidence-based Bronze profiling audit (delegates to procurement.profiling)."""

from __future__ import annotations

import sys

from procurement.profiling.adapters.driver.cli import run_cli

if __name__ == "__main__":
    sys.exit(run_cli())
