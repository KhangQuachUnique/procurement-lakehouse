"""Approved memory-only resume; reuse the unchanged frozen bootstrap algorithm."""

import argparse
import json
from pathlib import Path

from procurement.tools.gold_history_bootstrap import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--budget-used", type=float, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_isolated:
        raise SystemExit("Explicit isolated execution required")
    from dotenv import load_dotenv

    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect, literal

    load_dotenv(args.env_file)
    cfg = Settings.from_environment().iceberg_config()
    spill = args.output.resolve() / "spill"
    spill.mkdir(parents=True, exist_ok=True)
    with connect(cfg, max_memory="3GB", threads=1) as db, RestCatalog(cfg) as catalog:
        db.execute("SET temp_directory=" + literal(str(spill)))
        db.execute("SET max_temp_directory_size='20GiB'")
        print(json.dumps({"approved_duckdb_memory": db.execute(
            "SELECT current_setting('memory_limit')"
        ).fetchone()[0], "unchanged_process_hard_limit": "3GiB"}), flush=True)
        print(json.dumps(run(args.source, args.output, db, catalog,
                             budget_used=args.budget_used)), flush=True)


if __name__ == "__main__":
    main()
