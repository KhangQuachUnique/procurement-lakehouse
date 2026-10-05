"""Read-only quality audit of a frozen selection of committed Bronze days."""

import argparse
import json
from pathlib import Path

from procurement.common.catalog import SUPPORTED_RESOURCES
from procurement.common.dates import today_vn
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.audit import run_audit
from procurement.quality.contracts import load_config
from procurement.quality.evidence import load_snapshot
from procurement.quality.files import cli
from procurement.storage.object_store import create_s3_filesystem


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource", choices=SUPPORTED_RESOURCES, default="notify_contractor")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    if not 1 <= args.year < today_vn().year:
        parser.error("--year must be a closed year")
    config = load_config(args.config, resource=args.resource)
    if config.resource != args.resource:
        parser.error("A reviewed resource-specific config is required")
    index, days = load_snapshot(args.snapshot) if args.snapshot else (None, {})
    if index and index["year"] != args.year:
        parser.error("Snapshot year mismatch")
    report = run_audit(
        create_s3_filesystem(), resource=args.resource, year=args.year, config=config,
        output=args.output, search_days=days,
        snapshot_hash=calculate_content_hash(index) if index else None, resume=args.resume,
    )
    print(json.dumps({key: value for key, value in report.items()
                      if key not in {"workflows", "day_quality"}},
                     ensure_ascii=False, indent=2))
    return 0 if report["fully_verified"] else 2


if __name__ == "__main__":
    raise SystemExit(cli(main))
