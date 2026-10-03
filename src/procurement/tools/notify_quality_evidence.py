"""Capture search contexts and compare known detail endpoints without changing Bronze."""

import argparse
from pathlib import Path

from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.notify_contractor.resource import NotifyContractorApi
from procurement.quality.contracts import load_config
from procurement.quality.evidence import (
    ProbeBudget,
    export_config,
    load_snapshot,
    probe,
    revalidate_evidence,
    sample_workflows,
    snapshot,
)
from procurement.quality.files import cli, read_json, write_json


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    capture = commands.add_parser("snapshot")
    capture.add_argument("--year", type=int, required=True)
    capture.add_argument("--output", type=Path, required=True)
    capture.add_argument("--resume", action="store_true")
    check = commands.add_parser("probe")
    check.add_argument("--snapshot", type=Path, required=True)
    check.add_argument("--output", type=Path, required=True)
    check.add_argument("--config", type=Path)
    check.add_argument("--max-requests", type=int, default=800)
    check.add_argument("--resume", action="store_true")
    export = commands.add_parser("export-config", help="Export exact verified workflows for review")
    export.add_argument("--evidence", type=Path, required=True)
    export.add_argument("--config", type=Path)
    export.add_argument("--output", type=Path, required=True)
    recheck = commands.add_parser("revalidate", help="Recheck saved responses without HTTP requests")
    recheck.add_argument("--evidence", type=Path, required=True)
    recheck.add_argument("--config", type=Path)
    recheck.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.mode == "revalidate":
        if args.output.exists():
            parser.error("Output exists; choose a new path")
        write_json(args.output, revalidate_evidence(read_json(args.evidence), load_config(args.config)))
        return
    if args.mode == "export-config":
        result = export_config(read_json(args.evidence), load_config(args.config), args.output)
        print(f"Exported {len(result.routes)} exact rules to {args.output}")
        return
    if args.mode == "snapshot":
        with MuasamcongClient(token=settings.MUASAMCONG_TOKEN) as client:
            snapshot(NotifyContractorApi(client), args.year, args.output, resume=args.resume)
        return
    if args.max_requests < 1:
        parser.error("--max-requests must be positive")
    config = load_config(args.config)
    index, days = load_snapshot(args.snapshot)
    groups = sample_workflows(days)
    if args.output.exists() and not args.resume:
        parser.error("Output exists; use --resume")
    report = read_json(args.output) if args.resume else {
        "schema_version": 1, "snapshot_hash": calculate_content_hash(index),
        "config_hash": config.fingerprint, "requests": 0, "workflows": {},
    }
    if (report["snapshot_hash"] != calculate_content_hash(index)
            or report["config_hash"] != config.fingerprint):
        raise ValueError("Snapshot/config changed; start a new evidence report")
    def checkpoint(used):
        report["requests"] = used
        write_json(args.output, report)
    budget = ProbeBudget(args.max_requests, used=report["requests"], checkpoint=checkpoint)
    with MuasamcongClient(token=settings.MUASAMCONG_TOKEN, request_budget=budget) as client:
        probe(client, groups, config, args.output, report)


if __name__ == "__main__":
    raise SystemExit(cli(main))
