"""Benchmark read-only source APIs; same runner and persistent controls as the Ops UI."""
import argparse
import json
from pathlib import Path

from procurement.benchmark.models import BenchmarkConfig, StageConfig
from procurement.benchmark.runner import BenchmarkRunner
from procurement.benchmark.service import BenchmarkService
from procurement.benchmark.store import BenchmarkStore
from procurement.common.settings import settings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "worker", "status", "stop", "stage"):
        command = commands.add_parser(name)
        command.add_argument("--state-dir", type=Path, default=Path(settings.BENCHMARK_STATE_DIR))
        command.add_argument("--export-dir", type=Path, default=Path(settings.BENCHMARK_EXPORT_DIR))
        if name == "run":
            command.add_argument("--config", type=Path, required=True)
            command.add_argument("--background", action="store_true")
        elif name == "status":
            command.add_argument("run_id", nargs="?")
        else:
            command.add_argument("run_id")
        if name == "stage":
            command.add_argument("--max-inflight", type=int, required=True)
            command.add_argument("--request-interval", type=float, required=True)
            command.add_argument("--duration", type=float, default=60)
            command.add_argument("--warmup", type=float, default=5)
    args = parser.parse_args(argv)
    store = BenchmarkStore(args.state_dir, args.export_dir)
    service = BenchmarkService(store)
    try:
        if args.command == "worker":
            BenchmarkRunner(store, args.run_id).execute()
            return
        service.recover()
        if args.command == "run":
            config = BenchmarkConfig.model_validate_json(args.config.read_text(encoding="utf-8"))
            if not settings.MUASAMCONG_TOKEN:
                parser.error("MUASAMCONG_TOKEN is not configured")
            if args.background:
                result = service.start(config)
            else:
                run_id = store.create(config)
                print(f"Run: {run_id}", flush=True)
                runner = BenchmarkRunner(store, run_id)
                try:
                    runner.execute()
                except KeyboardInterrupt:
                    runner.halt("keyboard_interrupt")
                    raise
                result = store.get(run_id)
        elif args.command == "status":
            result = store.get(args.run_id) if args.run_id else store.list()
        else:
            stage = StageConfig(max_inflight=args.max_inflight, request_interval=args.request_interval,
                                duration_seconds=args.duration, warmup_seconds=args.warmup
                                ) if args.command == "stage" else None
            store.command(args.run_id, stage)
            result = store.get(args.run_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.command == "run" and isinstance(result, dict) and result["status"] == "failed":
            return 1
    except (ValueError, KeyError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
