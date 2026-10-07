"""Transfer committed Bronze data between machines using a portable yearly ZIP."""

import argparse
import json
import logging
import signal
from pathlib import Path

from procurement.common.catalog import SUPPORTED_RESOURCES
from procurement.common.errors import sanitize_error_message
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.storage.object_store import create_s3_filesystem
from procurement.transfer import (
    TransferError,
    export_bundle,
    import_bundle,
    inspect_bundle,
    year_dates,
)


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    export = commands.add_parser("export", help="Export effective SUCCESS days for one closed year")
    export.add_argument("--year", type=int, required=True)
    export.add_argument("--resource", choices=("all", *SUPPORTED_RESOURCES), default="all")
    export.add_argument("--output", type=Path, required=True)
    inspect = commands.add_parser("inspect", help="Verify a ZIP offline, including Parquet contents")
    inspect.add_argument("--archive", type=Path, required=True)
    restore = commands.add_parser("import", help="Fill missing committed days from a verified ZIP")
    restore.add_argument("--archive", type=Path, required=True)
    for command in (export, restore):
        command.add_argument("--dry-run", action="store_true")
        command.add_argument("--lock-dir", type=Path, default=Path(settings.INGESTION_LOCK_DIR))
    return parser


def _safe_error(exc):
    message = sanitize_error_message(str(exc))
    for secret in (settings.OBJECT_STORAGE_ACCESS_KEY, settings.OBJECT_STORAGE_SECRET_KEY,
                   settings.MUASAMCONG_TOKEN):
        if secret:
            message = message.replace(secret, "[REDACTED]")
    return message


def main(argv=None):
    configure_logging()
    parser = _parser()
    args = parser.parse_args(argv)
    if args.mode == "export":
        try:
            year_dates(args.year)
        except ValueError as exc:
            parser.error(str(exc))

    def terminate(_signum, _frame):
        raise KeyboardInterrupt("Termination requested")

    previous = signal.signal(signal.SIGTERM, terminate)
    code = 0
    try:
        if args.mode == "inspect":
            report = inspect_bundle(args.archive)
        elif args.mode == "export":
            report = export_bundle(create_s3_filesystem(), year=args.year, output=args.output,
                                   resource=args.resource, dry_run=args.dry_run,
                                   lock_dir=args.lock_dir)
        else:
            report = import_bundle(create_s3_filesystem(), args.archive, dry_run=args.dry_run,
                                   lock_dir=args.lock_dir)
    except KeyboardInterrupt:
        next_step = {
            "export": "rerun export from the beginning; no completed ZIP was published",
            "import": "rerun the same command; committed days will be skipped",
            "inspect": "rerun inspect to verify the archive",
        }[args.mode]
        report, code = {"error": f"interrupted; {next_step}", "mode": args.mode}, 130
    except Exception as exc:  # noqa: BLE001 -- CLI errors must redact credentials
        report = dict(exc.report) if isinstance(exc, TransferError) else {}
        report.update(error=type(exc).__name__, message=_safe_error(exc))
        logging.getLogger(__name__).error("%s", report["message"])
        code = 1
    finally:
        signal.signal(signal.SIGTERM, previous)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
