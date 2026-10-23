"""Transfer committed Bronze data between machines using a portable yearly ZIP."""

import argparse
import json
import logging
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from procurement.common.catalog import SUPPORTED_RESOURCES
from procurement.common.errors import sanitize_error_message
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.storage.object_store import create_s3_filesystem
from procurement.tools.ui import (
    create_progress_display,
    is_rich_enabled,
    render_transfer_summary,
)
from procurement.transfer import (
    TransferError,
    export_bundle,
    import_bundle,
    inspect_bundle,
    year_dates,
)


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Output raw JSON instead of interactive Rich UI")
    commands = parser.add_subparsers(dest="mode", required=True)
    export = commands.add_parser("export", help="Export effective SUCCESS days for one closed year")
    export.add_argument("--year", type=int, required=True)
    export.add_argument("--resource", choices=("all", *SUPPORTED_RESOURCES), default="all")
    export.add_argument("--output", type=Path, required=True)
    inspect = commands.add_parser("inspect", help="Verify a ZIP offline, including Parquet contents")
    inspect.add_argument("--archive", type=Path, required=True)
    restore = commands.add_parser("import", help="Fill missing committed days from a verified ZIP")
    restore.add_argument("--archive", type=Path, nargs="+", required=True, help="One or more ZIP archives to import")
    for command in (export, inspect, restore):
        command.add_argument("--json", action="store_true", help="Output raw JSON")
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


def _make_progress_callback(archive_path: Path, progress: Any):
    arch_name = archive_path.name
    task_id = progress.add_task(
        f"[bold cyan]{arch_name}[/bold cyan]",
        label=arch_name,
        sublabel="validating",
        total=100,
        status="[dim]Inspecting archive...[/dim]",
    )

    def callback(event: str = "day_imported", **kwargs: Any):
        if event == "start":
            year = str(kwargs.get("year", ""))
            planned = kwargs.get("planned", 0)
            skipped = kwargs.get("skipped", 0)
            status_text = (
                f"[yellow]{skipped} skipped[/yellow], [green]{planned} to import[/green]"
                if planned
                else f"[green]All {skipped} days on S3[/green]"
            )
            progress.update(
                task_id,
                label=f"Year {year}",
                sublabel=arch_name,
                total=max(planned, 1),
                completed=0,
                status=status_text,
            )
            if planned == 0:
                progress.update(task_id, completed=1, status="[bold green]Already committed ✔[/bold green]")
        elif event == "day_imported":
            entry = kwargs.get("entry")
            idx = kwargs.get("index", 1)
            total = kwargs.get("total", 1)
            if entry:
                progress.update(
                    task_id,
                    completed=idx,
                    total=total,
                    status=f"[magenta]{entry.resource}[/magenta] {entry.source_date} [dim]({len(entry.objects)} files)[/dim]",
                )
        elif event == "done":
            progress.update(task_id, status="[bold green]Done ✔[/bold green]")

    return callback


def main(argv=None):
    configure_logging()
    parser = _parser()
    args = parser.parse_args(argv)
    use_rich = is_rich_enabled(args)

    if use_rich:
        logging.getLogger("procurement.transfer").setLevel(logging.WARNING)
        logging.getLogger("procurement.storage").setLevel(logging.WARNING)

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
            report = export_bundle(
                create_s3_filesystem(),
                year=args.year,
                output=args.output,
                resource=args.resource,
                dry_run=args.dry_run,
                lock_dir=args.lock_dir,
            )
        else:
            archives = args.archive if isinstance(args.archive, list) else [args.archive]
            if not use_rich:
                if len(archives) == 1:
                    report = import_bundle(
                        create_s3_filesystem(),
                        archives[0],
                        dry_run=args.dry_run,
                        lock_dir=args.lock_dir,
                    )
                else:
                    reports = {}
                    with ThreadPoolExecutor(max_workers=min(len(archives), 4)) as pool:
                        futures = {
                            pool.submit(
                                import_bundle,
                                create_s3_filesystem(),
                                arch,
                                dry_run=args.dry_run,
                                lock_dir=args.lock_dir / arch.stem,
                            ): arch
                            for arch in archives
                        }
                        for fut in as_completed(futures):
                            arch = futures[fut]
                            reports[arch.name] = fut.result()
                    report = {"mode": "import_batch", "archives": reports}
            else:
                progress = create_progress_display()
                with progress:
                    if len(archives) == 1:
                        cb = _make_progress_callback(archives[0], progress)
                        report = import_bundle(
                            create_s3_filesystem(),
                            archives[0],
                            dry_run=args.dry_run,
                            lock_dir=args.lock_dir,
                            on_progress=cb,
                        )
                    else:
                        reports = {}
                        callbacks = {arch: _make_progress_callback(arch, progress) for arch in archives}
                        with ThreadPoolExecutor(max_workers=min(len(archives), 4)) as pool:
                            futures = {
                                pool.submit(
                                    import_bundle,
                                    create_s3_filesystem(),
                                    arch,
                                    dry_run=args.dry_run,
                                    lock_dir=args.lock_dir / arch.stem,
                                    on_progress=callbacks[arch],
                                ): arch
                                for arch in archives
                            }
                            for fut in as_completed(futures):
                                arch = futures[fut]
                                reports[arch.name] = fut.result()
                        report = {"mode": "import_batch", "archives": reports}
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

    if use_rich and args.mode in ("import", "inspect") and code != 130:
        default_arch = args.archive.name if hasattr(args, "archive") and isinstance(args.archive, Path) else None
        render_transfer_summary(report, default_archive=default_arch)
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
