"""CLI for verified Bronze ZIP imports and exports."""

import json
import logging
from pathlib import Path
from typing import Annotated

import s3fs
import typer

from procurement.common.catalog import SUPPORTED_RESOURCES
from procurement.common.settings import settings
from procurement.transfer import TransferError, export_bundle, import_bundle, inspect_bundle

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _storage():
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": settings.OBJECT_STORAGE_ENDPOINT},
        key=settings.OBJECT_STORAGE_ACCESS_KEY,
        secret=settings.OBJECT_STORAGE_SECRET_KEY,
    )


def _print_report(report: dict) -> None:
    fields = (
        "mode", "year", "archive", "dry_run", "resources", "verified",
        "selected_days", "planned_days", "imported_days", "skipped_days",
        "blocked_days", "conflicts", "objects", "uploaded_objects",
        "reused_objects", "destination_verified", "coverage_complete", "missing_days",
    )
    summary = {}
    for field in fields:
        if field in report:
            value = report[field]
            summary[field] = len(value) if isinstance(value, list) else value
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


def _progress(*, event: str, **details) -> None:
    if event == "start":
        typer.echo(
            f"Ready: {details['planned']} resource-days to import; "
            f"{details['skipped']} already present."
        )
    elif event == "day_imported":
        index, total = details["index"], details["total"]
        if index == 1 or index % 50 == 0 or index == total:
            typer.echo(f"Imported {index}/{total} resource-days", err=True)


def _run(action) -> None:
    try:
        _print_report(action())
    except TransferError as exc:
        _print_report(exc.report)
        typer.echo(f"Transfer failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    except (OSError, ValueError) as exc:
        typer.echo(f"Transfer failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("import")
def import_archive(
    archive: Annotated[Path, typer.Option("--archive", exists=True, dir_okay=False, readable=True)],
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    workers: Annotated[int, typer.Option("--workers", min=1)] = 4,
) -> None:
    """Validate and import committed Bronze days from a ZIP bundle."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    typer.echo(f"Validating archive: {archive} (including checksums)...")
    _run(lambda: import_bundle(
        _storage(), archive, dry_run=dry_run, workers=workers, on_progress=_progress
    ))


@app.command("inspect")
def inspect_archive(
    archive: Annotated[Path, typer.Option("--archive", exists=True, dir_okay=False, readable=True)],
    workers: Annotated[int, typer.Option("--workers", min=1)] = 4,
) -> None:
    """Verify ZIP integrity without connecting to S3."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _run(lambda: inspect_bundle(archive, workers=workers))


@app.command("export")
def export_archive(
    year: Annotated[int, typer.Option("--year", min=2000)],
    output: Annotated[Path, typer.Option("--output")],
    resource: Annotated[str, typer.Option("--resource")] = "all",
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """Export committed Bronze data for a closed calendar year to a ZIP bundle."""
    if resource != "all" and resource not in SUPPORTED_RESOURCES:
        raise typer.BadParameter("Unknown resource", param_hint="--resource")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _run(lambda: export_bundle(
        _storage(), year=year, output=output, resource=resource, dry_run=dry_run
    ))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
