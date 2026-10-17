"""Resource-oriented Silver CLI. Processing lives in the Silver service."""

from enum import StrEnum
from typing import Annotated

import typer

from procurement.silver.service import date_range, run_resource

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_show_locals=False,
    help="Build Silver candidates from committed Bronze, day by day.",
)


class NoticeFamily(StrEnum):
    standard = "standard"
    vk_adb = "vk_adb"
    reoffer = "reoffer"


def get_storage():
    import s3fs

    from procurement.common.settings import settings

    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": settings.OBJECT_STORAGE_ENDPOINT},
        key=settings.OBJECT_STORAGE_ACCESS_KEY,
        secret=settings.OBJECT_STORAGE_SECRET_KEY,
    )
    return fs, settings.OBJECT_STORAGE_BUCKET


def progress(entry: dict) -> None:
    quality = entry.get("quality", {})
    if entry["status"] == "failed":
        typer.echo(f"{entry['date']} FAILED: {entry['error_type']}: {entry['error']}", err=True)
        return
    counts = ", ".join(f"{name}={count}" for name, count in entry["input_counts"].items())
    typer.echo(
        f"{entry['date']} {entry['status']} | Bronze: {counts} | "
        f"accepted={quality['accepted']} quarantine={quality['quarantined']} "
        f"issues={quality['issues']}"
    )


@app.command("notice")
@app.command("tbmt", hidden=True)
def notice(
    day: Annotated[str | None, typer.Option("--date", help="One source day: YYYY-MM-DD.")] = None,
    start: Annotated[str | None, typer.Option("--start-date", help="Inclusive first day.")] = None,
    end: Annotated[str | None, typer.Option("--end-date", help="Inclusive last day.")] = None,
    family: Annotated[
        NoticeFamily | None, typer.Option(help="Optional: process one family only.")
    ] = None,
    continue_on_error: Annotated[
        bool, typer.Option(help="Continue later days; still exit nonzero on errors.")
    ] = False,
):
    """Process standard, VK/ADB and reoffer automatically for each day."""
    try:
        first, last = date_range(day, start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    fs, bucket = get_storage()
    typer.echo(f"notice | {first} .. {last} | family={family.value if family else 'all'}")
    report = run_resource(
        fs,
        bucket,
        "notice",
        first,
        last,
        family=family.value if family else None,
        continue_on_error=continue_on_error,
        on_progress=progress,
    )
    typer.echo(f"Status: {report['status']}")
    typer.echo(f"Report: s3://{report['output_prefix']}/report.json")
    typer.echo("Output published to Silver Lakehouse tables (s3://procurement-lakehouse/silver/<table>/source_date=YYYY-MM-DD/data.parquet).")
    if report["status"] != "completed":
        raise typer.Exit(code=1)


@app.command("list")
def list_resources():
    """List implemented resources and optional family choices."""
    typer.echo("notice (alias: tbmt): standard, vk_adb, reoffer")


@app.command("reset")
def reset(
    all_silver: Annotated[
        bool,
        typer.Option(
            "--all/--candidates-only",
            help="Reset both published silver tables and silver_candidates.",
        ),
    ] = True,
):
    """Cleanly reset Silver data on Object Storage (candidates and published tables)."""
    fs, bucket = get_storage()
    cleaned = []

    prefixes = [f"{bucket}/silver_candidates"]
    if all_silver:
        prefixes.append(f"{bucket}/silver")

    for prefix in prefixes:
        if fs.exists(prefix):
            all_files = fs.find(prefix)
            if all_files:
                fs.rm(all_files)
            import contextlib

            with contextlib.suppress(Exception):
                fs.rm(prefix, recursive=True)
            cleaned.append(f"s3://{prefix}")

    if cleaned:
        typer.echo("Successfully reset Silver storage:")
        for item in cleaned:
            typer.echo(f"  - Cleaned {item} (all parquet tables, child data, and candidate runs)")
        typer.echo("Silver layer is now 100% completely clean!")
    else:
        typer.echo("Silver storage is already completely clean!")


@app.command("report")
def report(
    resource: Annotated[str, typer.Option(help="Resource to inspect report for.")] = "notice",
):
    """Print the latest run report JSON."""
    import json

    fs, bucket = get_storage()
    prefixes = [
        f"{bucket}/silver/_reports/{resource}",
        f"{bucket}/silver_candidates/muasamcong/{resource}",
        f"{bucket}/silver_candidates/muasamcong/notify_contractor",
    ]
    runs = []
    for p in prefixes:
        if fs.exists(p):
            runs.extend(fs.ls(p))
    if not runs:
        typer.echo(f"No candidate runs found for '{resource}'.")
        return
    runs = sorted(runs)
    latest = runs[-1]
    report_file = f"{latest}/report.json"
    if not fs.exists(report_file):
        typer.echo(f"No report.json found in latest run: {latest}")
        return
    data = json.loads(fs.cat(report_file))
    typer.echo(json.dumps(data, indent=2))


def main():
    app()


if __name__ == "__main__":
    main()
