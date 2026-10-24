"""Resource-oriented Silver CLI. Processing lives in the Silver service."""

import sys
from datetime import date
from enum import StrEnum
from typing import Annotated

# Ensure UTF-8 output on Windows consoles to support Rich colors and Unicode characters
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        getattr(sys.stdout, "reconfigure")(encoding="utf-8")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        getattr(sys.stderr, "reconfigure")(encoding="utf-8")
    except Exception:
        pass

import typer
from dotenv import load_dotenv

from procurement.silver.service import (
    date_range,
    merge_run_reports,
    run_resource,
    split_date_range_by_year,
)

load_dotenv()

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


from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console(highlight=False)


def progress(entry: dict) -> None:
    quality = entry.get("quality", {})
    date_str = f"[bold cyan]{entry['date']}[/]"
    if entry["status"] == "failed":
        console.print(f"  {date_str} [bold red]✘ FAILED[/] | [red]{entry['error_type']}: {entry['error']}[/]")
        return
    if entry["status"] == "skipped":
        console.print(f"  {date_str} [dim cyan]↷ SKIPPED[/] | [dim]Already exists in Silver lakehouse[/]")
        return

    counts = " ".join(f"[dim]{name}=[/][bold white]{count}[/]" for name, count in entry["input_counts"].items())
    iceberg_part = ""
    if entry.get("iceberg"):
        staged_count = len(entry["iceberg"])
        iceberg_part = f" | [bold sky_blue2]🧊 Iceberg:[/] [bold cyan]{staged_count} tables[/]"

    status_tag = (
        "[bold green]✔ COMPLETED[/]"
        if entry["status"] == "completed"
        else "[bold yellow]⚠ REVIEW[/]"
    )
    acc = f"[bold green]accepted={quality.get('accepted', 0)}[/]"
    quar = (
        f"[bold yellow]quarantine={quality.get('quarantined', 0)}[/]"
        if quality.get("quarantined")
        else "[dim]quarantine=0[/]"
    )
    issues = (
        f"[bold red]issues={quality.get('issues', 0)}[/]"
        if quality.get("issues")
        else "[dim]issues=0[/]"
    )

    console.print(f"  {date_str} {status_tag} | {counts} | {acc} {quar} {issues}{iceberg_part}")


def display_run_summary(report: dict, resource: str, iceberg: bool) -> None:
    status = report.get("status", "unknown")
    status_style = (
        "bold green"
        if status == "completed"
        else "bold yellow"
        if status == "needs_review"
        else "bold red"
    )
    status_icon = "✔" if status == "completed" else "⚠" if status == "needs_review" else "✘"

    days = report.get("days", [])
    total_days = len(days)
    success_days = sum(1 for d in days if d.get("status") == "completed")
    skipped_days = sum(1 for d in days if d.get("status") == "skipped")
    review_days = sum(1 for d in days if d.get("status") == "needs_review")
    failed_days = sum(1 for d in days if d.get("status") == "failed")
    total_accepted = sum(d.get("quality", {}).get("accepted", 0) for d in days)
    total_quarantined = sum(d.get("quality", {}).get("quarantined", 0) for d in days)

    summary_table = Table.grid(padding=(0, 2))
    summary_table.add_column(style="bold white")
    summary_table.add_column()

    summary_table.add_row("Resource:", f"[bold cyan]{resource.upper()}[/]")
    summary_table.add_row("Status:", f"[{status_style}]{status_icon} {status.upper()}[/]")
    summary_table.add_row(
        "Date Range:",
        f"{report.get('start_date')} ➔ {report.get('end_date')} ({total_days} days)",
    )
    summary_table.add_row(
        "Records:",
        f"[bold green]{total_accepted:,}[/] accepted, [bold yellow]{total_quarantined:,}[/] quarantined",
    )
    breakdown_parts = [f"[green]{success_days} passed[/]"]
    if skipped_days:
        breakdown_parts.append(f"[dim cyan]{skipped_days} skipped[/]")
    breakdown_parts.extend([f"[yellow]{review_days} review[/]", f"[red]{failed_days} failed[/]"])
    summary_table.add_row(
        "Days Breakdown:",
        ", ".join(breakdown_parts),
    )
    if iceberg:
        summary_table.add_row(
            "Iceberg Catalog:", "[bold sky_blue2]lake.silver.*[/] (Format v2 tables & SIMD views)"
        )
    summary_table.add_row("Report:", f"s3://{report.get('output_prefix')}/report.json")

    console.print()
    console.print(
        Panel(
            summary_table,
            title=f"[bold]Lakehouse Silver Pipeline — {resource.upper()}[/]",
            border_style="cyan",
        )
    )


def execute_resource(
    fs,
    bucket: str,
    resource: str,
    start: date,
    end: date,
    *,
    family: str | None = None,
    continue_on_error: bool = False,
    skip_existing: bool = False,
    iceberg: bool = True,
    parallel: bool = True,
    workers: int | None = None,
) -> dict:
    year_chunks = split_date_range_by_year(start, end)

    if len(year_chunks) <= 1 or not parallel:
        return run_resource(
            fs,
            bucket,
            resource,
            start,
            end,
            family=family,
            continue_on_error=continue_on_error,
            skip_existing=skip_existing,
            on_progress=progress,
            iceberg=iceberg,
        )

    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    max_workers = workers or min(len(year_chunks), os.cpu_count() or 4)
    console.print(
        f"[bold cyan]⚡ Multi-year detected:[/] Running [bold green]{len(year_chunks)} years[/] in parallel "
        f"across [bold green]{max_workers}[/] workers "
        f"({', '.join(str(s.year) for s, _ in year_chunks)})"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.fields[name]}[/]"),
        BarColumn(bar_width=25),
        TaskProgressColumn(),
        TextColumn("[white]{task.completed}/{task.total} days[/]"),
        TextColumn("• [dim]acc=[/][bold green]{task.fields[acc]:,}[/]"),
        TextColumn("[dim]quar=[/][bold yellow]{task.fields[quar]:,}[/]"),
        TimeElapsedColumn(),
        console=console,
    ) as progress_ui:
        task_ids = {}
        for s, e in year_chunks:
            total_days = (e - s).days + 1
            t_id = progress_ui.add_task(
                f"{s.year}",
                total=total_days,
                name=f"[{s.year}]",
                acc=0,
                quar=0,
            )
            task_ids[s.year] = t_id

        def make_worker_progress(y: int):
            t_id = task_ids[y]
            acc_total = 0
            quar_total = 0

            def on_chunk_day(entry: dict) -> None:
                nonlocal acc_total, quar_total
                q = entry.get("quality", {})
                acc_total += q.get("accepted", 0)
                quar_total += q.get("quarantined", 0)
                progress_ui.update(t_id, advance=1, acc=acc_total, quar=quar_total)
                if entry.get("status") == "failed":
                    progress_ui.console.print(
                        f"  [bold red]✘ [{y}] {entry.get('date')}: {entry.get('error')}[/]"
                    )

            return on_chunk_day

        def run_chunk(chunk_start: date, chunk_end: date) -> dict:
            return run_resource(
                fs,
                bucket,
                resource,
                chunk_start,
                chunk_end,
                family=family,
                continue_on_error=continue_on_error,
                skip_existing=skip_existing,
                on_progress=make_worker_progress(chunk_start.year),
                iceberg=iceberg,
            )

        chunk_reports = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(run_chunk, s, e): (s, e) for s, e in year_chunks}
            for fut in as_completed(futures):
                rep = fut.result()
                chunk_reports.append(rep)

    return merge_run_reports(sorted(chunk_reports, key=lambda r: r.get("start_date", "")))


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
    skip_existing: Annotated[
        bool,
        typer.Option(
            "--skip-existing",
            help="Skip processing for days where Silver data already exists in storage.",
        ),
    ] = False,
    iceberg: Annotated[
        bool,
        typer.Option(
            "--iceberg/--no-iceberg",
            help="Publish into Apache Iceberg Format v2 tables (REST catalog).",
        ),
    ] = True,
    parallel: Annotated[
        bool,
        typer.Option(
            "--parallel/--no-parallel",
            help="Automatically run multi-year ranges in parallel worker threads.",
        ),
    ] = True,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help="Maximum concurrent workers (defaults to number of years, up to CPU count).",
        ),
    ] = None,
):
    """Process standard, VK/ADB and reoffer automatically for each day."""
    try:
        first, last = date_range(day, start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    fs, bucket = get_storage()
    typer.echo(f"notice | {first} .. {last} | family={family.value if family else 'all'}")
    report = execute_resource(
        fs,
        bucket,
        "notice",
        first,
        last,
        family=family.value if family else None,
        continue_on_error=continue_on_error,
        skip_existing=skip_existing,
        iceberg=iceberg,
        parallel=parallel,
        workers=workers,
    )
    typer.echo(f"Status: {report['status']}")
    typer.echo(f"Report: s3://{report['output_prefix']}/report.json")
    if iceberg:
        typer.echo(
            "Apache Iceberg Format v2 tables updated: lake.silver.* (views: silver.v_latest_*)."
        )
    typer.echo(
        "Output published to Silver Lakehouse tables (s3://procurement-lakehouse/silver/<table>/source_date=YYYY-MM-DD/data.parquet)."
    )
    display_run_summary(report, "notice", iceberg)
    if report["status"] != "completed":
        raise typer.Exit(code=1)


@app.command("project")
@app.command("du_an", hidden=True)
def project(
    day: Annotated[str | None, typer.Option("--date", help="One source day: YYYY-MM-DD.")] = None,
    start: Annotated[str | None, typer.Option("--start-date", help="Inclusive first day.")] = None,
    end: Annotated[str | None, typer.Option("--end-date", help="Inclusive last day.")] = None,
    continue_on_error: Annotated[
        bool, typer.Option(help="Continue later days; still exit nonzero on errors.")
    ] = False,
    skip_existing: Annotated[
        bool,
        typer.Option(
            "--skip-existing",
            help="Skip processing for days where Silver data already exists in storage.",
        ),
    ] = False,
    iceberg: Annotated[
        bool,
        typer.Option(
            "--iceberg/--no-iceberg",
            help="Publish into Apache Iceberg Format v2 tables (REST catalog).",
        ),
    ] = True,
    parallel: Annotated[
        bool,
        typer.Option(
            "--parallel/--no-parallel",
            help="Automatically run multi-year ranges in parallel worker threads.",
        ),
    ] = True,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help="Maximum concurrent workers (defaults to number of years, up to CPU count).",
        ),
    ] = None,
):
    """Process investment project (du_an) automatically for each day."""
    try:
        first, last = date_range(day, start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    fs, bucket = get_storage()
    typer.echo(f"project | {first} .. {last}")
    report = execute_resource(
        fs,
        bucket,
        "project",
        first,
        last,
        family=None,
        continue_on_error=continue_on_error,
        skip_existing=skip_existing,
        iceberg=iceberg,
        parallel=parallel,
        workers=workers,
    )
    typer.echo(f"Status: {report['status']}")
    typer.echo(f"Report: s3://{report['output_prefix']}/report.json")
    if iceberg:
        typer.echo(
            "Apache Iceberg Format v2 tables updated: lake.silver.* (views: silver.v_latest_*)."
        )
    typer.echo(
        "Output published to Silver Lakehouse tables "
        "(s3://procurement-lakehouse/silver/<table>/source_date=YYYY-MM-DD/data.parquet)."
    )
    display_run_summary(report, "project", iceberg)
    if report["status"] != "completed":
        raise typer.Exit(code=1)


@app.command("result")
@app.command("kq_lcnt", hidden=True)
@app.command("contractor_result", hidden=True)
def result(
    day: Annotated[str | None, typer.Option("--date", help="One source day: YYYY-MM-DD.")] = None,
    start: Annotated[str | None, typer.Option("--start-date", help="Inclusive first day.")] = None,
    end: Annotated[str | None, typer.Option("--end-date", help="Inclusive last day.")] = None,
    continue_on_error: Annotated[
        bool, typer.Option(help="Continue later days; still exit nonzero on errors.")
    ] = False,
    skip_existing: Annotated[
        bool,
        typer.Option(
            "--skip-existing",
            help="Skip processing for days where Silver data already exists in storage.",
        ),
    ] = False,
    iceberg: Annotated[
        bool,
        typer.Option(
            "--iceberg/--no-iceberg",
            help="Publish into Apache Iceberg Format v2 tables (REST catalog).",
        ),
    ] = True,
    parallel: Annotated[
        bool,
        typer.Option(
            "--parallel/--no-parallel",
            help="Automatically run multi-year ranges in parallel worker threads.",
        ),
    ] = True,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help="Maximum concurrent workers (defaults to number of years, up to CPU count).",
        ),
    ] = None,
):
    """Process contractor selection result (kq_lcnt) automatically for each day."""
    try:
        first, last = date_range(day, start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    fs, bucket = get_storage()
    typer.echo(f"contractor_result | {first} .. {last}")
    report = execute_resource(
        fs,
        bucket,
        "contractor_result",
        first,
        last,
        family=None,
        continue_on_error=continue_on_error,
        skip_existing=skip_existing,
        iceberg=iceberg,
        parallel=parallel,
        workers=workers,
    )
    typer.echo(f"Status: {report['status']}")
    typer.echo(f"Report: s3://{report['output_prefix']}/report.json")
    if iceberg:
        typer.echo(
            "Apache Iceberg Format v2 tables updated: lake.silver.* (views: silver.v_latest_*)."
        )
    typer.echo(
        "Output published to Silver Lakehouse tables "
        "(s3://procurement-lakehouse/silver/<table>/source_date=YYYY-MM-DD/data.parquet)."
    )
    display_run_summary(report, "result", iceberg)
    if report["status"] != "completed":
        raise typer.Exit(code=1)


class KHLCNTFamily(StrEnum):
    plan = "plan"
    package = "package"


@app.command("khlcnt")
@app.command("plan", hidden=True)
@app.command("package", hidden=True)
def khlcnt(
    day: Annotated[str | None, typer.Option("--date", help="One source day: YYYY-MM-DD.")] = None,
    start: Annotated[str | None, typer.Option("--start-date", help="Inclusive first day.")] = None,
    end: Annotated[str | None, typer.Option("--end-date", help="Inclusive last day.")] = None,
    family: Annotated[
        KHLCNTFamily | None, typer.Option(help="Optional: process plan or package only.")
    ] = None,
    continue_on_error: Annotated[
        bool, typer.Option(help="Continue later days; still exit nonzero on errors.")
    ] = False,
    skip_existing: Annotated[
        bool,
        typer.Option(
            "--skip-existing",
            help="Skip processing for days where Silver data already exists in storage.",
        ),
    ] = False,
    iceberg: Annotated[
        bool,
        typer.Option(
            "--iceberg/--no-iceberg",
            help="Publish into Apache Iceberg Format v2 tables (REST catalog).",
        ),
    ] = True,
    parallel: Annotated[
        bool,
        typer.Option(
            "--parallel/--no-parallel",
            help="Automatically run multi-year ranges in parallel worker threads.",
        ),
    ] = True,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help="Maximum concurrent workers (defaults to number of years, up to CPU count).",
        ),
    ] = None,
):
    """Process procurement plan & package (KHLCNT) automatically for each day."""
    try:
        first, last = date_range(day, start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    fs, bucket = get_storage()
    typer.echo(f"khlcnt | {first} .. {last} | family={family.value if family else 'all'}")
    report = execute_resource(
        fs,
        bucket,
        "khlcnt",
        first,
        last,
        family=family.value if family else None,
        continue_on_error=continue_on_error,
        skip_existing=skip_existing,
        iceberg=iceberg,
        parallel=parallel,
        workers=workers,
    )
    typer.echo(f"Status: {report['status']}")
    typer.echo(f"Report: s3://{report['output_prefix']}/report.json")
    if iceberg:
        typer.echo(
            "Apache Iceberg Format v2 tables updated: lake.silver.* (views: silver.v_latest_*)."
        )
    typer.echo(
        "Output published to Silver Lakehouse tables "
        "(s3://procurement-lakehouse/silver/<table>/source_date=YYYY-MM-DD/data.parquet)."
    )
    display_run_summary(report, "khlcnt", iceberg)
    if report["status"] != "completed":
        raise typer.Exit(code=1)


@app.command("all")
@app.command("run-all", hidden=True)
def all_resources(
    day: Annotated[str | None, typer.Option("--date", help="One source day: YYYY-MM-DD.")] = None,
    start: Annotated[str | None, typer.Option("--start-date", help="Inclusive first day.")] = None,
    end: Annotated[str | None, typer.Option("--end-date", help="Inclusive last day.")] = None,
    continue_on_error: Annotated[
        bool, typer.Option(help="Continue processing next days and resources on errors.")
    ] = True,
    skip_existing: Annotated[
        bool,
        typer.Option(
            "--skip-existing",
            help="Skip processing for days where Silver data already exists in storage.",
        ),
    ] = False,
    iceberg: Annotated[
        bool,
        typer.Option(
            "--iceberg/--no-iceberg",
            help="Publish into Apache Iceberg Format v2 tables (REST catalog).",
        ),
    ] = True,
    parallel: Annotated[
        bool,
        typer.Option(
            "--parallel/--no-parallel",
            help="Automatically run multi-year ranges in parallel worker threads.",
        ),
    ] = True,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help="Maximum concurrent workers (defaults to number of years, up to CPU count).",
        ),
    ] = None,
):
    """Run Silver candidate & Iceberg pipeline across ALL resources (khlcnt, notice, project, result)."""
    try:
        first, last = date_range(day, start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    fs, bucket = get_storage()
    resources = ["khlcnt", "notice", "project", "contractor_result"]
    year_chunks = split_date_range_by_year(first, last)
    is_multi_year = len(year_chunks) > 1 and parallel

    console.print(
        Panel(
            f"[bold]Batch Silver Pipeline Execution[/]\n"
            f"Resources: [bold cyan]{', '.join(resources)}[/]\n"
            f"Date Range: [bold yellow]{first}[/] ➔ [bold yellow]{last}[/]\n"
            f"Execution Mode: [bold green]{'Multi-Year Parallel (' + str(len(year_chunks)) + ' years)' if is_multi_year else 'Sequential'}[/]\n"
            f"Iceberg: [bold sky_blue2]{'Enabled (Format v2)' if iceberg else 'Disabled'}[/]\n"
            f"Skip existing: [white]{skip_existing}[/]\n"
            f"Continue on error: [white]{continue_on_error}[/]",
            title="[bold cyan]Procurement Lakehouse[/]",
            border_style="cyan",
        )
    )

    failed_resources = []
    for res in resources:
        display_name = "result" if res == "contractor_result" else res
        console.rule(f"[bold cyan]▶ Starting {display_name.upper()} ({first} ➔ {last})[/]")
        report = execute_resource(
            fs,
            bucket,
            res,
            first,
            last,
            continue_on_error=continue_on_error,
            skip_existing=skip_existing,
            iceberg=iceberg,
            parallel=parallel,
            workers=workers,
        )
        display_run_summary(report, display_name, iceberg)
        if report.get("status") == "failed":
            failed_resources.append(display_name)
            if not continue_on_error:
                break

    if failed_resources:
        console.rule("[bold red]✘ Execution finished with errors[/]")
        console.print(f"[bold red]Failed resources: {', '.join(failed_resources)}[/]")
        raise typer.Exit(code=1)
    else:
        console.rule("[bold green]✔ All Resources Processed Successfully![/]")


@app.command("list")
def list_resources():
    """List implemented resources and optional family choices."""
    typer.echo("notice (alias: tbmt): standard, vk_adb, reoffer")
    typer.echo("project (alias: du_an): project")
    typer.echo("result (alias: kq_lcnt, contractor_result): contractor_result")
    typer.echo("khlcnt (alias: plan, package): plan, package")


@app.command("reset")
def reset(
    all_silver: Annotated[
        bool,
        typer.Option(
            "--all/--candidates-only",
            help="Reset both published silver tables and silver_candidates.",
        ),
    ] = True,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Bỏ qua xác nhận, xóa ngay không hỏi (dành cho người dũng cảm).",
        ),
    ] = False,
):
    """Cleanly reset Silver data on Object Storage (candidates and published tables)."""
    if not force:
        target_desc = (
            "TOÀN BỘ tầng Silver (cả bảng Parquet lẫn candidate runs cày cuốc bao lâu nay)"
            if all_silver
            else "các bản chạy thử nháp (silver_candidates)"
        )
        console.print()
        console.print(
            Panel(
                f"[bold red]🚨 CẢNH BÁO NGUY CẤP: BẠN ĐANG BẤM NÚT TỰ HỦY! 🚨[/]\n\n"
                f"Thao tác này sẽ [bold red]XÓA SẠCH SÀNH SANH[/] [bold yellow]{target_desc}[/] trên Object Storage!\n"
                f"[italic dim]\"Dữ liệu một đi không trở lại, đừng khóc khi thấy S3 trống trơn...\"[/]\n\n"
                f"[dim]💡 Mẹo nhỏ: Dùng flag [bold cyan]--force[/] hoặc [bold cyan]-f[/] nếu bạn là dân chơi hệ liều![/]",
                title="[bold yellow]⚠️ XÁC NHẬN 'BÚNG TAY' THANOS ⚠️[/]",
                border_style="red",
            )
        )
        confirmed = typer.confirm(
            "👉 Bạn có THẬT SỰ muốn tiễn đống data này về miền cực lạc không?",
            default=False,
        )
        if not confirmed:
            console.print(
                "[bold green]Huề cả làng! 😅 Đã phanh xe trong gang tấc. Dữ liệu vẫn an toàn nguyên vẹn nhé![/]\n"
            )
            raise typer.Exit(code=0)

        console.print("[bold red]Đã nhận lệnh tử thủ! Đang 'búng tay' thanh trừng dữ liệu... 🔥[/]\n")

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
        console.print("[bold green]✔ Đã thanh tẩy thành công![/]")
        for item in cleaned:
            console.print(
                f"  [dim]- Đã dọn sạch:[/] [bold cyan]{item}[/] (toàn bộ parquet, partition và reports)"
            )
        console.print(
            "[bold cyan]Tầng Silver giờ đã sạch bóng kin kít như mới, tha hồ cho bạn cày lại từ đầu! 🧹✨[/]\n"
        )
    else:
        console.print(
            "[bold yellow]Tầng Silver vốn đã sạch bong kin kít rồi, không có gì để xóa cả! 🏖️[/]\n"
        )


@app.command("sync-iceberg")
@app.command("sync", hidden=True)
def sync_iceberg(
    resource: Annotated[
        str | None,
        typer.Option(
            "--resource",
            "-r",
            help="Chỉ đồng bộ một tài nguyên cụ thể (khlcnt, notice, project, result). Mặc định: tất cả.",
        ),
    ] = None,
    table: Annotated[
        str | None,
        typer.Option(
            "--table",
            "-t",
            help="Chỉ đồng bộ một bảng cụ thể (ví dụ notice_revision).",
        ),
    ] = None,
    year: Annotated[
        int | None,
        typer.Option(
            "--year",
            "-y",
            help="Chỉ đồng bộ các phân vùng của một năm cụ thể (ví dụ 2024).",
        ),
    ] = None,
):
    """Đồng bộ hàng loạt các bảng Silver Parquet từ S3 vào Apache Iceberg Format v2."""
    from procurement.common.settings import settings
    from procurement.silver.application.resources import get_silver_resource
    from procurement.silver.application.warehouse import ensure_views
    from procurement.silver.domain.schema import (
        get_primary_key,
        get_silver_schemas,
        metadata,
        to_iceberg_ddl,
    )
    from procurement.storage.iceberg import connect

    fs, bucket = get_storage()
    all_schemas = get_silver_schemas()

    if table:
        target_tables = [table]
    elif resource:
        res_name = "contractor_result" if resource in ("result", "kq_lcnt") else resource
        contract = get_silver_resource(res_name)
        target_tables = list(contract.output_tables)
    else:
        target_tables = [
            "plan_revision",
            "package_revision",
            "package_lot",
            "package_location",
            "notice_revision",
            "notice_lot",
            "notice_location",
            "notice_reoffer_submission",
            "project_revision",
            "project_location",
            "result_revision",
            "award_lot",
            "award_contractor",
            "quarantine",
            "quality_issue",
        ]

    console.print(
        Panel(
            f"[bold]Apache Iceberg Batch Sync[/]\n"
            f"Tables: [bold cyan]{len(target_tables)} bảng[/]\n"
            f"Filter Year: [bold yellow]{year or 'Tất cả các năm'}[/]\n"
            f"Catalog URI: [bold sky_blue2]{settings.ICEBERG_CATALOG_URI}[/]",
            title="[bold cyan]Procurement Lakehouse[/]",
            border_style="cyan",
        )
    )

    try:
        db = connect(settings.iceberg_config())
    except Exception as exc:
        console.print(f"[bold red]✘ Lỗi kết nối Iceberg REST Catalog:[/] {exc}")
        raise typer.Exit(code=1) from exc

    synced_tables = []
    total_synced_rows = 0

    try:
        for tbl_name in target_tables:
            clean_schema = all_schemas.get(tbl_name)
            if not clean_schema:
                console.print(f"  [dim]- Bỏ qua {tbl_name}: không tìm thấy định nghĩa schema.[/]")
                continue

            partition_paths = fs.glob(f"{bucket}/silver/{tbl_name}/source_date=*-*")
            discovered_months = set()
            for p in partition_paths:
                base = p.rsplit("/", 1)[-1]
                if base.startswith("source_date="):
                    ym_str = base[len("source_date=") : len("source_date=") + 7]
                    if len(ym_str) == 7 and ym_str[:4].isdigit() and ym_str[5:].isdigit():
                        if not year or ym_str.startswith(f"{year}-"):
                            discovered_months.add(ym_str)
            months_to_sync = sorted(discovered_months)

            if not months_to_sync:
                console.print(
                    f"  [dim]- {tbl_name:25}: Không tìm thấy phân vùng nào trên S3, bỏ qua.[/]"
                )
                continue

            # 1. Clean sync preparation
            if not year:
                # Full sync: recreate clean Iceberg table directly from S3 Parquet source of truth
                db.execute(f"DROP TABLE IF EXISTS lake.silver.{tbl_name}")
                db.execute(to_iceberg_ddl(tbl_name, clean_schema, "silver"))
            else:
                db.execute(to_iceberg_ddl(tbl_name, clean_schema, "silver"))
                spec = get_partition_spec(tbl_name)
                if spec:
                    import contextlib

                    with contextlib.suppress(Exception):
                        db.execute(
                            f"DELETE FROM lake.silver.{tbl_name} WHERE {spec} >= '{year}-01-01' AND {spec} <= '{year}-12-31'"
                        )

            # 2. Select only exact table columns (exclude Hive partition source_date if not in schema)
            sa_table = metadata.tables.get(f"silver.{tbl_name}")
            if sa_table is not None:
                col_names = [c.name for c in sa_table.columns]
            else:
                col_names = [col.split()[0].strip('", ') for col in clean_schema.split(",")]
            cols_sql = ", ".join(f'"{c}"' for c in col_names)

            tbl_total_rows = 0
            tbl_total_partitions = 0

            # 3. Stream ingest in bounded monthly chunks (~30k - 70k rows per chunk)
            for m in months_to_sync:
                s3_pattern = f"s3://{bucket}/silver/{tbl_name}/source_date={m}-*/*.parquet"
                fs_pattern = f"{bucket}/silver/{tbl_name}/source_date={m}-*/*.parquet"
                files = fs.glob(fs_pattern)
                if not files:
                    continue

                try:
                    db.execute(
                        f"CREATE OR REPLACE TEMP VIEW _cand AS SELECT * FROM read_parquet('{s3_pattern}')"
                    )
                    cand_res = db.execute("SELECT count(*) FROM _cand").fetchone()
                    row_count = cand_res[0] if cand_res else 0
                except Exception as e:
                    console.print(
                        f"  [yellow]⚠ {tbl_name:25} ({m}): Lỗi đọc Parquet từ S3 ({e}), bỏ qua.[/]"
                    )
                    continue

                if row_count == 0:
                    db.execute("DROP VIEW IF EXISTS _cand")
                    continue

                db.execute("BEGIN TRANSACTION")
                try:
                    db.execute(
                        f"INSERT INTO lake.silver.{tbl_name} ({cols_sql}) SELECT {cols_sql} FROM _cand"
                    )
                    db.execute("COMMIT")
                except Exception as exc:
                    db.execute("ROLLBACK")
                    console.print(
                        f"  [bold red]✘ {tbl_name:25} ({m}): Lỗi khi commit vào Iceberg: {exc}[/]"
                    )
                    continue
                finally:
                    db.execute("DROP VIEW IF EXISTS _cand")
                    import contextlib
                    import ctypes

                    with contextlib.suppress(Exception):
                        ctypes.CDLL("libc.so.6").malloc_trim(0)

                tbl_total_rows += row_count
                tbl_total_partitions += len(files)

            if tbl_total_rows > 0:
                console.print(
                    f"  [bold green]✔[/] [bold white]{tbl_name:25}[/]: [bold cyan]{tbl_total_rows:,}[/] dòng ({tbl_total_partitions} phân vùng) ➔ Iceberg"
                )
                synced_tables.append(tbl_name)
                total_synced_rows += tbl_total_rows

        # 4. Refresh deduplication views
        console.print(
            "\n[bold sky_blue2]Đang cập nhật các Views SIMD khử trùng (silver.v_latest_*)...[/]"
        )
        try:
            created_views = ensure_views(db, "silver")
            for v in created_views:
                console.print(f"  [bold green]✔[/] [dim]View:[/] [bold cyan]{v}[/]")
        except Exception as exc:
            console.print(f"  [yellow]⚠ Lỗi tạo view: {exc}[/]")
    finally:
        db.close()
        import contextlib
        import ctypes

        with contextlib.suppress(Exception):
            ctypes.CDLL("libc.so.6").malloc_trim(0)

    console.print(
        Panel(
            f"[bold green]✔ Đồng bộ Iceberg hoàn tất![/]\n"
            f"Số bảng đã nạp: [bold cyan]{len(synced_tables)} bảng[/]\n"
            f"Tổng số bản ghi: [bold green]{total_synced_rows:,} rows[/]\n"
            f"Catalog: [bold sky_blue2]lake.silver.*[/] (Đã sẵn sàng cho DuckDB / Trino / Dagster)",
            title="[bold green]Hoàn Tất Đồng Bộ[/]",
            border_style="green",
        )
    )



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
    raw = fs.cat(report_file)
    data = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    typer.echo(json.dumps(data, indent=2))


@app.command("inspect")
@app.command("info", hidden=True)
def inspect(
    table: Annotated[
        str | None, typer.Option("--table", "-t", help="Specific table to inspect.")
    ] = None,
):
    """Inspect Apache Iceberg tables and latest deduplicated views."""
    from procurement.common.settings import settings
    from procurement.silver.application.warehouse import VIEW_DEFINITIONS, ensure_views
    from procurement.storage.iceberg import RestCatalog, connect

    iceberg_cfg = settings.iceberg_config()
    catalog = RestCatalog(iceberg_cfg)

    try:
        db = connect(iceberg_cfg)
    except Exception as exc:
        typer.echo(f"Error connecting to Iceberg catalog: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        ensure_views(db, namespace="silver")
        typer.echo(f"\nIceberg Catalog: {iceberg_cfg.catalog_uri} | Warehouse: {iceberg_cfg.warehouse}")
        typer.echo(
            f"{'Table / View':<30} | {'Type':<12} | {'Format':<10} | {'Rows':<10} | {'Snapshot ID':<20}"
        )
        typer.echo("-" * 90)

        for view_name, cfg in VIEW_DEFINITIONS.items():
            src_table = cfg["source_table"]
            if table and table not in (view_name, src_table):
                continue

            # Check Iceberg base table
            try:
                table_info = catalog.table("silver", src_table)
                metadata = table_info.get("metadata", {}) if table_info else {}
                fmt = f"v{metadata.get('format-version', '?')}"
                snap_id = str(metadata.get("current-snapshot-id", "none"))
                row_res = db.sql(f"SELECT count(*) FROM lake.silver.{src_table}").fetchone()
                row_count = row_res[0] if row_res else 0
                typer.echo(
                    f"lake.silver.{src_table:<18} | Table (Raw)  | {fmt:<10} | {row_count:<10} | {snap_id:<20}"
                )
            except Exception:
                typer.echo(f"lake.silver.{src_table:<18} | Table (Raw)  | not created| 0          | none")

            # Check vectorized view
            try:
                view_res = db.sql(f"SELECT count(*) FROM silver.{view_name}").fetchone()
                view_count = view_res[0] if view_res else 0
                typer.echo(
                    f"silver.{view_name:<23} | View(Latest) | SIMD View  | {view_count:<10} | -"
                )
            except Exception:
                typer.echo(f"silver.{view_name:<23} | View(Latest) | -          | 0          | -")

            typer.echo("-" * 90)
    finally:
        db.close()


@app.command("ui")
def ui(
    port: Annotated[int, typer.Option("--port", "-p", help="Port to bind DuckDB Web UI.")] = 4213,
    browser: Annotated[
        bool, typer.Option("--browser/--no-browser", help="Open browser automatically.")
    ] = True,
):
    """Launch local DuckDB Web SQL UI with Iceberg tables & latest views attached."""
    import time

    from procurement.common.settings import settings
    from procurement.silver.application.warehouse import ensure_views
    from procurement.storage.iceberg import connect

    iceberg_cfg = settings.iceberg_config()
    typer.echo(f"Connecting to Apache Iceberg REST Catalog at {iceberg_cfg.catalog_uri}...")
    try:
        db = connect(iceberg_cfg)
    except Exception as exc:
        typer.echo(
            f"Error: Could not connect to SeaweedFS Iceberg REST catalog at {iceberg_cfg.catalog_uri} ({exc}).\n"
            f"Please ensure SeaweedFS is running: docker compose up -d object-storage",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    try:
        # 1. Ensure lake.silver schema exists
        db.execute("CREATE SCHEMA IF NOT EXISTS lake.silver")

        # 2. Mount vectorized latest views
        views = ensure_views(db, namespace="silver")

        # 3. Load DuckDB UI extension
        db.execute("INSTALL ui")
        db.execute("LOAD ui")
        db.execute(f"SET ui_local_port = {port}")
        db.execute("CALL start_ui()")

        url = f"http://localhost:{port}"
        typer.echo("=" * 65)
        typer.echo("🦆 DuckDB Web SQL UI is running!")
        typer.echo(f"🌐 URL: {url}")
        typer.echo("")
        typer.echo("📦 Catalogs & Schemas mounted:")
        typer.echo("  - lake.silver.*          (Historical Apache Iceberg Format v2 tables)")
        typer.echo(f"  - silver.*               (Vectorized latest views: {', '.join(views)})")
        typer.echo("")
        typer.echo("💡 Tips:")
        typer.echo("  - SELECT * FROM silver.v_latest_notice LIMIT 10;")
        typer.echo("  - SELECT * FROM silver.v_latest_plan LIMIT 10;")
        typer.echo("  - SELECT * FROM silver.v_latest_package LIMIT 10;")
        typer.echo("")
        typer.echo("Press Ctrl+C to stop.")
        typer.echo("=" * 65)

        if browser:
            import webbrowser

            webbrowser.open(url)

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        typer.echo("\nStopping DuckDB Web UI...")
    finally:
        try:
            db.execute("CALL stop_ui_server()")
        except Exception:
            pass
        db.close()


def main():
    app()


if __name__ == "__main__":
    main()
