from __future__ import annotations

from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Annotated
from urllib.parse import quote
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from procurement.api.ops.dependencies import get_ops_service
from procurement.api.ops.views import (
    _attempt_row,
    _bad_request,
    _badge,
    _breadcrumbs,
    _date_url,
    _e,
    _error_row,
    _layout,
    _not_found,
    _query,
    _resource_options,
    _run_row,
    _run_url,
    _sync_banner,
    _year_heatmap,
)
from procurement.models.control import RunStatus
from procurement.ops.service import DEFAULT_SOURCE, OpsService

router = APIRouter(include_in_schema=False)

@router.get("/ops/static/{name}")
def static_asset(name: str):
    if name not in {"ops.css", "tooltip.js", "calendar.js"}:
        raise HTTPException(404)
    return FileResponse(Path(__file__).parent / "static" / name)

Service = Annotated[OpsService, Depends(get_ops_service)]
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")



_TOOLTIP_SCRIPT = '<script src="/ops/static/tooltip.js" defer></script>'

_CALENDAR_REFRESH_SCRIPT = '<script src="/ops/static/calendar.js" defer></script>'





@router.get("/")
def root() -> RedirectResponse:
    return RedirectResponse("/ops/overview", status_code=307)


@router.get("/ops", response_class=HTMLResponse)
def runs_page(service: Service, source: str = DEFAULT_SOURCE, resource: str | None = None, status: str | None = None, start_date: date | None = None, end_date: date | None = None, limit: Annotated[int, Query(ge=1, le=500)] = 100, offset: Annotated[int, Query(ge=0, le=100000)] = 0) -> HTMLResponse:
    try:
        runs = service.list_runs(source=source, resource=resource, status=status, start_date=start_date, end_date=end_date, limit=limit, offset=offset)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="runs")
    from procurement.watcher import read_status
    try:
        watch = read_status()
        watch_summary = " | ".join(f"{_e(key)}: {_e(value)}" for key, value in watch.items())
    except Exception:  # noqa: BLE001 -- optional local status must not break Ops
        watch_summary = "Watch status unavailable; inspect the watch database namespace."
    rows = "".join(_run_row(run, source) for run in runs)
    table = f'<div class="panel table-wrap"><table><thead><tr><th>Run ID</th><th>Resource</th><th>Range</th><th>Status</th><th>Success</th><th>Failed</th><th>Duration</th><th>Started</th></tr></thead><tbody>{rows}</tbody></table></div>' if rows else '<div class="panel empty">No runs match these filters.</div>'
    status_options = ['<option value="">All statuses</option>']
    for item in RunStatus:
        mark = " selected" if item.value == status else ""
        status_options.append(f'<option value="{item.value}"{mark}>{item.value}</option>')
    api_href = _query("/api/ops/runs", source=source, resource=resource, status=status, start_date=start_date, end_date=end_date, limit=limit, offset=offset)
    body = f"""<div class="heading"><div><h1>Runs</h1><p>Recent ingestion runs. Start here when debugging operational failures.</p></div><div class="actions"><a href="{escape(api_href, quote=True)}">JSON</a></div></div><div class="panel panel-pad"><form class="filters" method="get"><input type="hidden" name="source" value="{escape(source, quote=True)}"><div class="field"><label>Resource</label><select name="resource">{_resource_options(resource)}</select></div><div class="field"><label>Status</label><select name="status">{''.join(status_options)}</select></div><div class="field"><label>From</label><input type="date" name="start_date" value="{'' if start_date is None else start_date.isoformat()}"></div><div class="field"><label>To</label><input type="date" name="end_date" value="{'' if end_date is None else end_date.isoformat()}"></div><div class="field"><label>Limit</label><input type="number" min="1" max="500" name="limit" value="{limit}"></div><button type="submit">Apply</button></form></div><div class="section">{table}</div>"""
    body += f'<div class="panel panel-pad"><h2>Bid opening watch</h2><p>{watch_summary}</p><a href="/api/ops/bid-opening-watch">JSON</a></div>'
    links = []
    for label, position in (("Previous", max(0, offset - limit)), ("Next", offset + limit)):
        if (label == "Previous" and offset == 0) or (label == "Next" and len(runs) < limit):
            continue
        href = _query("/ops", source=source, resource=resource, status=status,
                      start_date=start_date, end_date=end_date, limit=limit, offset=position)
        links.append(f'<a href="{escape(href, quote=True)}">{label}</a>')
    body += '<nav class="actions" aria-label="Run pages">' + " ".join(links) + "</nav>"
    return _layout("Runs", _sync_banner(service) + body, active="runs", source=source)


@router.get("/ops/calendar", response_class=HTMLResponse)
def calendar_page(service: Service, source: str = DEFAULT_SOURCE, resource: str = "notify_contractor", year: int | None = None) -> HTMLResponse:
    today = datetime.now(VIETNAM_TZ).date()
    year = year or today.year
    if year < 2000 or year > 2100:
        return _bad_request(ValueError("year must be between 2000 and 2100"), source=source, active="calendar")
    try:
        service.identity(resource, source=source)
        dates = service.list_dates(resource, source=source, start_date=date(year, 1, 1), end_date=date(year, 12, 31))
    except ValueError as exc:
        return _bad_request(exc, source=source, active="calendar")
    heatmap, weeks = _year_heatmap(dates, resource=resource, source=source, year=year, today=today)
    counts = {
        "success": sum(1 for item in dates if item.status.value == "success"),
        "failed": sum(1 for item in dates if item.status.value == "failed"),
        "running": sum(1 for item in dates if item.status.value == "running"),
        "unconfirmed": sum(1 for item in dates if item.status.value in {"stale", "unknown", "interrupted"}),
        "no_attempt": sum(1 for item in dates if item.status.value == "no_attempt" and item.source_date <= today),
    }
    prev_href = _query("/ops/calendar", source=source, resource=resource, year=year - 1)
    next_href = _query("/ops/calendar", source=source, resource=resource, year=year + 1)
    body = f"""<div class="heading"><div><h1>Calendar</h1><p>Yearly ingestion coverage. Hover a day for details; click it to inspect attempts.</p></div></div><div class="panel panel-pad"><div class="year-toolbar"><form class="filters" method="get"><input type="hidden" name="source" value="{escape(source, quote=True)}"><input type="hidden" name="year" value="{year}"><div class="field"><label>Resource</label><select name="resource">{_resource_options(resource, include_all=False)}</select></div><button type="submit">Apply</button></form><div class="year-nav"><a href="{escape(prev_href, quote=True)}" aria-label="Previous year">←</a><div class="year-label">{year}</div><a href="{escape(next_href, quote=True)}" aria-label="Next year">→</a></div></div></div><div class="section panel heatmap-panel"><div class="heatmap-scroll"><div class="heatmap" style="--weeks:{weeks}">{heatmap}</div></div><div class="heat-legend"><div class="legend-items"><span class="legend-item"><i class="legend-dot success"></i>Success</span><span class="legend-item"><i class="legend-dot failed"></i>Failed</span><span class="legend-item"><i class="legend-dot running"></i>Running</span><span class="legend-item"><i class="legend-dot"></i>No attempt</span></div><div class="year-counts"><span><strong>{counts["success"]}</strong> success</span><span><strong>{counts["failed"]}</strong> failed</span><span><strong>{counts["running"]}</strong> running</span><span><strong>{counts["unconfirmed"]}</strong> unconfirmed</span><span><strong>{counts["no_attempt"]}</strong> not run</span></div></div></div><div id="day-tooltip" class="day-tooltip" role="tooltip"></div>"""
    return _layout("Calendar", _sync_banner(service) + body, active="calendar", source=source,
                   extra_script=_TOOLTIP_SCRIPT + _CALENDAR_REFRESH_SCRIPT)


@router.get("/ops/calendar/{resource}/{source_date}", response_class=HTMLResponse)
def calendar_date_page(resource: str, source_date: date, service: Service, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    try:
        detail = service.get_date(resource, source_date, source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="calendar")
    attempts = sorted(detail.attempts, key=lambda item: item.started_at, reverse=True)
    rows = "".join(_attempt_row(item, source) for item in attempts)
    content = f'<div class="panel table-wrap"><table><thead><tr><th>Date</th><th>Status</th><th>Pages</th><th>Search</th><th>Bronze</th><th>Errors</th><th>Duration</th><th>Started</th></tr></thead><tbody>{rows}</tbody></table></div>' if rows else '<div class="panel empty">No ingestion attempt was recorded for this date.</div>'
    calendar_href = _query("/ops/calendar", source=source, resource=resource, year=source_date.year)
    body = f"""{_breadcrumbs(("Calendar", calendar_href), (resource, calendar_href), (source_date.isoformat(), None))}<div class="heading"><div><h1>{source_date.isoformat()}</h1><p>{escape(resource)}</p></div></div><div class="stats"><span>state <strong>{_badge(detail.status)}</strong></span><span>attempts <strong>{len(attempts)}</strong></span><span>effective run <strong class="mono">{_e(detail.effective_run_id)}</strong></span></div><div class="section">{content}</div>"""
    return _layout(str(source_date), _sync_banner(service) + body, active="calendar", source=source)


@router.get("/ops/resources/{resource}")
def legacy_resource_page(resource: str, source: str = DEFAULT_SOURCE) -> RedirectResponse:
    return RedirectResponse(_query("/ops/calendar", source=source, resource=resource), status_code=307)


@router.get("/ops/resources/{resource}/dates/{source_date}")
def legacy_date_page(resource: str, source_date: date, source: str = DEFAULT_SOURCE) -> RedirectResponse:
    return RedirectResponse(_date_url(resource, source_date, source), status_code=307)


@router.get("/ops/runs/{run_id}", response_class=HTMLResponse)
def run_page(run_id: str, service: Service, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    try:
        detail = service.get_run(run_id, source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="runs")
    if detail is None:
        return _not_found("Run not found", f"No run exists with id {run_id}.", source=source, active="runs")
    run = detail.run
    attempts = sorted(detail.attempts, key=lambda item: item.source_date, reverse=True)
    bronze = sum(item.bronze_records for item in attempts)
    errors = sum(item.error_count for item in attempts)
    rows = "".join(_attempt_row(item, source) for item in attempts)
    attempts_content = f'<div class="panel table-wrap"><table><thead><tr><th>Date</th><th>Status</th><th>Pages</th><th>Search</th><th>Bronze</th><th>Errors</th><th>Duration</th><th>Started</th></tr></thead><tbody>{rows}</tbody></table></div>' if rows else '<div class="panel empty">This run has no day attempts.</div>'
    api_href = _query(f"/api/ops/runs/{quote(run_id, safe='')}", source=source)
    body = f"""{_breadcrumbs(("Runs", _query('/ops', source=source)), (run_id, None))}<div class="heading"><div><h1 class="mono">{_e(run_id)}</h1><p>{_e(run.resource)} · {_e(run.start_date)} → {_e(run.end_date)}</p></div><div class="actions"><a href="{escape(api_href, quote=True)}">JSON</a></div></div><div class="panel"><dl class="kv"><div><dt>Status</dt><dd>{_badge(run.execution_state or run.status)}</dd></div><div><dt>Dates</dt><dd>{run.success_dates} success / {run.failed_dates} failed / {run.total_dates}</dd></div><div><dt>Bronze records</dt><dd>{bronze}</dd></div><div><dt>Errors</dt><dd>{errors}</dd></div><div><dt>Duration</dt><dd>{_e(run.duration_seconds)}s</dd></div><div><dt>Started</dt><dd>{_e(run.started_at)}</dd></div><div><dt>Completed</dt><dd>{_e(run.completed_at)}</dd></div><div><dt>Resource</dt><dd>{_e(run.resource)}</dd></div></dl></div><div class="section"><h2>Date attempts</h2>{attempts_content}</div>"""
    return _layout(f"Run {run_id}", _sync_banner(service) + body, active="runs", source=source)


@router.get("/ops/attempts/{run_id}/{source_date}", response_class=HTMLResponse)
def attempt_page(run_id: str, source_date: date, service: Service, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    try:
        detail = service.get_attempt(run_id, source_date, source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="runs")
    if detail is None:
        return _not_found("Attempt not found", f"No attempt exists for {run_id} on {source_date}.", source=source, active="runs")
    attempt = detail.attempt
    error_rows = "".join(_error_row(item, source) for item in detail.errors)
    errors_content = f'<div class="panel table-wrap"><table><thead><tr><th>Occurred</th><th>Resource</th><th>Date</th><th>Stage</th><th>Type</th><th>Page</th><th>HTTP</th><th>Message</th><th>Run</th></tr></thead><tbody>{error_rows}</tbody></table></div>' if error_rows else '<div class="panel empty">No errors recorded for this attempt.</div>'
    page_rows = "".join(f'<tr><td>{item.page_number}</td><td>{_badge(item.status)}</td><td>{item.page_size}</td><td>{item.search_items}</td><td>{item.bronze_records}</td><td>{item.error_count}</td><td>{_e(item.duration_seconds)}s</td></tr>' for item in detail.pages)
    pages_content = f'<div class="panel table-wrap"><table><thead><tr><th>Page</th><th>Status</th><th>Size</th><th>Search</th><th>Bronze</th><th>Errors</th><th>Duration</th></tr></thead><tbody>{page_rows}</tbody></table></div>' if page_rows else '<div class="panel empty">No page manifests found.</div>'
    api_href = _query(f"/api/ops/attempts/{quote(run_id, safe='')}/{source_date.isoformat()}", source=source)
    body = f"""{_breadcrumbs(("Runs", _query('/ops', source=source)), (run_id, _run_url(run_id, source)), (source_date.isoformat(), None))}<div class="heading"><div><h1>Attempt · {source_date.isoformat()}</h1><p class="mono">{_e(run_id)}</p></div><div class="actions"><a href="{escape(_run_url(run_id, source), quote=True)}">Run</a><a href="{escape(api_href, quote=True)}">JSON</a></div></div><div class="panel"><dl class="kv"><div><dt>Status</dt><dd>{_badge(attempt.execution_state or attempt.status)}</dd></div><div><dt>Pages</dt><dd>{attempt.completed_pages}/{_e(attempt.expected_pages)}</dd></div><div><dt>Search items</dt><dd>{attempt.search_items}</dd></div><div><dt>Bronze records</dt><dd>{attempt.bronze_records}</dd></div><div><dt>Errors</dt><dd>{attempt.error_count}</dd></div><div><dt>Duration</dt><dd>{_e(attempt.duration_seconds)}s</dd></div><div><dt>Started</dt><dd>{_e(attempt.started_at)}</dd></div><div><dt>Completed</dt><dd>{_e(attempt.completed_at)}</dd></div></dl></div><div class="section"><h2>Errors</h2>{errors_content}</div><div class="section"><h2>Pages</h2>{pages_content}</div>"""
    return _layout(f"Attempt {run_id}", _sync_banner(service) + body, active="runs", source=source)


@router.get("/ops/errors", response_class=HTMLResponse)
def errors_page(service: Service, source: str = DEFAULT_SOURCE, resource: str | None = None, source_date: date | None = None, run_id: str | None = None, stage: str | None = None, error_type: str | None = None, limit: Annotated[int, Query(ge=1, le=1000)] = 200) -> HTMLResponse:
    try:
        errors = service.list_errors(source=source, resource=resource, source_date=source_date, run_id=run_id, stage=stage, error_type=error_type, limit=limit)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="errors")
    rows = "".join(_error_row(item, source) for item in errors)
    table = f'<div class="panel table-wrap"><table><thead><tr><th>Occurred</th><th>Resource</th><th>Date</th><th>Stage</th><th>Type</th><th>Page</th><th>HTTP</th><th>Message</th><th>Run</th></tr></thead><tbody>{rows}</tbody></table></div>' if rows else '<div class="panel empty">No errors match these filters.</div>'
    body = f"""<div class="heading"><div><h1>Errors</h1><p>Actual ingestion errors only. A date with no attempt is not listed here.</p></div></div><div class="panel panel-pad"><form class="filters" method="get"><input type="hidden" name="source" value="{escape(source, quote=True)}"><div class="field"><label>Resource</label><select name="resource">{_resource_options(resource)}</select></div><div class="field"><label>Date</label><input type="date" name="source_date" value="{'' if source_date is None else source_date.isoformat()}"></div><div class="field"><label>Run ID</label><input name="run_id" value="{escape(run_id or '', quote=True)}"></div><div class="field"><label>Stage</label><input name="stage" value="{escape(stage or '', quote=True)}"></div><div class="field"><label>Error type</label><input name="error_type" value="{escape(error_type or '', quote=True)}"></div><button type="submit">Filter</button></form></div><div class="section">{table}</div>"""
    return _layout("Errors", _sync_banner(service) + body, active="errors", source=source)


@router.get("/ops/overview", response_class=HTMLResponse)
def overview_page(service: Service, source: str = DEFAULT_SOURCE):
    try:
        overview = service.overview(source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="overview")
    rows = "".join(
        f'<tr><td><a href="{escape(_query("/ops/calendar", source=source, resource=item.resource), quote=True)}">{_e(item.resource)}</a></td>'
        f'<td>{_badge(item.health)}</td><td>{_e(item.latest_success_source_date)}</td>'
        f'<td>{_e(item.freshness_days)}</td><td>{item.unresolved_failed_dates}</td><td>{item.total_attempts}</td></tr>'
        for item in overview.resources
    )
    body = '<h1>Data health</h1><p>Coverage and freshness from committed Bronze manifests.</p>'
    body += '<div class="panel table-wrap"><table><thead><tr><th>Resource</th><th>Health</th><th>Latest success</th><th>Age (days)</th><th>Unresolved failed dates</th><th>Attempts</th></tr></thead><tbody>' + rows + '</tbody></table></div>'
    return _layout("Data health", _sync_banner(service) + body, active="overview", source=source)
