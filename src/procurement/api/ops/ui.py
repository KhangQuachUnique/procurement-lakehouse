from __future__ import annotations

from datetime import date, datetime, timedelta
from html import escape
from typing import Annotated, Any
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse

from procurement.api.ops.dependencies import get_ops_service
from procurement.models.control import RunStatus
from procurement.ops.models import AttemptSummary, DateSummary, ErrorSummary, RunSummary
from procurement.ops.service import DEFAULT_SOURCE, SUPPORTED_RESOURCES, OpsService

router = APIRouter(include_in_schema=False)
Service = Annotated[OpsService, Depends(get_ops_service)]
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_STYLE = """
:root {
  color-scheme: light;
  --bg: #f7f8fa;
  --surface: #fff;
  --text: #172033;
  --muted: #6c778b;
  --line: #e5e9ef;
  --line-strong: #ccd4df;
  --accent: #1769e0;
  --success: #1f9d62;
  --danger: #d94b45;
  --warn: #c99722;
  --neutral: #dfe3e9;
  --cell: 11px;
  --gap: 3px;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.5 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
a { color: inherit; text-decoration: none; }
a:hover { color: var(--accent); }
.topbar { position: sticky; top: 0; z-index: 20; display: flex; align-items: center; justify-content: space-between; min-height: 58px; padding: 0 28px; border-bottom: 1px solid var(--line); background: rgba(255,255,255,.96); backdrop-filter: blur(10px); }
.brand { font-weight: 760; letter-spacing: -.02em; }
.brand span { color: var(--muted); font-weight: 560; margin-left: 7px; }
.nav { display: flex; align-items: center; gap: 4px; }
.nav a { padding: 7px 10px; border-radius: 8px; color: var(--muted); font-weight: 650; }
.nav a.active { background: #eef3fb; color: var(--accent); }
.main { width: min(1180px, calc(100% - 36px)); margin: 0 auto; padding: 32px 0 56px; }
.heading { display: flex; justify-content: space-between; align-items: flex-end; gap: 20px; margin-bottom: 20px; }
h1 { margin: 0; font-size: clamp(25px,3vw,34px); line-height: 1.12; letter-spacing: -.035em; }
h2 { margin: 0 0 12px; font-size: 17px; letter-spacing: -.02em; }
p { margin: 5px 0 0; color: var(--muted); }
.mono { font-family: ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size: .92em; }
.link { color: var(--accent); font-weight: 680; }
.panel { overflow: hidden; border: 1px solid var(--line); border-radius: 12px; background: var(--surface); }
.panel-pad { padding: 16px; }
.section { margin-top: 26px; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
th,td { padding: 12px 14px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; white-space: nowrap; }
th { background: #fafbfc; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .055em; }
tr:last-child td { border-bottom: 0; }
tbody tr:hover { background: #fbfcfe; }
.wrap { min-width: 260px; white-space: normal; }
.badge { display: inline-flex; align-items: center; border-radius: 999px; padding: 3px 8px; font-size: 11px; font-weight: 760; text-transform: uppercase; letter-spacing: .035em; }
.badge.success,.badge.healthy { color: #157347; background: #edf8f2; }
.badge.failed,.badge.partial_failed,.badge.error { color: #b42318; background: #fff0ee; }
.badge.stale,.badge.unknown,.badge.interrupted,.badge.running,.badge.degraded { color: #8a6100; background: #fff6d8; }
.badge.no_attempt,.badge.no_data { color: #657083; background: #f0f2f5; }
.filters { display: flex; flex-wrap: wrap; gap: 9px; align-items: end; }
.field { display: grid; gap: 5px; }
.field label { color: var(--muted); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .045em; }
input,select,button { min-height: 36px; border: 1px solid var(--line-strong); border-radius: 8px; background: #fff; color: var(--text); padding: 7px 9px; font: inherit; }
button { border-color: var(--text); background: var(--text); color: #fff; cursor: pointer; font-weight: 700; padding-inline: 14px; }
.actions { display: flex; flex-wrap: wrap; gap: 8px; }
.actions a { padding: 7px 10px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); color: var(--muted); font-size: 12px; font-weight: 650; }
.stats { display: flex; flex-wrap: wrap; gap: 10px 22px; color: var(--muted); font-size: 13px; }
.stats strong { color: var(--text); }
.kv { display: grid; grid-template-columns: repeat(4,minmax(0,1fr)); margin: 0; }
.kv > div { padding: 15px 16px; border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); }
.kv > div:nth-child(4n) { border-right: 0; }
.kv dt { color: var(--muted); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .045em; }
.kv dd { margin: 5px 0 0; font-weight: 680; overflow-wrap: anywhere; }
.empty { padding: 36px 18px; text-align: center; color: var(--muted); }
.breadcrumbs { display: flex; flex-wrap: wrap; gap: 7px; margin-bottom: 12px; color: var(--muted); font-size: 12px; }
.error-box { padding: 14px 16px; border: 1px solid #ffd2cc; border-radius: 10px; background: #fff0ee; color: #b42318; }
.year-toolbar { display: flex; align-items: end; justify-content: space-between; gap: 14px; flex-wrap: wrap; }
.year-nav { display: flex; align-items: center; gap: 7px; }
.year-nav a { display: grid; place-items: center; width: 36px; height: 36px; border: 1px solid var(--line); border-radius: 8px; background: #fff; color: var(--muted); font-size: 18px; }
.year-label { min-width: 76px; text-align: center; font-size: 17px; font-weight: 760; letter-spacing: -.02em; }
.heatmap-panel { padding: 18px; }
.heatmap-scroll { overflow-x: auto; padding: 4px 3px 12px; scrollbar-width: thin; }
.heatmap { display: grid; grid-template-columns: 28px repeat(var(--weeks), var(--cell)); grid-template-rows: 18px repeat(7, var(--cell)); gap: var(--gap); width: max-content; min-width: 100%; align-items: center; }
.month-label { grid-row: 1; align-self: end; color: var(--muted); font-size: 10px; font-weight: 700; white-space: nowrap; pointer-events: none; }
.weekday-label { grid-column: 1; color: var(--muted); font-size: 9px; line-height: 1; text-align: right; padding-right: 4px; }
.heat-day { width: var(--cell); height: var(--cell); border: 1px solid rgba(23,32,51,.06); border-radius: 2px; background: var(--neutral); outline: none; transition: transform .08s ease, box-shadow .08s ease; }
.heat-day:hover,.heat-day:focus-visible { z-index: 3; transform: scale(1.35); box-shadow: 0 0 0 2px #fff, 0 0 0 3px rgba(23,32,51,.16); }
.heat-day.success { background: var(--success); }
.heat-day.failed { background: var(--danger); }
.heat-day.stale,.heat-day.unknown,.heat-day.interrupted { background: #a879b9; }
.sync-status { padding: 10px 0; color: #64748b; font-size: 13px; }
.heat-day.running { background: var(--warn); }
.heat-day.no_attempt { background: var(--neutral); }
.heat-day.future { opacity: .42; }
.heat-day.today { box-shadow: 0 0 0 1px #fff, 0 0 0 2px var(--accent); }
.heat-legend { display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap; margin-top: 13px; color: var(--muted); font-size: 12px; }
.legend-items { display: flex; gap: 13px; flex-wrap: wrap; }
.legend-item { display: inline-flex; align-items: center; gap: 6px; }
.legend-dot { width: 9px; height: 9px; border-radius: 2px; background: var(--neutral); }
.legend-dot.success { background: var(--success); }
.legend-dot.failed { background: var(--danger); }
.legend-dot.running { background: var(--warn); }
.year-counts { display: flex; gap: 12px; flex-wrap: wrap; }
.year-counts strong { color: var(--text); }
.day-tooltip { position: fixed; z-index: 1000; display: none; max-width: 280px; pointer-events: none; padding: 9px 11px; border: 1px solid rgba(23,32,51,.12); border-radius: 9px; background: #111827; color: #fff; box-shadow: 0 10px 30px rgba(17,24,39,.22); font-size: 12px; line-height: 1.45; white-space: pre-line; }
.day-tooltip.show { display: block; }
@media (max-width: 850px) { .kv { grid-template-columns: repeat(2,minmax(0,1fr)); }.kv > div:nth-child(4n) { border-right: 1px solid var(--line); }.kv > div:nth-child(2n) { border-right: 0; } }
@media (max-width: 620px) { .topbar { padding: 0 14px; }.brand span,.nav .api-docs { display:none; }.main { width: min(100% - 22px,1180px); padding-top: 22px; }.heading { align-items:flex-start; flex-direction:column; }.kv { grid-template-columns:1fr; }.kv > div { border-right:0 !important; }.heatmap-panel { padding: 13px; }:root { --cell: 10px; --gap: 3px; } }
"""

_TOOLTIP_SCRIPT = """
<script>
(() => {
  const tooltip = document.getElementById("day-tooltip");
  if (!tooltip) return;
  const cells = document.querySelectorAll(".heat-day[data-tip]");
  const move = (event) => {
    const pad = 14;
    const rect = tooltip.getBoundingClientRect();
    let left = event.clientX + pad;
    let top = event.clientY + pad;
    if (left + rect.width + 8 > window.innerWidth) left = event.clientX - rect.width - pad;
    if (top + rect.height + 8 > window.innerHeight) top = event.clientY - rect.height - pad;
    tooltip.style.left = Math.max(8, left) + "px";
    tooltip.style.top = Math.max(8, top) + "px";
  };
  cells.forEach((cell) => {
    cell.addEventListener("mouseenter", (event) => {
      tooltip.textContent = cell.dataset.tip || "";
      tooltip.classList.add("show");
      move(event);
    });
    cell.addEventListener("mousemove", move);
    cell.addEventListener("mouseleave", () => tooltip.classList.remove("show"));
  });
})();
</script>
"""


def _text(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.astimezone(VIETNAM_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return str(getattr(value, "value", value))


def _e(value: Any) -> str:
    return escape(_text(value))


def _badge(value: Any) -> str:
    raw = _text(value).lower()
    return f'<span class="badge {escape(raw)}">{escape(raw.replace("_", " "))}</span>'


def _sync_banner(service) -> str:
    state = getattr(service, "sync_status", None)
    if not state:
        return ""
    updated = datetime.fromisoformat(state["last_success_at"])
    age = max(0, int((datetime.now(updated.tzinfo) - updated).total_seconds()))
    warning = " · Sync delayed; showing the last snapshot" if state["last_error"] or age > 30 else ""
    return (
        f'<div class="sync-status" role="status">Updated {_e(updated)} '
        f'({age}s ago){warning}</div>'
    )


def _query(path: str, **params: Any) -> str:
    clean = {key: _text(value) for key, value in params.items() if value not in (None, "")}
    return path if not clean else f"{path}?{urlencode(clean)}"


def _run_url(run_id: str, source: str = DEFAULT_SOURCE) -> str:
    return _query(f"/ops/runs/{quote(run_id, safe='')}", source=source)


def _attempt_url(run_id: str, source_date: date, source: str = DEFAULT_SOURCE) -> str:
    return _query(f"/ops/attempts/{quote(run_id, safe='')}/{source_date.isoformat()}", source=source)


def _date_url(resource: str, source_date: date, source: str = DEFAULT_SOURCE) -> str:
    return _query(f"/ops/calendar/{quote(resource, safe='')}/{source_date.isoformat()}", source=source)


def _breadcrumbs(*items: tuple[str, str | None]) -> str:
    rendered: list[str] = []
    for label, href in items:
        rendered.append(f'<a href="{escape(href, quote=True)}">{escape(label)}</a>' if href else f"<span>{escape(label)}</span>")
    return '<div class="breadcrumbs">' + '<span>/</span>'.join(rendered) + "</div>"


def _layout(title: str, body: str, *, active: str, source: str = DEFAULT_SOURCE, extra_script: str = "") -> HTMLResponse:
    runs_class = "active" if active == "runs" else ""
    calendar_class = "active" if active == "calendar" else ""
    errors_class = "active" if active == "errors" else ""
    html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)} · Procurement Ops</title><style>{_STYLE}</style></head><body><header class="topbar"><a class="brand" href="{escape(_query('/ops', source=source), quote=True)}">Procurement <span>Ops</span></a><nav class="nav"><a class="{runs_class}" href="{escape(_query('/ops', source=source), quote=True)}">Runs</a><a class="{calendar_class}" href="{escape(_query('/ops/calendar', source=source), quote=True)}">Calendar</a><a class="{errors_class}" href="{escape(_query('/ops/errors', source=source), quote=True)}">Errors</a><a class="api-docs" href="/docs">API</a></nav></header><main class="main">{body}</main>{extra_script}</body></html>"""
    return HTMLResponse(html)


def _not_found(title: str, message: str, *, source: str, active: str) -> HTMLResponse:
    response = _layout(title, f'<div class="heading"><div><h1>{escape(title)}</h1><p>{escape(message)}</p></div></div>', active=active, source=source)
    response.status_code = 404
    return response


def _bad_request(exc: ValueError, *, source: str, active: str) -> HTMLResponse:
    response = _layout("Invalid request", f'<div class="heading"><div><h1>Invalid request</h1></div></div><div class="error-box">{escape(str(exc))}</div>', active=active, source=source)
    response.status_code = 400
    return response


def _resource_options(selected: str | None, *, include_all: bool = True) -> str:
    items = ['<option value="">All resources</option>'] if include_all else []
    for resource in SUPPORTED_RESOURCES:
        mark = " selected" if resource == selected else ""
        items.append(f'<option value="{escape(resource, quote=True)}"{mark}>{escape(resource)}</option>')
    return "".join(items)


def _run_row(run: RunSummary, source: str) -> str:
    return f"""<tr><td><a class="link mono" href="{escape(_run_url(run.run_id, source), quote=True)}">{_e(run.run_id)}</a></td><td>{_e(run.resource)}</td><td>{_e(run.start_date)} → {_e(run.end_date)}</td><td>{_badge(run.execution_state or run.status)}</td><td>{run.success_dates}/{run.total_dates}</td><td>{run.failed_dates}</td><td>{_e(run.duration_seconds)}s</td><td>{_e(run.started_at)}</td></tr>"""


def _attempt_row(attempt: AttemptSummary, source: str) -> str:
    return f"""<tr><td><a class="link" href="{escape(_attempt_url(attempt.run_id, attempt.source_date, source), quote=True)}">{_e(attempt.source_date)}</a></td><td>{_badge(attempt.execution_state or attempt.status)}</td><td>{attempt.completed_pages}/{_e(attempt.expected_pages)}</td><td>{attempt.search_items}</td><td>{attempt.bronze_records}</td><td>{attempt.error_count}</td><td>{_e(attempt.duration_seconds)}s</td><td>{_e(attempt.started_at)}</td></tr>"""


def _error_row(error: ErrorSummary, source: str) -> str:
    return f"""<tr><td>{_e(error.occurred_at)}</td><td>{_e(error.resource)}</td><td><a class="link" href="{escape(_date_url(error.resource, error.source_date, source), quote=True)}">{_e(error.source_date)}</a></td><td>{_e(error.stage)}</td><td>{_e(error.error_type)}</td><td>{_e(error.page_number)}</td><td>{_e(error.http_status)}</td><td class="wrap">{_e(error.message)}</td><td><a class="link mono" href="{escape(_run_url(error.run_id, source), quote=True)}">{_e(error.run_id)}</a></td></tr>"""


def _day_tip(item: DateSummary, *, today: date) -> str:
    state = item.status.value
    if item.source_date > today:
        return f"{item.source_date.isoformat()}\nFuture date\nNo attempt yet"
    if state == "success":
        run = item.effective_run_id or item.latest_run_id or "—"
        return f"{item.source_date.isoformat()}\nSUCCESS\n{item.bronze_records} records · {item.attempt_count} attempt(s)\nRun {run}"
    if state == "failed":
        run = item.latest_run_id or "—"
        return f"{item.source_date.isoformat()}\nFAILED\n{item.error_count} error(s) · {item.attempt_count} attempt(s)\nRun {run}"
    if state == "running":
        run = item.latest_run_id or "—"
        return f"{item.source_date.isoformat()}\nRUNNING\n{item.bronze_records} records so far · {item.attempt_count} attempt(s)\nRun {run}"
    if state in {"stale", "unknown", "interrupted"}:
        return (
            f"{item.source_date.isoformat()}\n{state.upper()}\n"
            f"Worker liveness is not confirmed\nRun {item.latest_run_id or '—'}"
        )
    return f"{item.source_date.isoformat()}\nNO ATTEMPT\nNo ingestion attempt was recorded"


def _year_heatmap(items: list[DateSummary], *, resource: str, source: str, year: int, today: date) -> tuple[str, int]:
    by_date = {item.source_date: item for item in items}
    first = date(year, 1, 1)
    last = date(year, 12, 31)
    grid_start = first - timedelta(days=first.weekday())
    grid_end = last + timedelta(days=6 - last.weekday())
    weeks = ((grid_end - grid_start).days // 7) + 1
    parts: list[str] = []
    for label, weekday in (("Mon", 0), ("Wed", 2), ("Fri", 4), ("Sun", 6)):
        parts.append(f'<div class="weekday-label" style="grid-row:{weekday + 2}">{label}</div>')
    seen_months: set[int] = set()
    cursor = first
    while cursor <= last:
        if cursor.month not in seen_months:
            week_index = (cursor - grid_start).days // 7
            parts.append(f'<div class="month-label" style="grid-column:{week_index + 2}">{cursor.strftime("%b")}</div>')
            seen_months.add(cursor.month)
        cursor += timedelta(days=1)
    cursor = first
    while cursor <= last:
        item = by_date[cursor]
        week_index = (cursor - grid_start).days // 7
        weekday = cursor.weekday()
        state = item.status.value
        classes = ["heat-day", state]
        if cursor > today:
            classes.append("future")
        if cursor == today:
            classes.append("today")
        href = _date_url(resource, cursor, source)
        tip = _day_tip(item, today=today)
        parts.append(f'<a class="{" ".join(classes)}" style="grid-column:{week_index + 2};grid-row:{weekday + 2}" href="{escape(href, quote=True)}" data-tip="{escape(tip, quote=True)}" aria-label="{escape(tip.replace(chr(10), ". "), quote=True)}"></a>')
        cursor += timedelta(days=1)
    return "".join(parts), weeks


@router.get("/")
def root() -> RedirectResponse:
    return RedirectResponse("/ops", status_code=307)


@router.get("/ops", response_class=HTMLResponse)
def runs_page(service: Service, source: str = DEFAULT_SOURCE, resource: str | None = None, status: str | None = None, start_date: date | None = None, end_date: date | None = None, limit: Annotated[int, Query(ge=1, le=500)] = 100, offset: Annotated[int, Query(ge=0, le=100000)] = 0) -> HTMLResponse:
    try:
        runs = service.list_runs(source=source, resource=resource, status=status, start_date=start_date, end_date=end_date, limit=limit, offset=offset)
    except ValueError as exc:
        return _bad_request(exc, source=source, active="runs")
    rows = "".join(_run_row(run, source) for run in runs)
    table = f'<div class="panel table-wrap"><table><thead><tr><th>Run ID</th><th>Resource</th><th>Range</th><th>Status</th><th>Success</th><th>Failed</th><th>Duration</th><th>Started</th></tr></thead><tbody>{rows}</tbody></table></div>' if rows else '<div class="panel empty">No runs match these filters.</div>'
    status_options = ['<option value="">All statuses</option>']
    for item in RunStatus:
        mark = " selected" if item.value == status else ""
        status_options.append(f'<option value="{item.value}"{mark}>{item.value}</option>')
    api_href = _query("/api/ops/runs", source=source, resource=resource, status=status, start_date=start_date, end_date=end_date, limit=limit, offset=offset)
    body = f"""<div class="heading"><div><h1>Runs</h1><p>Recent ingestion runs. Start here when debugging operational failures.</p></div><div class="actions"><a href="{escape(api_href, quote=True)}">JSON</a></div></div><div class="panel panel-pad"><form class="filters" method="get"><input type="hidden" name="source" value="{escape(source, quote=True)}"><div class="field"><label>Resource</label><select name="resource">{_resource_options(resource)}</select></div><div class="field"><label>Status</label><select name="status">{''.join(status_options)}</select></div><div class="field"><label>From</label><input type="date" name="start_date" value="{'' if start_date is None else start_date.isoformat()}"></div><div class="field"><label>To</label><input type="date" name="end_date" value="{'' if end_date is None else end_date.isoformat()}"></div><div class="field"><label>Limit</label><input type="number" min="1" max="500" name="limit" value="{limit}"></div><button type="submit">Apply</button></form></div><div class="section">{table}</div>"""
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
    return _layout("Calendar", _sync_banner(service) + body, active="calendar", source=source, extra_script=_TOOLTIP_SCRIPT)


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
