from __future__ import annotations

from datetime import date, datetime
from html import escape
from typing import Annotated, Any
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse

from procurement.api.ops.dependencies import get_ops_service
from procurement.ops.models import AttemptSummary, ErrorSummary
from procurement.ops.service import DEFAULT_SOURCE, OpsService

router = APIRouter(include_in_schema=False)
Service = Annotated[OpsService, Depends(get_ops_service)]


_STYLE = """
:root {
  color-scheme: light;
  --bg: #f6f7f9;
  --surface: #ffffff;
  --surface-2: #f1f3f5;
  --text: #18212f;
  --muted: #687386;
  --line: #e3e7ed;
  --accent: #1769e0;
  --success: #117a4b;
  --success-bg: #eaf8f1;
  --warn: #946200;
  --warn-bg: #fff6d8;
  --danger: #b42318;
  --danger-bg: #fff0ee;
  --neutral: #596579;
  --neutral-bg: #eef1f5;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font: 14px/1.5 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
a { color: inherit; text-decoration: none; }
a:hover { color: var(--accent); }
.shell { min-height: 100vh; }
.topbar {
  position: sticky;
  top: 0;
  z-index: 20;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  min-height: 58px;
  padding: 0 28px;
  border-bottom: 1px solid var(--line);
  background: rgba(255,255,255,.94);
  backdrop-filter: blur(12px);
}
.brand { font-weight: 720; letter-spacing: -.01em; }
.brand small { margin-left: 8px; color: var(--muted); font-weight: 550; }
.nav { display: flex; gap: 8px; }
.nav a { padding: 7px 10px; border-radius: 8px; color: var(--muted); font-weight: 600; }
.nav a:hover { background: var(--surface-2); color: var(--text); }
.main { width: min(1180px, calc(100% - 36px)); margin: 0 auto; padding: 34px 0 56px; }
.heading { display: flex; align-items: flex-end; justify-content: space-between; gap: 20px; margin-bottom: 22px; }
h1 { margin: 0; font-size: clamp(25px, 3vw, 34px); line-height: 1.15; letter-spacing: -.035em; }
h2 { margin: 0 0 14px; font-size: 18px; letter-spacing: -.02em; }
p { margin: 5px 0 0; color: var(--muted); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .92em; }
.muted { color: var(--muted); }
.link { color: var(--accent); font-weight: 650; }
.grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
.card {
  display: block;
  padding: 18px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--surface);
}
a.card:hover { border-color: #bdc7d5; color: inherit; transform: translateY(-1px); }
.card .label { color: var(--muted); font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .055em; }
.card .value { margin-top: 7px; font-size: 20px; font-weight: 760; letter-spacing: -.025em; overflow-wrap: anywhere; }
.card .meta { margin-top: 11px; display: grid; gap: 4px; color: var(--muted); font-size: 13px; }
.section { margin-top: 28px; }
.panel { overflow: hidden; border: 1px solid var(--line); border-radius: 14px; background: var(--surface); }
.panel-pad { padding: 18px; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 12px 14px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; white-space: nowrap; }
th { color: var(--muted); background: #fafbfc; font-size: 11px; text-transform: uppercase; letter-spacing: .055em; }
tr:last-child td { border-bottom: 0; }
tbody tr:hover { background: #fbfcfe; }
.wrap { white-space: normal; min-width: 260px; }
.badge { display: inline-flex; align-items: center; border-radius: 999px; padding: 3px 8px; font-size: 11px; font-weight: 750; text-transform: uppercase; letter-spacing: .035em; }
.badge.healthy, .badge.success, .badge.completed { color: var(--success); background: var(--success-bg); }
.badge.degraded, .badge.running, .badge.partial { color: var(--warn); background: var(--warn-bg); }
.badge.failed, .badge.error { color: var(--danger); background: var(--danger-bg); }
.badge.no_data, .badge.missing, .badge.pending, .badge.unknown { color: var(--neutral); background: var(--neutral-bg); }
.stats { display: flex; flex-wrap: wrap; gap: 8px 18px; color: var(--muted); font-size: 13px; }
.stats strong { color: var(--text); }
.toolbar { display: flex; align-items: end; justify-content: space-between; gap: 14px; flex-wrap: wrap; }
.filters { display: flex; align-items: end; gap: 9px; flex-wrap: wrap; }
.field { display: grid; gap: 5px; }
.field label { color: var(--muted); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .045em; }
input, select, button {
  min-height: 36px;
  border: 1px solid #ccd4df;
  border-radius: 8px;
  background: #fff;
  color: var(--text);
  padding: 7px 9px;
  font: inherit;
}
button { cursor: pointer; padding-inline: 13px; background: var(--text); border-color: var(--text); color: #fff; font-weight: 700; }
button:hover { opacity: .9; }
.kv { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0; }
.kv > div { padding: 15px 16px; border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); }
.kv > div:nth-child(4n) { border-right: 0; }
.kv dt { color: var(--muted); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .045em; }
.kv dd { margin: 5px 0 0; font-weight: 650; overflow-wrap: anywhere; }
.empty { padding: 38px 18px; text-align: center; color: var(--muted); }
.error-box { padding: 14px 16px; border: 1px solid #ffd2cc; border-radius: 12px; background: var(--danger-bg); color: var(--danger); }
.breadcrumbs { display: flex; flex-wrap: wrap; gap: 7px; margin-bottom: 12px; color: var(--muted); font-size: 12px; }
.breadcrumbs a:hover { color: var(--accent); }
.actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.actions a { padding: 7px 10px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); color: var(--muted); font-size: 12px; font-weight: 650; }
.actions a:hover { color: var(--text); border-color: #bdc7d5; }
@media (max-width: 900px) {
  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .kv { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .kv > div:nth-child(4n) { border-right: 1px solid var(--line); }
  .kv > div:nth-child(2n) { border-right: 0; }
}
@media (max-width: 620px) {
  .topbar { padding: 0 16px; }
  .brand small { display: none; }
  .main { width: min(100% - 24px, 1180px); padding-top: 24px; }
  .heading { align-items: flex-start; flex-direction: column; }
  .grid, .kv { grid-template-columns: 1fr; }
  .kv > div { border-right: 0 !important; }
  .nav a:first-child { display: none; }
}
"""


def _text(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return str(getattr(value, "value", value))


def _e(value: Any) -> str:
    return escape(_text(value))


def _badge(value: Any) -> str:
    raw = _text(value).lower()
    css = raw.replace(" ", "_")
    return f'<span class="badge {escape(css)}">{escape(raw)}</span>'


def _query(path: str, **params: Any) -> str:
    clean = {key: _text(value) for key, value in params.items() if value not in (None, "")}
    return path if not clean else f"{path}?{urlencode(clean)}"


def _resource_url(resource: str, source: str = DEFAULT_SOURCE) -> str:
    return _query(f"/ops/resources/{quote(resource, safe='')}", source=source)


def _date_url(resource: str, source_date: date, source: str = DEFAULT_SOURCE) -> str:
    return _query(
        f"/ops/resources/{quote(resource, safe='')}/dates/{source_date.isoformat()}",
        source=source,
    )


def _run_url(run_id: str, source: str = DEFAULT_SOURCE) -> str:
    return _query(f"/ops/runs/{quote(run_id, safe='')}", source=source)


def _attempt_url(run_id: str, source_date: date, source: str = DEFAULT_SOURCE) -> str:
    return _query(
        f"/ops/attempts/{quote(run_id, safe='')}/{source_date.isoformat()}",
        source=source,
    )


def _api_url(path: str, source: str = DEFAULT_SOURCE, **params: Any) -> str:
    return _query(path, source=source, **params)


def _breadcrumbs(*items: tuple[str, str | None]) -> str:
    rendered: list[str] = []
    for label, href in items:
        if href is None:
            rendered.append(f"<span>{escape(label)}</span>")
        else:
            rendered.append(f'<a href="{escape(href, quote=True)}">{escape(label)}</a>')
    return '<div class="breadcrumbs">' + "<span>/</span>".join(rendered) + "</div>"


def _layout(title: str, body: str, *, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    overview_url = _query("/ops", source=source)
    errors_url = _query("/ops/errors", source=source)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{escape(title)} · Procurement Ops</title>
  <style>{_STYLE}</style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <a class="brand" href="{escape(overview_url, quote=True)}">Procurement Lakehouse <small>Ops</small></a>
      <nav class="nav">
        <a href="{escape(overview_url, quote=True)}">Overview</a>
        <a href="{escape(errors_url, quote=True)}">Errors</a>
        <a href="/docs">API docs</a>
      </nav>
    </header>
    <main class="main">{body}</main>
  </div>
</body>
</html>"""
    return HTMLResponse(html)


def _not_found(title: str, message: str, *, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    response = _layout(
        title,
        f'<div class="heading"><div><h1>{escape(title)}</h1><p>{escape(message)}</p></div></div>',
        source=source,
    )
    response.status_code = 404
    return response


def _bad_request(exc: ValueError, *, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    response = _layout(
        "Invalid request",
        '<div class="heading"><div><h1>Invalid request</h1></div></div>'
        f'<div class="error-box">{escape(str(exc))}</div>',
        source=source,
    )
    response.status_code = 400
    return response


def _attempt_row(attempt: AttemptSummary, source: str) -> str:
    return f"""
<tr>
  <td><a class="link mono" href="{escape(_attempt_url(attempt.run_id, attempt.source_date, source), quote=True)}">{_e(attempt.run_id)}</a></td>
  <td>{_badge(attempt.status)}</td>
  <td>{_e(attempt.source_date)}</td>
  <td>{attempt.completed_pages}/{_e(attempt.expected_pages)}</td>
  <td>{attempt.search_items}</td>
  <td>{attempt.bronze_records}</td>
  <td>{attempt.error_count}</td>
  <td>{_e(attempt.duration_seconds)}s</td>
  <td><a class="link" href="{escape(_run_url(attempt.run_id, source), quote=True)}">run</a></td>
</tr>"""


def _error_row(error: ErrorSummary, source: str) -> str:
    attempt_href = _attempt_url(error.run_id, error.source_date, source)
    return f"""
<tr>
  <td>{_e(error.occurred_at)}</td>
  <td><a class="link" href="{escape(_resource_url(error.resource, source), quote=True)}">{_e(error.resource)}</a></td>
  <td><a class="link" href="{escape(attempt_href, quote=True)}">{_e(error.source_date)}</a></td>
  <td>{_e(error.stage)}</td>
  <td>{_e(error.error_type)}</td>
  <td>{_e(error.page_number)}</td>
  <td>{_e(error.http_status)}</td>
  <td class="wrap">{_e(error.message)}</td>
  <td><a class="link mono" href="{escape(_run_url(error.run_id, source), quote=True)}">{_e(error.run_id)}</a></td>
</tr>"""


@router.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/ops", status_code=307)


@router.get("/ops", response_class=HTMLResponse)
def overview_page(service: Service, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    try:
        overview = service.overview(source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source)

    cards = "".join(
        f"""
<a class="card" href="{escape(_resource_url(item.resource, source), quote=True)}">
  <div class="label">{_e(item.resource)}</div>
  <div class="value">{_badge(item.health)}</div>
  <div class="meta">
    <span>latest: <strong>{_e(item.latest_source_date)}</strong></span>
    <span>latest success: <strong>{_e(item.latest_success_source_date)}</strong></span>
    <span>failed dates: <strong>{item.unresolved_failed_dates}</strong> · attempts: <strong>{item.total_attempts}</strong></span>
  </div>
</a>"""
        for item in overview.resources
    )
    body = f"""
<div class="heading">
  <div>
    <h1>Ingestion overview</h1>
    <p>Operational health projected from control manifests and ingestion errors.</p>
  </div>
  <div class="actions"><a href="{escape(_api_url('/api/ops/overview', source), quote=True)}">JSON</a></div>
</div>
<div class="stats"><span>source <strong>{escape(source)}</strong></span><span>generated <strong>{_e(overview.generated_at)}</strong></span></div>
<div class="section grid">{cards}</div>
"""
    return _layout("Overview", body, source=source)


@router.get("/ops/resources/{resource}", response_class=HTMLResponse)
def resource_page(
    resource: str,
    service: Service,
    source: str = DEFAULT_SOURCE,
    start_date: date | None = None,
    end_date: date | None = None,
) -> HTMLResponse:
    try:
        summary = service.get_resource_summary(resource, source=source)
        dates = service.list_dates(
            resource,
            source=source,
            start_date=start_date,
            end_date=end_date,
        )
    except ValueError as exc:
        return _bad_request(exc, source=source)

    rows = "".join(
        f"""
<tr>
  <td><a class="link" href="{escape(_date_url(resource, item.source_date, source), quote=True)}">{_e(item.source_date)}</a></td>
  <td>{_badge(item.status)}</td>
  <td>{item.attempt_count}</td>
  <td>{item.bronze_records}</td>
  <td>{item.error_count}</td>
  <td>{_e(item.last_attempt_at)}</td>
  <td>{('<a class="link mono" href="' + escape(_run_url(item.latest_run_id, source), quote=True) + '">' + _e(item.latest_run_id) + '</a>') if item.latest_run_id else '—'}</td>
</tr>"""
        for item in dates
    )
    start_value = "" if start_date is None else start_date.isoformat()
    end_value = "" if end_date is None else end_date.isoformat()
    body = f"""
{_breadcrumbs(("Ops", _query('/ops', source=source)), (resource, None))}
<div class="heading">
  <div><h1>{escape(resource)}</h1><p>Daily ingestion timeline and recovery state.</p></div>
  <div class="actions"><a href="{escape(_api_url(f'/api/ops/resources/{quote(resource, safe="")}/dates', source, start_date=start_date, end_date=end_date), quote=True)}">JSON</a></div>
</div>
<div class="stats">
  <span>health <strong>{_badge(summary.health)}</strong></span>
  <span>latest <strong>{_e(summary.latest_source_date)}</strong></span>
  <span>latest success <strong>{_e(summary.latest_success_source_date)}</strong></span>
  <span>unresolved failed dates <strong>{summary.unresolved_failed_dates}</strong></span>
  <span>attempts <strong>{summary.total_attempts}</strong></span>
</div>
<div class="section panel panel-pad">
  <form class="toolbar" method="get">
    <input type="hidden" name="source" value="{escape(source, quote=True)}">
    <div class="filters">
      <div class="field"><label for="start_date">Start date</label><input id="start_date" type="date" name="start_date" value="{escape(start_value, quote=True)}"></div>
      <div class="field"><label for="end_date">End date</label><input id="end_date" type="date" name="end_date" value="{escape(end_value, quote=True)}"></div>
      <button type="submit">Apply</button>
    </div>
  </form>
</div>
<div class="section panel table-wrap">
  <table>
    <thead><tr><th>Date</th><th>Status</th><th>Attempts</th><th>Bronze records</th><th>Errors</th><th>Last attempt</th><th>Latest run</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>
"""
    return _layout(resource, body, source=source)


@router.get("/ops/resources/{resource}/dates/{source_date}", response_class=HTMLResponse)
def date_page(
    resource: str,
    source_date: date,
    service: Service,
    source: str = DEFAULT_SOURCE,
) -> HTMLResponse:
    try:
        detail = service.get_date(resource, source_date, source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source)

    rows = "".join(_attempt_row(item, source) for item in detail.attempts)
    empty = '<div class="empty">No attempts were recorded for this date.</div>' if not rows else ""
    table = "" if not rows else f"""
<div class="panel table-wrap">
  <table>
    <thead><tr><th>Run</th><th>Status</th><th>Date</th><th>Pages</th><th>Search items</th><th>Bronze</th><th>Errors</th><th>Duration</th><th></th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""
    body = f"""
{_breadcrumbs(("Ops", _query('/ops', source=source)), (resource, _resource_url(resource, source)), (source_date.isoformat(), None))}
<div class="heading">
  <div><h1>{source_date.isoformat()}</h1><p>{escape(resource)} ingestion attempts for this source date.</p></div>
  <div class="actions"><a href="{escape(_api_url(f'/api/ops/resources/{quote(resource, safe="")}/dates/{source_date.isoformat()}', source), quote=True)}">JSON</a></div>
</div>
<div class="stats"><span>status <strong>{_badge(detail.status)}</strong></span><span>attempts <strong>{len(detail.attempts)}</strong></span><span>effective run <strong class="mono">{_e(detail.effective_run_id)}</strong></span></div>
<div class="section">{table}{empty}</div>
"""
    return _layout(f"{resource} · {source_date}", body, source=source)


@router.get("/ops/runs/{run_id}", response_class=HTMLResponse)
def run_page(run_id: str, service: Service, source: str = DEFAULT_SOURCE) -> HTMLResponse:
    try:
        detail = service.get_run(run_id, source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source)
    if detail is None:
        return _not_found("Run not found", f"No run exists with id {run_id}.", source=source)

    run = detail.run
    rows = "".join(_attempt_row(item, source) for item in detail.attempts)
    body = f"""
{_breadcrumbs(("Ops", _query('/ops', source=source)), (run.resource, _resource_url(run.resource, source)), (run_id, None))}
<div class="heading">
  <div><h1 class="mono">{_e(run_id)}</h1><p>Range run across {run.start_date} → {run.end_date}.</p></div>
  <div class="actions"><a href="{escape(_api_url(f'/api/ops/runs/{quote(run_id, safe="")}', source), quote=True)}">JSON</a></div>
</div>
<div class="panel">
  <dl class="kv">
    <div><dt>Status</dt><dd>{_badge(run.status)}</dd></div>
    <div><dt>Resource</dt><dd><a class="link" href="{escape(_resource_url(run.resource, source), quote=True)}">{_e(run.resource)}</a></dd></div>
    <div><dt>Dates</dt><dd>{run.success_dates} success / {run.failed_dates} failed / {run.total_dates} total</dd></div>
    <div><dt>Duration</dt><dd>{_e(run.duration_seconds)}s</dd></div>
    <div><dt>Started</dt><dd>{_e(run.started_at)}</dd></div>
    <div><dt>Completed</dt><dd>{_e(run.completed_at)}</dd></div>
    <div><dt>Source</dt><dd>{_e(run.source)}</dd></div>
    <div><dt>Window</dt><dd>{_e(run.start_date)} → {_e(run.end_date)}</dd></div>
  </dl>
</div>
<div class="section"><h2>Day attempts</h2><div class="panel table-wrap"><table><thead><tr><th>Run</th><th>Status</th><th>Date</th><th>Pages</th><th>Search items</th><th>Bronze</th><th>Errors</th><th>Duration</th><th></th></tr></thead><tbody>{rows}</tbody></table></div></div>
"""
    return _layout(f"Run {run_id}", body, source=source)


@router.get("/ops/attempts/{run_id}/{source_date}", response_class=HTMLResponse)
def attempt_page(
    run_id: str,
    source_date: date,
    service: Service,
    source: str = DEFAULT_SOURCE,
) -> HTMLResponse:
    try:
        detail = service.get_attempt(run_id, source_date, source=source)
    except ValueError as exc:
        return _bad_request(exc, source=source)
    if detail is None:
        return _not_found("Attempt not found", f"No attempt exists for {run_id} on {source_date}.", source=source)

    attempt = detail.attempt
    page_rows = "".join(
        f"<tr><td>{item.page_number}</td><td>{_badge(item.status)}</td><td>{item.page_size}</td><td>{item.search_items}</td><td>{item.bronze_records}</td><td>{item.error_count}</td><td>{_e(item.duration_seconds)}s</td></tr>"
        for item in detail.pages
    )
    error_rows = "".join(_error_row(item, source) for item in detail.errors)
    pages_content = (
        f'<div class="panel table-wrap"><table><thead><tr><th>Page</th><th>Status</th><th>Size</th><th>Search items</th><th>Bronze</th><th>Errors</th><th>Duration</th></tr></thead><tbody>{page_rows}</tbody></table></div>'
        if page_rows
        else '<div class="panel empty">No page manifests found.</div>'
    )
    errors_content = (
        f'<div class="panel table-wrap"><table><thead><tr><th>Occurred</th><th>Resource</th><th>Date</th><th>Stage</th><th>Type</th><th>Page</th><th>HTTP</th><th>Message</th><th>Run</th></tr></thead><tbody>{error_rows}</tbody></table></div>'
        if error_rows
        else '<div class="panel empty">No errors recorded for this attempt.</div>'
    )
    body = f"""
{_breadcrumbs(("Ops", _query('/ops', source=source)), (attempt.resource, _resource_url(attempt.resource, source)), (source_date.isoformat(), _date_url(attempt.resource, source_date, source)), (run_id, None))}
<div class="heading">
  <div><h1>Attempt · {source_date.isoformat()}</h1><p class="mono">{_e(run_id)}</p></div>
  <div class="actions"><a href="{escape(_run_url(run_id, source), quote=True)}">Run</a><a href="{escape(_api_url(f'/api/ops/attempts/{quote(run_id, safe="")}/{source_date.isoformat()}', source), quote=True)}">JSON</a></div>
</div>
<div class="panel">
  <dl class="kv">
    <div><dt>Status</dt><dd>{_badge(attempt.status)}</dd></div>
    <div><dt>Pages</dt><dd>{attempt.completed_pages} / {_e(attempt.expected_pages)}</dd></div>
    <div><dt>Search items</dt><dd>{attempt.search_items}</dd></div>
    <div><dt>Bronze records</dt><dd>{attempt.bronze_records}</dd></div>
    <div><dt>Errors</dt><dd>{attempt.error_count}</dd></div>
    <div><dt>Duration</dt><dd>{_e(attempt.duration_seconds)}s</dd></div>
    <div><dt>Started</dt><dd>{_e(attempt.started_at)}</dd></div>
    <div><dt>Completed</dt><dd>{_e(attempt.completed_at)}</dd></div>
  </dl>
</div>
<div class="section"><h2>Pages</h2>{pages_content}</div>
<div class="section"><h2>Errors</h2>{errors_content}</div>
"""
    return _layout(f"Attempt {run_id}", body, source=source)


@router.get("/ops/errors", response_class=HTMLResponse)
def errors_page(
    service: Service,
    source: str = DEFAULT_SOURCE,
    resource: str | None = None,
    source_date: date | None = None,
    run_id: str | None = None,
    stage: str | None = None,
    error_type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> HTMLResponse:
    try:
        errors = service.list_errors(
            source=source,
            resource=resource,
            source_date=source_date,
            run_id=run_id,
            stage=stage,
            error_type=error_type,
            limit=limit,
        )
    except ValueError as exc:
        return _bad_request(exc, source=source)

    rows = "".join(_error_row(item, source) for item in errors)
    resource_value = "" if resource is None else resource
    date_value = "" if source_date is None else source_date.isoformat()
    run_value = "" if run_id is None else run_id
    stage_value = "" if stage is None else stage
    type_value = "" if error_type is None else error_type
    api_href = _api_url(
        "/api/ops/errors",
        source,
        resource=resource,
        source_date=source_date,
        run_id=run_id,
        stage=stage,
        error_type=error_type,
        limit=limit,
    )
    table = (
        f'<div class="panel table-wrap"><table><thead><tr><th>Occurred</th><th>Resource</th><th>Date</th><th>Stage</th><th>Type</th><th>Page</th><th>HTTP</th><th>Message</th><th>Run</th></tr></thead><tbody>{rows}</tbody></table></div>'
        if rows
        else '<div class="panel empty">No errors match these filters.</div>'
    )
    body = f"""
{_breadcrumbs(("Ops", _query('/ops', source=source)), ("Errors", None))}
<div class="heading">
  <div><h1>Errors</h1><p>Read-only ingestion errors. Recovery is represented by a later successful attempt.</p></div>
  <div class="actions"><a href="{escape(api_href, quote=True)}">JSON</a></div>
</div>
<div class="panel panel-pad">
  <form class="filters" method="get">
    <input type="hidden" name="source" value="{escape(source, quote=True)}">
    <div class="field"><label>Resource</label><input name="resource" value="{escape(resource_value, quote=True)}" placeholder="notify_contractor"></div>
    <div class="field"><label>Date</label><input type="date" name="source_date" value="{escape(date_value, quote=True)}"></div>
    <div class="field"><label>Run id</label><input name="run_id" value="{escape(run_value, quote=True)}"></div>
    <div class="field"><label>Stage</label><input name="stage" value="{escape(stage_value, quote=True)}"></div>
    <div class="field"><label>Error type</label><input name="error_type" value="{escape(type_value, quote=True)}"></div>
    <div class="field"><label>Limit</label><input type="number" min="1" max="1000" name="limit" value="{limit}"></div>
    <button type="submit">Filter</button>
  </form>
</div>
<div class="section">{table}</div>
"""
    return _layout("Errors", body, source=source)
