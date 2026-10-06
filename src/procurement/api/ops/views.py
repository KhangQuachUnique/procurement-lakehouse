from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from string import Template
from typing import Any
from urllib.parse import quote, urlencode

from fastapi.responses import HTMLResponse

from procurement.common.dates import VIETNAM_TZ
from procurement.common.settings import settings
from procurement.ops.models import AttemptSummary, DateSummary, ErrorSummary, RunSummary
from procurement.ops.service import DEFAULT_SOURCE, SUPPORTED_RESOURCES

_TEMPLATE = Template((Path(__file__).parent / "templates/layout.html").read_text(encoding="utf-8"))

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
    html = _TEMPLATE.substitute(
        title=escape(title), runs_url=escape(_query('/ops', source=source), quote=True),
        calendar_url=escape(_query('/ops/calendar', source=source), quote=True),
        errors_url=escape(_query('/ops/errors', source=source), quote=True),
        runs_class=runs_class, calendar_class=calendar_class, errors_class=errors_class,
        body=body, extra_script=extra_script, dagster_url=escape(settings.DAGSTER_URL, quote=True),
    )
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


