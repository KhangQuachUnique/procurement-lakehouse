from contextlib import asynccontextmanager
from datetime import UTC, datetime
from html import escape

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from procurement.api.ops import router as ops_api_router
from procurement.api.ops.dependencies import get_ops_runtime
from procurement.api.ops.ui import router as ops_ui_router
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.ops.index import IndexNotReady
from procurement.storage.object_store import create_s3_filesystem

configure_logging()


@asynccontextmanager
async def lifespan(app):
    factory = app.dependency_overrides.get(get_ops_runtime, get_ops_runtime)
    runtime = factory()
    runtime.start()
    try:
        yield
    finally:
        runtime.stop()
        get_ops_runtime.cache_clear()


app = FastAPI(title="Procurement Lakehouse Ops API", version="0.3.0", lifespan=lifespan)
app.include_router(ops_ui_router)
app.include_router(ops_api_router)


@app.exception_handler(IndexNotReady)
async def index_not_ready(request: Request, exc: IndexNotReady):
    headers = {"Retry-After": "5"}
    if request.url.path.startswith("/ops"):
        return HTMLResponse(
            '<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="refresh" '
            'content="5"><title>Ops synchronizing</title></head><body><h1>Ops is synchronizing</h1>'
            f'<p>{escape(str(exc))}</p><p>This page retries in 5 seconds.</p></body></html>',
            status_code=503, headers=headers,
        )
    return JSONResponse({"detail": str(exc)}, status_code=503, headers=headers)


@app.middleware("http")
async def sync_headers(request, call_next):
    response = await call_next(request)
    status = getattr(request.state, "ops_sync", None)
    if status:
        response.headers["X-Ops-Last-Sync"] = status["last_success_at"]
        age = (datetime.now(UTC) - datetime.fromisoformat(status["last_success_at"])).total_seconds()
        response.headers["X-Ops-Sync-State"] = (
            "degraded" if status["last_error"] else "stale" if age > 30 else "ready"
        )
    return response


@app.get("/health/live", include_in_schema=False)
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", include_in_schema=False)
def ready() -> dict[str, str]:
    try:
        available = create_s3_filesystem().exists(settings.OBJECT_STORAGE_BUCKET)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Object storage unavailable") from exc
    if not available:
        raise HTTPException(status_code=503, detail="Object storage bucket unavailable")
    return {"status": "ok"}
