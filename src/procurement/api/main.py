from fastapi import FastAPI, HTTPException

from procurement.api.ops import router as ops_api_router
from procurement.api.ops.ui import router as ops_ui_router
from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.storage.object_store import create_s3_filesystem

configure_logging()
app = FastAPI(title="Procurement Lakehouse Ops API", version="0.2.0")
app.include_router(ops_ui_router)
app.include_router(ops_api_router)


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
