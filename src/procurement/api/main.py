from datetime import date
from functools import lru_cache

from fastapi import FastAPI, HTTPException

from procurement.observability.repositories import ErrorRepository, RunRepository
from procurement.observability.service import OpsService
from procurement.storage.object_store import create_s3_filesystem

app = FastAPI(title="Procurement Lakehouse Ops API", version="0.1.0")


@lru_cache(maxsize=1)
def get_ops_service() -> OpsService:
    fs = create_s3_filesystem()
    return OpsService(RunRepository(fs), ErrorRepository(fs))


@app.get("/api/ops/runs")
def list_runs(
    source: str = "muasamcong",
    resource: str = "khlcnt",
    start_date: date | None = None,
    end_date: date | None = None,
):
    return get_ops_service().list_runs(
        source=source,
        resource=resource,
        start_date=start_date,
        end_date=end_date,
    )


@app.get("/api/ops/runs/{source}/{resource}/{run_id}")
def get_run(source: str, resource: str, run_id: str):
    run = get_ops_service().get_run(source=source, resource=resource, run_id=run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.get("/api/ops/errors")
def list_errors(
    source: str = "muasamcong",
    resource: str = "khlcnt",
    source_date: date | None = None,
    run_id: str | None = None,
):
    return get_ops_service().list_errors(
        source=source,
        resource=resource,
        source_date=source_date,
        run_id=run_id,
    )
