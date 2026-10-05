from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse

from procurement.api.ops.ui import _layout
from procurement.benchmark.models import BenchmarkConfig, StageConfig
from procurement.benchmark.service import BenchmarkService
from procurement.common.file_lock import LockBusy

router = APIRouter()
ASSETS = Path(__file__).with_name("benchmark_assets")
ARTIFACTS = {"config.json", "samples.json", "metadata.json", "attempts.jsonl",
             "summary.json", "stages.csv", "report.html"}


@lru_cache(maxsize=1)
def get_benchmark_service():
    return BenchmarkService()


Service = Annotated[BenchmarkService, Depends(get_benchmark_service)]


def controls(request: Request):
    # Local Ops has no login. A custom header prevents cross-origin browser form submissions.
    if request.headers.get("x-benchmark-control") != "1":
        raise HTTPException(403, "Benchmark controls require X-Benchmark-Control: 1")


@router.get("/ops/benchmark", response_class=HTMLResponse, include_in_schema=False)
def page():
    return _layout("Benchmark", (ASSETS / "page.html").read_text(encoding="utf-8"),
                   active="benchmark", extra_script='<script src="/ops/benchmark.js" defer></script>')


@router.get("/ops/benchmark.js", include_in_schema=False)
def script():
    return FileResponse(ASSETS / "app.js", media_type="text/javascript")


@router.get("/api/benchmarks")
def runs(service: Service):
    service.recover()
    return service.store.list()


@router.get("/api/benchmarks/{run_id}")
def detail(run_id: str, service: Service):
    service.recover()
    try:
        return service.store.get(run_id)
    except KeyError:
        raise HTTPException(404, "Benchmark not found") from None


@router.post("/api/benchmarks", dependencies=[Depends(controls)], status_code=202)
def start(config: BenchmarkConfig, service: Service):
    try:
        return service.start(config)
    except (ValueError, LockBusy) as exc:
        raise HTTPException(409, str(exc)) from None


def command(service, run_id, stage=None):
    try:
        service.store.command(run_id, stage)
        return service.store.get(run_id)
    except KeyError:
        raise HTTPException(404, "Benchmark not found") from None
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/api/benchmarks/{run_id}/stop", dependencies=[Depends(controls)])
def stop(run_id: str, service: Service):
    return command(service, run_id)


@router.post("/api/benchmarks/{run_id}/stage", dependencies=[Depends(controls)])
def stage(run_id: str, config: StageConfig, service: Service):
    return command(service, run_id, config)


@router.get("/api/benchmarks/{run_id}/artifacts/{name}")
def artifact(run_id: str, name: str, service: Service):
    if name not in ARTIFACTS:
        raise HTTPException(404, "Artifact not found")
    try:
        folder = service.store.folder(run_id)
    except KeyError:
        raise HTTPException(404, "Benchmark not found") from None
    if not (folder / name).is_file():
        raise HTTPException(404, "Artifact is not available yet")
    return FileResponse(folder / name, filename=name)
