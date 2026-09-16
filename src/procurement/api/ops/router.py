from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from procurement.api.ops.dependencies import get_ops_service
from procurement.ops.models import (
    AttemptDetail,
    DateDetail,
    DateSummary,
    ErrorSummary,
    OpsOverview,
    ResourceSummary,
    RunDetail,
    RunSummary,
)
from procurement.ops.service import DEFAULT_SOURCE, OpsService

router = APIRouter(prefix="/api/ops", tags=["ops"])
Service = Annotated[OpsService, Depends(get_ops_service)]


def _bad_request(exc: ValueError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.get("/overview", response_model=OpsOverview)
def overview(service: Service, source: str = DEFAULT_SOURCE) -> OpsOverview:
    try:
        return service.overview(source=source)
    except ValueError as exc:
        raise _bad_request(exc) from exc


@router.get("/resources", response_model=list[ResourceSummary])
def list_resources(service: Service, source: str = DEFAULT_SOURCE) -> list[ResourceSummary]:
    try:
        return service.list_resources(source=source)
    except ValueError as exc:
        raise _bad_request(exc) from exc


@router.get("/resources/{resource}/dates", response_model=list[DateSummary])
def list_dates(
    resource: str,
    service: Service,
    source: str = DEFAULT_SOURCE,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[DateSummary]:
    try:
        return service.list_dates(
            resource,
            source=source,
            start_date=start_date,
            end_date=end_date,
        )
    except ValueError as exc:
        raise _bad_request(exc) from exc


@router.get("/resources/{resource}/dates/{source_date}", response_model=DateDetail)
def get_date(
    resource: str,
    source_date: date,
    service: Service,
    source: str = DEFAULT_SOURCE,
) -> DateDetail:
    try:
        return service.get_date(resource, source_date, source=source)
    except ValueError as exc:
        raise _bad_request(exc) from exc


@router.get("/runs", response_model=list[RunSummary])
def list_runs(
    service: Service,
    source: str = DEFAULT_SOURCE,
    resource: str | None = None,
    status: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> list[RunSummary]:
    try:
        return service.list_runs(
            source=source,
            resource=resource,
            status=status,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except ValueError as exc:
        raise _bad_request(exc) from exc


@router.get("/runs/{run_id}", response_model=RunDetail)
def get_run(run_id: str, service: Service, source: str = DEFAULT_SOURCE) -> RunDetail:
    try:
        result = service.get_run(run_id, source=source)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return result


@router.get("/attempts/{run_id}/{source_date}", response_model=AttemptDetail)
def get_attempt(
    run_id: str,
    source_date: date,
    service: Service,
    source: str = DEFAULT_SOURCE,
) -> AttemptDetail:
    try:
        result = service.get_attempt(run_id, source_date, source=source)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Attempt not found")
    return result


@router.get("/errors", response_model=list[ErrorSummary])
def list_errors(
    service: Service,
    source: str = DEFAULT_SOURCE,
    resource: str | None = None,
    source_date: date | None = None,
    run_id: str | None = None,
    stage: str | None = None,
    error_type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> list[ErrorSummary]:
    try:
        return service.list_errors(
            source=source,
            resource=resource,
            source_date=source_date,
            run_id=run_id,
            stage=stage,
            error_type=error_type,
            limit=limit,
        )
    except ValueError as exc:
        raise _bad_request(exc) from exc
