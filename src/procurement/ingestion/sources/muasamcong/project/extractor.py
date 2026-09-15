import logging
from collections.abc import Iterator
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.errors import ErrorStage, classify_exception
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.metadata import calculate_content_hash, utc_now
from procurement.ingestion.engine.models import BronzeItem
from procurement.ingestion.engine.stats import PageStats
from procurement.models.bronze import BronzeRecord
from procurement.models.errors import ErrorRecord
from procurement.storage.errors import build_error_record

logger = logging.getLogger(__name__)

PROGRESS_INTERVAL = 10
DETAIL_EXCEPTIONS = (httpx.HTTPError, KeyError, TypeError, ValueError)
PROJECT_TABLE = "project_detail"


class ProjectDetailApi(Protocol):
    def get_project_detail(self, project_id: str) -> dict[str, Any]: ...


def extract_project_version(payload: dict[str, Any]) -> str | None:
    project_dto = payload.get("projectDTO")
    version: Any = None
    if isinstance(project_dto, dict):
        version = project_dto.get("version")
    if version is None:
        version = payload.get("pversion")
    return None if version is None else str(version)


def build_project_record(
    *,
    source_id: str,
    source_version: str | None,
    payload: dict[str, Any],
    run_id: str,
    source_date: date,
) -> BronzeItem:
    return BronzeItem(
        table=PROJECT_TABLE,
        record=BronzeRecord(
            source_id=source_id,
            source_version=source_version,
            run_id=run_id,
            source_date=source_date,
            ingested_at=utc_now(),
            content_hash=calculate_content_hash(payload),
            payload=payload,
        ),
    )


def _record_detail_error(
    *,
    identity: ResourceIdentity,
    errors: list[ErrorRecord],
    exc: Exception,
    run_id: str,
    source_date: date,
    search_page: int,
    source_id: str | None,
) -> None:
    errors.append(
        build_error_record(
            identity=identity,
            run_id=run_id,
            stage=ErrorStage.PROJECT_DETAIL,
            source_date=source_date,
            page_number=search_page,
            exc=exc,
            classification=classify_exception(exc),
            source_id=source_id,
        )
    )
    logger.error(
        "detail_fetch_failed run_id=%s source_date=%s page=%s stage=%s source_id=%s",
        run_id,
        source_date,
        search_page,
        ErrorStage.PROJECT_DETAIL.value,
        source_id,
    )


def iter_project_records(
    client: ProjectDetailApi,
    *,
    identity: ResourceIdentity,
    search_items: list[dict[str, Any]],
    run_id: str,
    source_date: date,
    search_page: int,
    errors: list[ErrorRecord],
    stats: PageStats,
) -> Iterator[BronzeItem]:
    total_projects = len(search_items)

    for project_index, search_item in enumerate(search_items, start=1):
        project_id = search_item.get("id")
        if not project_id:
            stats.error("project")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=KeyError("id"),
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=None,
            )
            continue

        try:
            project_detail = client.get_project_detail(project_id)
        except DETAIL_EXCEPTIONS as exc:
            stats.error("project")
            _record_detail_error(
                identity=identity,
                errors=errors,
                exc=exc,
                run_id=run_id,
                source_date=source_date,
                search_page=search_page,
                source_id=project_id,
            )
            continue

        yield build_project_record(
            source_id=project_id,
            source_version=extract_project_version(project_detail),
            payload=project_detail,
            run_id=run_id,
            source_date=source_date,
        )
        stats.record("project")

        if project_index % PROGRESS_INTERVAL == 0 or project_index == total_projects:
            logger.info(
                "detail_progress run_id=%s source_date=%s page=%s projects=%s/%s errors=%s",
                run_id,
                source_date,
                search_page,
                project_index,
                total_projects,
                len(errors),
            )
