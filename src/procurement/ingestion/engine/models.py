from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, TypedDict

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.storage.errors import ErrorRecord

FetchPage = Callable[..., dict[str, Any]]
BuildQueryDefinition = Callable[..., Mapping[str, Any]]
IterRecords = Callable[..., Iterator[dict[str, Any]]]
RetryError = Callable[..., Iterator[dict[str, Any]]]


class DailyResult(TypedDict):
    status: str
    pages: int
    search_items: int
    errors: int


@dataclass(frozen=True)
class ResourceSpec:
    """Resource hooks consumed by the shared ingestion/retry engines."""

    identity: ResourceIdentity
    pipeline_name: str
    dataset_name: str
    fetch_page: FetchPage
    build_query_definition: BuildQueryDefinition
    iter_records: IterRecords
    retry_error: RetryError | None = None

    def query_definition(
        self, *, source_date: date, window_from: str, window_to: str, page_size: int
    ) -> Mapping[str, Any]:
        return self.build_query_definition(
            source_date=source_date,
            window_from=window_from,
            window_to=window_to,
            page_size=page_size,
        )

    def records(
        self,
        *,
        search_items: list[dict[str, Any]],
        run_id: str,
        source_date: date,
        search_page: int,
        errors: list[ErrorRecord],
        stats: PageStats,
    ) -> Iterator[dict[str, Any]]:
        return self.iter_records(
            search_items=search_items,
            run_id=run_id,
            source_date=source_date,
            search_page=search_page,
            errors=errors,
            stats=stats,
        )

    def retry_records(
        self,
        *,
        error: ErrorRecord,
        retry_run_id: str,
        source_date: date,
    ) -> Iterator[dict[str, Any]]:
        if self.retry_error is None:
            raise RuntimeError(f"Resource {self.identity.resource} does not support record retry")
        return self.retry_error(
            error=error,
            retry_run_id=retry_run_id,
            source_date=source_date,
        )
