from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, TypedDict

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.storage.error_records import ErrorRecord

FetchPage = Callable[..., dict[str, Any]]
BuildQueryDefinition = Callable[..., Mapping[str, Any]]
IterRecords = Callable[..., Iterator[dict[str, Any]]]


class DailyResult(TypedDict):
    status: str
    pages: int
    search_items: int
    errors: int


@dataclass(frozen=True)
class ResourceSpec:
    """The resource-specific hooks required by the shared ingestion engine."""

    identity: ResourceIdentity
    pipeline_name: str
    dataset_name: str
    fetch_page: FetchPage
    build_query_definition: BuildQueryDefinition
    iter_records: IterRecords

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
