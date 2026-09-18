from collections.abc import Callable, Hashable, Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol, TypedDict

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.stats import PageStats
from procurement.models.bronze import BronzeRecord
from procurement.models.errors import ErrorRecord


class FetchPage(Protocol):
    def __call__(
        self, *, page_number: int, page_size: int, window_from: str, window_to: str
    ) -> dict[str, Any]: ...


class IterRecords(Protocol):
    def __call__(
        self,
        *,
        search_items: list[dict[str, Any]],
        run_id: str,
        source_date: date,
        search_page: int,
        errors: list[ErrorRecord],
        stats: PageStats,
    ) -> Iterator["BronzeItem"]: ...


class DailyResult(TypedDict):
    status: str
    pages: int
    search_items: int
    bronze_records: int
    errors: int


@dataclass(frozen=True)
class BronzeItem:
    """Internal routing wrapper; only ``record`` is persisted to Parquet."""

    table: str
    record: BronzeRecord


@dataclass(frozen=True)
class ResourceSpec:
    """Source-specific hooks consumed by the shared daily ingestion engine."""

    identity: ResourceIdentity
    pipeline_name: str
    dataset_name: str
    fetch_page: FetchPage
    iter_records: IterRecords
    search_key: Callable[[dict[str, Any]], Hashable | None] | None = None

    def records(
        self,
        *,
        search_items: list[dict[str, Any]],
        run_id: str,
        source_date: date,
        search_page: int,
        errors: list[ErrorRecord],
        stats: PageStats,
    ) -> Iterator[BronzeItem]:
        return self.iter_records(
            search_items=search_items,
            run_id=run_id,
            source_date=source_date,
            search_page=search_page,
            errors=errors,
            stats=stats,
        )
