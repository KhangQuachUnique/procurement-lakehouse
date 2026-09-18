"""Extract and load a page; the daily coordinator owns persisted status transitions."""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from procurement.common.errors import build_error_record
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.engine.stats import PageStats
from procurement.models.bronze import BronzeRecord
from procurement.models.errors import ErrorRecord
from procurement.storage.bronze import BronzeWriteError, BronzeWriter


@dataclass
class PageOutcome:
    stats: PageStats
    bronze_records: int = 0
    errors: list[ErrorRecord] = field(default_factory=list)


def run_page(
    *,
    spec: ResourceSpec,
    writer: BronzeWriter,
    search_items: list[dict[str, Any]],
    run_id: str,
    source_date: date,
    page_number: int,
) -> PageOutcome:
    outcome = PageOutcome(stats=PageStats(search_items=len(search_items)))
    grouped: dict[str, list[BronzeRecord]] = defaultdict(list)
    stage = "internal"
    try:
        for item in spec.records(
            search_items=search_items,
            run_id=run_id,
            source_date=source_date,
            search_page=page_number,
            errors=outcome.errors,
            stats=outcome.stats,
        ):
            if item.record.run_id != run_id or item.record.source_date != source_date:
                raise ValueError("Extractor returned a record belonging to another attempt")
            grouped[item.table].append(item.record)

        if not outcome.errors:
            stage = "bronze_load"
            outcome.bronze_records = writer.write_page(grouped)
    except Exception as exc:  # noqa: BLE001 -- source/writer extension boundary
        if isinstance(exc, BronzeWriteError):
            outcome.bronze_records = exc.persisted_records
            exc = exc.cause
        outcome.errors.append(
            build_error_record(
                identity=spec.identity,
                run_id=run_id,
                stage=stage,
                source_date=source_date,
                page_number=page_number,
                exc=exc,
            )
        )
    return outcome
