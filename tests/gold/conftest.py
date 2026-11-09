from datetime import UTC, date, datetime

import duckdb
import pytest

from procurement.gold.domain.models import INPUT_TABLES, RunPlan, SilverReadyInput, Snapshot


@pytest.fixture
def plan():
    ready = SilverReadyInput(
        input_id="input1",
        cohort_id="cohort1",
        namespace="silver",
        mapping_version="mapping1",
        completed_at=datetime(2026, 11, 7, tzinfo=UTC),
        source_start=date(2022, 1, 1),
        source_end=date(2026, 11, 6),
        source_complete=True,
        blocking_issues=0,
        quarantined=0,
        tables=tuple(
            Snapshot(table=n, table_uuid="uuid_" + n, snapshot_id=1, rows=0) for n in INPUT_TABLES
        ),
    )
    return RunPlan(
        run_id="r1",
        mode="backfill",
        logic_version="a" * 64,
        reference_version="raw_v1",
        target_namespace="gold_v2_test",
        source=ready,
        source_cutoff=date(2026, 11, 6),
        event_start=date(2022, 1, 1),
        event_end=date(2025, 12, 31),
        observation_day=date(2026, 11, 7),
    )


@pytest.fixture
def db():
    with duckdb.connect(config={"threads": "1", "memory_limit": "256MB"}) as connection:
        yield connection
