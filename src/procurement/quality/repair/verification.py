"""Verification and baseline validation for quality repair."""

from collections.abc import Callable
from datetime import date
from typing import Any

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.quality.audit import assess_record, record_key, selection_day
from procurement.storage.committed import CommittedDay, iter_committed_records


def assert_baseline(
    fs: Any, identity: Any, selection: dict[str, Any], coverage_reader: Callable[..., Any] | None = None
) -> None:
    """Verify that the baseline attempt is still effective and uncontested."""
    day = date.fromisoformat(selection["date"])
    coverage = coverage_reader(day) if coverage_reader else read_coverage(fs, identity, day, day)[0]
    if coverage.active_run_ids:
        raise ValueError(f"Another active run exists for the repair date: {coverage.active_run_ids}")
    if coverage.effective is None or coverage.effective.run_id != selection["run_id"]:
        raise ValueError("Effective baseline changed; create a new audit/plan")


def read_baseline(
    fs: Any, selection: dict[str, Any], records: list[dict[str, Any]]
) -> list[tuple[str, dict[str, Any]]]:
    """Read committed baseline records and ensure identities/hashes match plan."""
    expected = {record_key(row): row for row in records}
    if len(expected) != len(records) or len(records) != selection["expected_records"]:
        raise ValueError("Duplicate identities or inconsistent baseline count")
    baseline = list(iter_committed_records(fs, (selection_day(selection),), verify_hash=True))
    seen: set[tuple[Any, Any]] = set()
    for table, record in baseline:
        key = record_key(record)
        reference = expected.get(key)
        if (
            key in seen
            or reference is None
            or reference["table"] != table
            or reference["content_hash"] != record["content_hash"]
        ):
            raise ValueError("Baseline identity/table/hash changed")
        seen.add(key)
    if seen != set(expected):
        raise ValueError("Baseline identity set changed")
    return baseline


def verify_output(
    fs: Any, identity: Any, day: date, run_id: str, count: int, expected: dict[tuple[Any, Any], dict[str, Any]], config: Any
) -> None:
    """Read back written parquet files and verify hashes and validity against expected records."""
    files = []
    for table in get_resource(identity.resource).tables:
        prefix = f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{identity.source}/{table}/source_date={day}/run_id={run_id}"
        files.extend((table, key) for key in sorted(fs.glob(f"{prefix}/*.parquet")))
    selection = CommittedDay(day, run_id, count, tuple(files))
    observed: set[tuple[Any, Any]] = set()
    for table, record in iter_committed_records(fs, (selection,), verify_hash=True):
        key = record_key(record)
        if key in observed or key not in expected:
            raise ValueError("Duplicate/unexpected repaired identity")
        observed.add(key)
        if expected[key].get("output_hash") != record["content_hash"]:
            raise ValueError("Written repair payload differs from validated replacement")
        result = assess_record(table, record, expected[key]["context"], config)["result"]
        if result["status"] not in {"pass", "warn"}:
            raise ValueError("Written repair record failed post-write validation")
    if observed != set(expected):
        raise ValueError("Repaired identity set changed")
