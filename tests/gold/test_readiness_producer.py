from copy import deepcopy
from datetime import UTC, date, datetime
from unittest.mock import Mock

import pytest

from procurement.gold.domain.models import SilverReadyInput
from procurement.silver.application.readiness import (
    OWNED,
    RESOURCES,
    complete_day_publication,
    seal_readiness,
    validate_evidence,
)
from procurement.silver.application.resources import get_silver_resource

DAY = date(2026, 11, 6)


def reports():
    result = []
    for resource in RESOURCES:
        tables = (*OWNED[resource], "observation")
        result.append(
            {
                "resource": resource,
                "mapping_version": get_silver_resource(resource).mapping_version,
                "run_id": "run_" + resource,
                "family": "all",
                "iceberg_enabled": True,
                "days": [
                    {
                        "date": str(DAY),
                        "status": "completed",
                        "input_run_id": "bronze_" + resource,
                        "input_files": [
                            {"table": "raw", "key": "source.parquet", "sha256": "test_hash"}
                        ],
                        "quality": {"accepted": 1, "quarantined": 0, "blocking_issues": 0},
                        "iceberg": {
                            t: {"snapshot_id": 1, "table_uuid": "uuid_" + t} for t in tables
                        },
                    }
                ],
            }
        )
    return result


def catalog():
    value = Mock()
    value.table.side_effect = lambda ns, t: {
        "metadata": {
            "table-uuid": "uuid_" + t,
            "current-snapshot-id": 2,
            "snapshots": [{"snapshot-id": 1}, {"snapshot-id": 2, "parent-snapshot-id": 1}],
        }
    }
    return value


def test_receipt_seals_only_complete_coherent_evidence():
    bundle = seal_readiness(
        reports(),
        start=DAY,
        end=DAY,
        namespace="silver",
        catalog=catalog(),
        count_rows=lambda t, s: 10,
        completed_at=datetime(2026, 11, 7, tzinfo=UTC),
    )
    receipt = SilverReadyInput.model_validate(bundle["receipt"])
    assert receipt.source_complete and len(receipt.tables) == 8
    assert bundle["evidence"] and receipt.mapping_version


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "duplicate",
        "skipped",
        "quarantine",
        "missing_uuid",
        "snapshot_fork",
        "empty_without_receipt",
    ],
)
def test_incomplete_or_ambiguous_producer_evidence_rejected(change):
    values = reports()
    cat = catalog()
    if change == "missing":
        values.pop()
    elif change == "duplicate":
        values.append(deepcopy(values[0]))
    elif change == "skipped":
        values[0]["days"][0]["status"] = "skipped"
    elif change == "quarantine":
        values[0]["days"][0]["quality"]["quarantined"] = 1
    elif change == "missing_uuid":
        values[0]["days"][0]["iceberg"]["package_revision"].pop("table_uuid")
    elif change == "snapshot_fork":
        cat.table.side_effect = lambda ns, t: {
            "metadata": {
                "table-uuid": "uuid_" + t,
                "current-snapshot-id": 3,
                "snapshots": [{"snapshot-id": 3}],
            }
        }
    elif change == "empty_without_receipt":
        values[0]["days"][0]["iceberg"].pop("package_location")
    with pytest.raises(ValueError):
        seal_readiness(
            values, start=DAY, end=DAY, namespace="silver", catalog=cat, count_rows=lambda t, s: 0
        )


def test_empty_day_requires_explicit_completed_source_lineage():
    values = reports()
    for r in values:
        r["days"][0]["quality"]["accepted"] = 0
        r["days"][0]["input_files"] = []
    assert len(validate_evidence(values, DAY, DAY)[0]) == 3


def test_day_completion_does_not_relabel_new_concurrent_snapshot():
    entry = reports()[0]["days"][0]
    with pytest.raises(ValueError, match="Concurrent"):
        complete_day_publication(entry, resource="khlcnt", namespace="silver", catalog=catalog())


def test_initial_empty_day_is_valid_ancestor_of_later_first_data_snapshot():
    values = reports()
    for item in values:
        entry = item["days"][0]
        empty = deepcopy(entry)
        empty["date"] = "2026-11-05"
        empty["quality"]["accepted"] = 0
        empty["input_files"] = []
        for snapshot in empty["iceberg"].values():
            snapshot["snapshot_id"] = None
        item["days"].insert(0, empty)
    bundle = seal_readiness(
        values,
        start=date(2026, 11, 5),
        end=DAY,
        namespace="silver",
        catalog=catalog(),
        count_rows=lambda t, s: 1,
    )
    assert len(bundle["evidence"]) == 6


def test_new_sealed_cohorts_do_not_overwrite_shared_governance_parquet():
    paths = []
    for resource in RESOURCES:
        contract = get_silver_resource(resource)
        groups = contract.candidate_groups("bucket/silver", isolate_governance=True)
        for prefix, tables in groups:
            for table in tables:
                if table == "observation":
                    paths.append(prefix + "/" + table + "/source_date=2022-12-31/data.parquet")
    assert len(paths) == 3 and len(set(paths)) == 3
    assert all("/_governance/" in path for path in paths)
    # Existing legacy write layout is not silently migrated by this opt-in.
    assert get_silver_resource("khlcnt").candidate_groups("bucket/silver")[0][0] == "bucket/silver"


def test_isolated_writer_catalog_refuses_production_read_or_final_pin():
    from procurement.storage.catalog_scope import NamespaceCatalog

    underlying = Mock()
    bound = NamespaceCatalog(underlying, "test_silver_only")
    with pytest.raises(ValueError, match="namespace"):
        bound.pin("silver", "package_revision", 1, "run")
    with pytest.raises(ValueError, match="namespace"):
        bound.table("silver", "package_revision")
    with pytest.raises(ValueError, match="namespace"):
        bound.request("POST", "namespaces/silver/tables/package_revision", json={})
    assert (
        not underlying.pin.called and not underlying.table.called and not underlying.request.called
    )
    bound.pin("test_silver_only", "package_revision", 1, "run")
    underlying.pin.assert_called_once_with("test_silver_only", "package_revision", 1, "run")
