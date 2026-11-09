import pyarrow as pa
import pytest

from procurement.tools.gold_history_batches import groups


def item(day, **tables):
    return {"report": {"days": [{"date": day}]}, "tables": tables}


def revision(key):
    return pa.table({"revision_id": [key]})


def test_disjoint_inputs_batch_but_daily_default_remains_one_cohort():
    values = [
        item("2024-04-09", plan_revision=revision("a")),
        item("2024-04-10", plan_revision=revision("b")),
    ]
    assert [len(g) for g in groups(values)] == [2]
    assert [len(g) for g in groups(values, max_days=1)] == [1, 1]


def test_repeated_primary_key_and_child_owner_force_ordered_boundary():
    values = [
        item(
            "2024-04-09",
            result_revision=revision("r"),
            award_lot=pa.table({"award_lot_id": ["old"], "revision_id": ["r"]}),
        ),
        item(
            "2024-04-10",
            result_revision=revision("r"),
            award_lot=pa.table(
                {
                    "award_lot_id": pa.array([], type=pa.string()),
                    "revision_id": pa.array([], type=pa.string()),
                }
            ),
        ),
    ]
    assert [len(g) for g in groups(values)] == [1, 1]
    # Even without a repeated parent PK, a later child write for that owner must not merge.
    values[1]["tables"].pop("result_revision")
    values[1]["tables"]["award_lot"] = pa.table({"award_lot_id": ["new"], "revision_id": ["r"]})
    assert [len(g) for g in groups(values)] == [1, 1]


def test_governance_cleanup_observation_boundary():
    values = [
        item("2024-04-09", quarantine=pa.table({"observation_id": ["o"]})),
        item("2024-04-10", observation=pa.table({"observation_id": ["o"]})),
    ]
    assert [len(g) for g in groups(values)] == [1, 1]


def test_budget_calendar_prefix_and_unknown_semantics_are_singletons():
    values = [
        item("2024-12-31", plan_revision=revision("a")),
        item("2025-01-01", plan_revision=revision("b")),
    ]
    assert [len(g) for g in groups(values)] == [1, 1]
    values[1]["report"]["days"][0]["date"] = "2024-12-31"
    assert [len(g) for g in groups(values, max_rows=1)] == [1, 1]
    assert [len(g) for g in groups(values, max_bytes=1)] == [1, 1]
    values[1]["report"]["days"][0]["date"] = "2025-01-08"
    assert [len(g) for g in groups(values)] == [1, 1]
    values[1]["tables"] = {"contract_performance_revision": revision("c")}
    assert [len(g) for g in groups(values)] == [1, 1]


def test_existing_ack_flushes_pending_before_reuse():
    values = [
        item("2024-04-09", plan_revision=revision("a")),
        item("2024-04-10", plan_revision=revision("b")),
    ]
    values[1]["existing"] = {"report": "published"}
    assert [len(g) for g in groups(values)] == [1, 1]


@pytest.mark.parametrize("bounds", [{"max_days": 0}, {"max_rows": 0}, {"max_bytes": 0}])
def test_invalid_bounds_rejected(bounds):
    with pytest.raises(ValueError, match="Positive"):
        list(groups([], **bounds))
