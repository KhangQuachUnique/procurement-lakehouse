from datetime import date
from decimal import Decimal

import pytest

from procurement.gold.application.aggregation import accept_candidate, create_ledger, prepare_candidate
from procurement.gold.domain.schema import DIMENSIONS, FACTS, TABLES, create_tables
from procurement.gold.domain.validation import validate

DAY = date(2026, 11, 7)
KEYS = {
    "report_date": DAY,
    "geography_key": "GEO_A",
    "sector_key": "SEC_A",
    "method_key": "M_A",
    "value_band_key": "B_A",
    "currency_key": "VND",
    "party_key": "P_A",
    "metric_key": "MET_A",
    "status_key": "ST_A",
    "notice_family": "standard",
    "recipient_allocation_state": "single",
}
DIM_KEY = {"date_key": DAY, "band_key": "B_A", **KEYS}


def setup(db):
    create_tables(db, "dimensions")
    create_ledger(db, "changes")
    db.execute("CREATE TABLE affected(owner_key VARCHAR PRIMARY KEY)")
    for name in DIMENSIONS:
        table = TABLES[name]
        values = [
            DIM_KEY[table.key]
            if c.name == table.key
            else "days"
            if c.name == "duration_unit"
            else None
            for c in table.columns
        ]
        db.execute(
            f"INSERT INTO dimensions.{name} VALUES ({','.join('?' for _ in values)})", values
        )
    db.execute("INSERT INTO dimensions.dim_geography(geography_key) VALUES ('GEO_B')")
    db.execute("INSERT INTO dimensions.dim_date(date_key) VALUES (?)", [date(2026, 11, 8)])


def changes(db, owners, geography="GEO_A", amount=10):
    db.execute("DELETE FROM affected")
    for name in FACTS:
        db.execute("DELETE FROM changes." + name)
    for owner in owners:
        db.execute("INSERT INTO affected VALUES (?)", [owner])
        for name in FACTS:
            table = TABLES[name]
            fields = [c for c in table.columns if c.name != "group_key"]
            values = [owner]
            for c in fields:
                if c.name in table.grain:
                    values.append(geography if c.name == "geography_key" else KEYS[c.name])
                elif c.sql_type == "BIGINT":
                    values.append(1)
                elif c.name.endswith("baseline_sum"):
                    values.append(amount * 2)
                else:
                    values.append(amount)
            db.execute(
                f"INSERT INTO changes.{name} VALUES ({','.join('?' for _ in values)})", values
            )


def prepare(db, run, day=DAY):
    return prepare_candidate(
        db,
        run,
        contributions="changes",
        affected="affected",
        dimensions="dimensions",
        observation_day=day,
    )


def public(db, schema):
    return {n: db.execute(f"SELECT * FROM {schema}.{n} ORDER BY ALL").fetchall() for n in TABLES}


def test_all_seven_fact_replay_and_fixed_schema(db):
    setup(db)
    changes(db, ["owner1", "owner2"])
    assert prepare(db, "r1")["tables"] == len(TABLES)
    accept_candidate(db, "r1")
    first = public(db, "accepted")
    changes(db, ["owner1", "owner2"])
    prepare(db, "r2")
    accept_candidate(db, "r2")
    assert public(db, "accepted") == first
    assert db.execute("SELECT planned_amount_sum FROM accepted.fact_demand_daily").fetchone()[
        0
    ] == Decimal(20)


def test_move_owner_between_old_and_new_groups_then_delete(db):
    setup(db)
    changes(db, ["owner1", "owner2"])
    prepare(db, "r1")
    accept_candidate(db, "r1")
    changes(db, ["owner1"], geography="GEO_B", amount=30)
    prepare(db, "r2")
    # Pending candidate does not mutate accepted totals.
    assert (
        db.execute("SELECT planned_amount_sum FROM accepted.fact_demand_daily").fetchone()[0] == 20
    )
    accept_candidate(db, "r2")
    assert db.execute(
        "SELECT geography_key,planned_amount_sum FROM accepted.fact_demand_daily ORDER BY 1"
    ).fetchall() == [("GEO_A", Decimal(10)), ("GEO_B", Decimal(30))]
    # Duration/status grain has no geography: one merged group, not two rows.
    assert db.execute(
        "SELECT count(*),sum(duration_sum) FROM accepted.fact_duration_daily"
    ).fetchone() == (1, Decimal(40))
    changes(db, [])
    db.execute("INSERT INTO affected VALUES ('owner1')")
    prepare(db, "r3")
    accept_candidate(db, "r3")
    assert db.execute(
        "SELECT geography_key,planned_amount_sum FROM accepted.fact_demand_daily"
    ).fetchall() == [("GEO_A", Decimal(10))]
    assert (
        db.execute(
            "SELECT count(*) FROM accepted_ledger.fact_demand_daily WHERE owner_key='owner1'"
        ).fetchone()[0]
        == 0
    )


def test_incremental_equals_clean_rebuild_at_same_observation_date(db):
    setup(db)
    changes(db, ["owner1", "owner2"])
    prepare(db, "r1")
    accept_candidate(db, "r1")
    changes(db, ["owner1"], geography="GEO_B", amount=30)
    prepare(db, "r2")
    accept_candidate(db, "r2")
    incremental = public(db, "accepted")
    # Independent full regroup from final owner contributions, not existing group totals.
    db.execute("CREATE SCHEMA final_rows")
    for n in FACTS:
        db.execute(f"CREATE TABLE final_rows.{n} AS SELECT * FROM accepted_ledger.{n}")
    for n in TABLES:
        db.execute("DELETE FROM accepted." + n)
    for n in FACTS:
        db.execute("DELETE FROM accepted_ledger." + n)
        db.execute("DELETE FROM changes." + n)
        db.execute(f"INSERT INTO changes.{n} SELECT * FROM final_rows.{n}")
    db.execute("DELETE FROM affected")
    db.execute("INSERT INTO affected VALUES ('owner1'),('owner2')")
    prepare(db, "clean")
    accept_candidate(db, "clean")
    assert public(db, "accepted") == incremental


def test_new_observation_day_preserves_past_status(db):
    setup(db)
    changes(db, ["owner1"])
    prepare(db, "r1")
    accept_candidate(db, "r1")
    changes(db, [])
    prepare(db, "r2", date(2026, 11, 8))
    accept_candidate(db, "r2")
    assert db.execute(
        "SELECT report_date,package_count FROM accepted.fact_status_daily ORDER BY 1"
    ).fetchall() == [(DAY, 1), (date(2026, 11, 8), 1)]
    assert db.execute("SELECT count(*) FROM accepted.fact_demand_daily").fetchone()[0] == 1


def test_invalid_dimensions_do_not_accept_candidate(db):
    setup(db)
    changes(db, ["owner1"])
    prepare(db, "r1")
    accept_candidate(db, "r1")
    before = public(db, "accepted")
    db.execute("INSERT INTO dimensions.dim_sector(sector_key) VALUES ('SEC_A')")
    db.execute("BEGIN")
    with pytest.raises(ValueError, match="duplicate logical key"):
        try:
            prepare(db, "r2")
        finally:
            db.execute("ROLLBACK")
    assert public(db, "accepted") == before


def test_unlisted_contribution_owner_rejected(db):
    setup(db)
    changes(db, ["owner1"])
    db.execute("DELETE FROM affected")
    with pytest.raises(ValueError, match="outside explicitly affected"):
        prepare(db, "bad")


def test_grain_hash_and_dimension_fanout_are_gates(db):
    setup(db)
    changes(db, ["owner1"])
    prepare(db, "r1")
    db.execute("UPDATE candidate_r1.fact_duration_daily SET group_key='bad'")
    with pytest.raises(ValueError, match="fixed fact grain"):
        validate(db, "candidate_r1")
