from datetime import date

import pytest

from procurement.gold.v2.aggregation import accept_candidate
from procurement.gold.v2.queries import rollup_sql
from tests.gold_v2.test_aggregation import changes, prepare, setup


def test_nonmoney_facts_do_not_require_removed_currency_columns(db):
    setup(db)
    changes(db, ["owner1"])
    prepare(db, "r1")
    accept_candidate(db, "r1")
    assert db.execute(rollup_sql("fact_notice_daily", schema="accepted")).fetchall()
    assert db.execute(rollup_sql("fact_duration_daily", schema="accepted")).fetchall()
    with pytest.raises(ValueError, match="currencies"):
        rollup_sql("fact_savings_daily", dimensions=["sector_key"])
    with pytest.raises(ValueError, match="metrics"):
        rollup_sql("fact_duration_daily", dimensions=["sector_key"])


def test_status_rollup_selects_last_actual_observation_not_sum_of_days(db):
    setup(db)
    changes(db, ["owner1"])
    prepare(db, "r1")
    accept_candidate(db, "r1")
    changes(db, [])
    prepare(db, "r2", date(2026, 11, 8))
    accept_candidate(db, "r2")
    row = db.execute(
        rollup_sql("fact_status_daily", dimensions=["sector_key", "status_key"], schema="accepted")
    ).fetchone()
    assert row[-2:] == (1, date(2026, 11, 8))
