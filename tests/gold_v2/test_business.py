from procurement.gold.v2.aggregation import accept_candidate, prepare_candidate
from procurement.gold.v2.compute.engine import run_business
from procurement.gold.v2.compute.projection import project
from procurement.silver.domain.schema import get_silver_schemas
from tests.support.silver_business import populate_comprehensive_silver_fixtures


def populated(db):
    db.execute("CREATE SCHEMA silver")
    for name, ddl in get_silver_schemas().items():
        db.execute(f"CREATE TABLE silver.{name} ({ddl})")
    populate_comprehensive_silver_fixtures(db)


def test_ported_business_steps_generate_valid_16_table_candidate(db, plan):
    populated(db)
    steps = []
    counts = run_business(db, plan, source="silver", on_step=steps.append)
    assert len(steps) == 8 and counts["fact_procurement_demand"] > 0
    # Source versions 2 and 10 must rank numerically, not lexicographically.
    assert (
        db.execute(
            "SELECT planned_package_amount FROM business.fact_procurement_demand WHERE package_key='PKG:p12'"
        ).fetchone()[0]
        == 50000000
    )
    project(db, plan)
    db.execute(
        "CREATE TABLE affected AS "
        + " UNION ".join(
            "SELECT owner_key FROM changes." + n
            for n in (
                "fact_demand_daily",
                "fact_notice_daily",
                "fact_award_daily",
                "fact_savings_daily",
                "fact_duration_daily",
                "fact_supplier_daily",
                "fact_status_daily",
            )
        )
    )
    evidence = prepare_candidate(
        db,
        "r1",
        contributions="changes",
        affected="affected",
        dimensions="dimensions",
        observation_day=plan.observation_day,
    )
    assert evidence["verified"] and evidence["tables"] == 16
    accept_candidate(db, "r1")
    before = db.execute("SELECT * FROM accepted.fact_savings_daily ORDER BY ALL").fetchall()
    # Repeat same immutable inputs; no temp-table leftovers or double counting.
    run_business(db, plan, source="silver")
    project(db, plan)
    prepare_candidate(
        db,
        "r2",
        contributions="changes",
        affected="affected",
        dimensions="dimensions",
        observation_day=plan.observation_day,
    )
    accept_candidate(db, "r2")
    assert db.execute("SELECT * FROM accepted.fact_savings_daily ORDER BY ALL").fetchall() == before


def test_business_selection_and_core_metrics_preserved_against_baseline(db, plan):
    # Frozen legacy output is supplemental; explicit business assertions remain.
    import hashlib
    import json
    from pathlib import Path

    populated(db)
    run_business(db, plan, source="silver")
    folder = Path(__file__).parents[1] / "fixtures/gold_port_baseline"
    provenance = json.loads((folder / "provenance.json").read_text())
    for name, proof in provenance["tables"].items():
        path = folder / (name + ".parquet")
        assert hashlib.sha256(path.read_bytes()).hexdigest() == proof["sha256"]
        db.read_parquet(str(path)).create_view("expected", replace=True)
        mismatch = db.execute(
            f"SELECT count(*) FROM ((SELECT * EXCLUDE(created_at) FROM business.{name} EXCEPT ALL SELECT * FROM expected) UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT * EXCLUDE(created_at) FROM business.{name}))"
        ).fetchone()[0]
        assert mismatch == 0, name


def test_unresolved_round_classifications_use_explicit_unknown_buckets(db, plan):
    populated(db)
    run_business(db, plan, source="silver")
    # Reproduce a real unmatched round with no linked demand classifications.
    db.execute("UPDATE business.fact_selection_round SET sector_key=NULL,method_key=NULL")
    project(db, plan)
    assert (
        db.execute(
            "SELECT count(*) FROM changes.fact_duration_daily WHERE sector_key IS NULL OR method_key IS NULL"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM changes.fact_duration_daily WHERE sector_key='SEC_UNKNOWN' AND method_key='METHOD_UNKNOWN'"
        ).fetchone()[0]
        > 0
    )
