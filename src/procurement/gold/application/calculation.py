"""Durable complete-component batches for the same business engine in both modes."""

from pathlib import Path

from .aggregation import create_ledger
from .compute.engine import run_business
from .compute.labels import refresh_party_labels
from .compute.projection import project
from procurement.gold.domain.clustering.full_uf_planner import prepare_full
from procurement.gold.domain.clustering.planning import LINKS, ROOTS, affected_owners, plan_components, select_batch
from procurement.gold.domain.clustering.relationship_stage import prepare_stage
from procurement.gold.domain.schema import DIMENSIONS, FACTS, TABLES, create_tables


def calculate(db, plan, on_batch=None):
    db.execute(
        "CREATE TABLE IF NOT EXISTS control.calculations(run_id VARCHAR PRIMARY KEY,batches BIGINT,nodes BIGINT)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS control.completed_batches(run_id VARCHAR,batch BIGINT,PRIMARY KEY(run_id,batch))"
    )
    existing = db.execute(
        "SELECT batches,nodes FROM control.calculations WHERE run_id=?", [plan.run_id]
    ).fetchone()
    if existing:
        batches, nodes = existing
    else:
        prepare_stage(db, plan, ROOTS, LINKS)
        full_result = None
        if plan.mode == "backfill":
            path = db.execute(
                "SELECT path FROM duckdb_databases() WHERE database_name=current_database()"
            ).fetchone()[0]
            if not path:
                raise ValueError("Durable full Union-Find planning requires an on-disk state")
            full_result = prepare_full(db, plan, Path(path).parent / "uf-checkpoints" / plan.run_id)
        db.execute("BEGIN")
        try:
            accepted = db.execute("SELECT release_id FROM control.accepted").fetchone()
            result = full_result or plan_components(
                db,
                bootstrap=plan.mode == "backfill",
                batch_rows=plan.budget.batch_rows,
                cache_epoch=accepted[0] if accepted else None,
                stage_identity=plan.identity,
            )
            batches, nodes = result["batches"], result["affected_nodes"]
            affected_owners(db)
            db.execute("DROP SCHEMA IF EXISTS computed CASCADE")
            db.execute("DROP SCHEMA IF EXISTS computed_dimensions CASCADE")
            create_ledger(db, "computed")
            create_tables(db, "computed_dimensions")
            db.execute(
                "INSERT INTO control.calculations VALUES (?,?,?)", [plan.run_id, batches, nodes]
            )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    for batch in range(batches):
        if db.execute(
            "SELECT count(*) FROM control.completed_batches WHERE run_id=? AND batch=?",
            [plan.run_id, batch],
        ).fetchone()[0]:
            continue
        select_batch(db, batch)
        db.execute("BEGIN")
        try:
            run_business(db, plan, source="scoped")
            project(db, plan, retain_accepted=False)
            for name in FACTS:
                db.execute(f"INSERT INTO computed.{name} SELECT * FROM changes.{name}")
                # Include all newly resolved owners as well as old unresolved aliases.
                db.execute(
                    f"INSERT INTO affected SELECT DISTINCT owner_key FROM changes.{name} ON CONFLICT DO NOTHING"
                )
            for name in DIMENSIONS:
                db.execute(f"INSERT INTO computed_dimensions.{name} SELECT * FROM dimensions.{name}")
                if (batch + 1) % 50 == 0:
                    key = TABLES[name].key
                    db.execute(f"""CREATE OR REPLACE TABLE computed_dimensions.{name} AS
                      SELECT * FROM computed_dimensions.{name} t
                      QUALIFY row_number() OVER(PARTITION BY {key} ORDER BY to_json(t))=1""")
            db.execute("INSERT INTO control.completed_batches VALUES (?,?)", [plan.run_id, batch])
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        if on_batch:
            on_batch(batch + 1, batches)
    db.execute("DROP SCHEMA IF EXISTS changes CASCADE")
    create_ledger(db, "changes")
    db.execute("DROP SCHEMA IF EXISTS dimensions CASCADE")
    create_tables(db, "dimensions")
    has_accepted = db.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='accepted' AND table_name='dim_date'"
    ).fetchone()[0]
    for name in FACTS:
        db.execute(f"INSERT INTO changes.{name} SELECT * FROM computed.{name}")
    for name in DIMENSIONS:
        key = TABLES[name].key
        db.execute(f"""CREATE OR REPLACE TABLE computed_dimensions.{name} AS
          SELECT * FROM computed_dimensions.{name} t
          QUALIFY row_number() OVER(PARTITION BY {key} ORDER BY to_json(t))=1""")
        db.execute(f"INSERT INTO dimensions.{name} SELECT * FROM computed_dimensions.{name}")
        if has_accepted:
            db.execute(
                f"INSERT INTO dimensions.{name} SELECT * FROM accepted.{name} a WHERE NOT EXISTS (SELECT 1 FROM dimensions.{name} d WHERE a.{key}=d.{key})"
            )
    # Shared geography labels are source-wide, not component-local. Ensure all 65
    # canonical geography records are populated with official names and regions,
    # while allowing synthetic non-canonical test codes if present.
    from procurement.gold.domain.geography import ensure_province_map_table

    ensure_province_map_table(db)
    db.execute("""INSERT INTO dimensions.dim_geography
      SELECT cg.geography_key, 'v1', cg.province_code, cg.province_name, cg.region_name,
             cg.is_multi_geography, cg.is_unknown, NULL, NULL, TRUE
      FROM _gtmp_canonical_geography cg
      WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_geography d WHERE d.geography_key=cg.geography_key)""")
    db.execute("""UPDATE dimensions.dim_geography d
      SET province_code=cg.province_code,
          province_name=cg.province_name,
          region_name=cg.region_name,
          is_multi_geography=cg.is_multi_geography,
          is_unknown=cg.is_unknown,
          is_current=COALESCE(d.is_current, TRUE)
      FROM _gtmp_canonical_geography cg
      WHERE d.geography_key=cg.geography_key AND d.geography_version='v1'""")
    db.execute("""CREATE OR REPLACE TEMP TABLE global_geography AS
      SELECT 'GEO_'||upper(trim(pl.province_code)) geography_key,upper(trim(pl.province_code)) province_code,
        CASE WHEN count(DISTINCT nullif(trim(pl.province_name),''))=1 THEN min(nullif(trim(pl.province_name),'')) ELSE NULL END province_name
      FROM frozen.package_location pl
      LEFT JOIN _gtmp_province_map pm ON upper(trim(pl.province_code))=pm.raw_code
      WHERE nullif(trim(pl.province_code),'') IS NOT NULL AND pm.raw_code IS NULL
      GROUP BY upper(trim(pl.province_code))""")
    db.execute("""INSERT INTO dimensions.dim_geography SELECT geography_key,'v1',province_code,province_name,NULL,FALSE,FALSE,NULL,NULL,TRUE
      FROM global_geography g WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_geography d WHERE d.geography_key=g.geography_key)""")
    db.execute(
        "UPDATE dimensions.dim_geography d SET province_name=g.province_name FROM global_geography g WHERE d.geography_key=g.geography_key AND d.geography_version='v1'"
    )
    # Retract orphan supplier descriptors after source deletion. Keep current
    # source identities and any key referenced by remaining/new contributions.
    # Old dimension cache alone is not proof an organization is still present.
    has_ledger = db.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='accepted_ledger' AND table_name='fact_supplier_daily'"
    ).fetchone()[0]
    retained = "SELECT party_key FROM computed.fact_supplier_daily"
    if has_ledger:
        retained += " UNION SELECT party_key FROM accepted_ledger.fact_supplier_daily WHERE owner_key NOT IN (SELECT owner_key FROM affected)"
    db.execute(
        """DELETE FROM dimensions.dim_party p WHERE source_namespace='silver.award_contractor'
      AND NOT EXISTS (SELECT 1 FROM computed_dimensions.dim_party c WHERE c.party_key=p.party_key)
      AND p.party_key NOT IN ("""
        + retained
        + """)
      AND NOT EXISTS (SELECT 1 FROM frozen.award_contractor a WHERE
        p.party_key='PARTY:'||coalesce(nullif(trim(a.contractor_code),''),nullif(trim(a.tax_code),''))
        OR p.party_key='CON:'||nullif(trim(a.venture_code),'')
        OR p.party_key='PARTY_UNRESOLVED:business_v2:'||a.award_contractor_id)"""
    )
    refresh_party_labels(db)
    from procurement.gold.domain.policies import apply_dictionary

    apply_dictionary(db, plan)
    day = plan.observation_day
    db.execute(
        """INSERT INTO dimensions.dim_date SELECT ?,year(?),quarter(?),month(?),day(?),dayofweek(?),
      dayofweek(?) IN (0,6),year(?)||'-Q'||quarter(?),strftime(?,'%Y-%m'),extract(isoyear FROM ?),week(?),date_trunc('week',?)::DATE
      WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_date WHERE date_key=?)""",
        [day] * 14,
    )
    # Empty bootstrap still needs complete static dictionaries for valid readers.
    for metric, unit in (
        ("plan_to_notice_days", "days"),
        ("notice_to_close_hours", "hours"),
        ("close_to_first_award_days", "days"),
        ("close_to_complete_award_days", "days"),
        ("close_to_open_hours", "hours"),
        ("open_to_award_days", "days"),
        ("planned_to_actual_delay_days", "days"),
    ):
        db.execute(
            "INSERT INTO dimensions.dim_duration_metric SELECT ?,?,? WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_duration_metric WHERE metric_key=?)",
            [metric, metric, unit, metric],
        )
    return {
        "batches": batches,
        "affected_nodes": nodes,
        "source_strategy": "local_multiset_diff_after_frozen_capture",
    }
