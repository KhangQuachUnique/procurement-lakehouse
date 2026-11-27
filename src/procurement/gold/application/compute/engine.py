"""Explicit ordered business computation over local frozen source tables.

SQL was ported, not rederived from memory. Private detailed relations and bridges
are temporary computation/evidence, never the 17-table public contract.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from procurement.gold.application.silver_adapter import prepare_business_source
from procurement.gold.domain.models import INPUT_TABLES, RunPlan, identifier

from . import (
    award,
    competition,
    demand,
    dimensions_base,
    dimensions_business,
    duration,
    notice,
    savings,
    selection,
)


@dataclass(frozen=True)
class BusinessConfig:
    # Only the currently implemented baseline policy is accepted. No invented mappings.
    geography_version: str = "v1"
    taxonomy_version: str = "v1"
    policy_version: str = "v1"
    mapping_version: str = "v1"
    comparison_rule_version: str = "v1"
    value_bands: tuple = ()


@dataclass(frozen=True)
class Context:
    rel_pkg: str
    rel_not: str
    rel_res: str
    rel_obs: str
    rel_award_lot: str
    rel_award_ctr: str
    rel_pkg_loc: str
    rel_plan: str
    release_config: BusinessConfig
    ev_start: str
    ev_end: str
    build_ts_iso: str
    release_id: str = "business_v2"
    schema_prefix: str = "business."
    obs_cutoff_pred: str = "TRUE"
    rel_open_rev: str = ""
    rel_open_sub: str = ""
    rel_contract_rev: str = ""
    rel_contract_perf: str = ""
    rel_not_loc: str = ""


STEPS = (
    dimensions_base,
    selection,
    dimensions_business,
    demand,
    notice,
    award,
    savings,
    duration,
    competition,
)


def run_business(db, plan: RunPlan, source="frozen", on_step=None):
    identifier(source)
    effective_source = prepare_business_source(db, source)
    try:
        for name in INPUT_TABLES:
            db.execute(f"SELECT * FROM {effective_source}.{name} LIMIT 0")
        # Validate the exact relationships this business engine relies on.
        duplicate = db.execute(
            f"SELECT count(*) FROM (SELECT observation_id FROM {effective_source}.observation GROUP BY 1 HAVING count(*)>1 OR observation_id IS NULL)"
        ).fetchone()[0]
        if duplicate:
            raise ValueError("Null/duplicate observation identity")
        for name in ("package_revision", "notice_revision", "result_revision", "plan_revision"):
            bad = db.execute(
                f"SELECT count(*) FROM {effective_source}.{name} r LEFT JOIN {effective_source}.observation o USING(observation_id) WHERE o.observation_id IS NULL OR o.observed_at IS NULL"
            ).fetchone()[0]
            if bad:
                raise ValueError("Missing observed lineage: " + name)
        db.execute("DROP SCHEMA IF EXISTS business CASCADE")
        db.execute("CREATE SCHEMA business")
        db.execute("""
            CREATE OR REPLACE MACRO norm_bid_price(raw_val, ref_baseline) AS (
                CASE
                    WHEN raw_val IS NULL OR raw_val <= 0 THEN NULL
                    WHEN ref_baseline IS NOT NULL AND raw_val <= ref_baseline * 2 THEN raw_val
                    WHEN ref_baseline IS NOT NULL AND raw_val > ref_baseline * 2 THEN (
                        CASE
                            WHEN FLOOR(raw_val / 1000000.0) BETWEEN ref_baseline * 0.2 AND ref_baseline * 1.5
                            THEN CAST(FLOOR(raw_val / 1000000.0) AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 1000.0) BETWEEN ref_baseline * 0.2 AND ref_baseline * 1.5
                            THEN CAST(FLOOR(raw_val / 1000.0) AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 100.0) BETWEEN ref_baseline * 0.2 AND ref_baseline * 1.5
                            THEN CAST(FLOOR(raw_val / 100.0) AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 10.0) BETWEEN ref_baseline * 0.2 AND ref_baseline * 1.5
                            THEN CAST(FLOOR(raw_val / 10.0) AS DECIMAL(38,6))
                            ELSE NULL
                        END
                    )
                    WHEN ref_baseline IS NULL AND raw_val > 10000000000000 AND FLOOR(raw_val / 1000.0) <= 500000000000
                    THEN CAST(FLOOR(raw_val / 1000.0) AS DECIMAL(38,6))
                    WHEN raw_val <= 50000000000000 THEN raw_val
                    ELSE NULL
                END
            );
            CREATE OR REPLACE MACRO recover_award_amount(raw_val, ref_baseline) AS (
                CASE
                    WHEN raw_val IS NULL OR raw_val <= 0 THEN raw_val
                    WHEN ref_baseline IS NOT NULL AND raw_val <= ref_baseline * 2 THEN raw_val
                    WHEN ref_baseline IS NOT NULL AND raw_val > ref_baseline * 2 THEN (
                        CASE
                            WHEN FLOOR(raw_val / 1000000.0) = ref_baseline
                                 AND ((CAST(raw_val AS BIGINT) % 1000000) * 1000) BETWEEN ref_baseline * 0.3 AND ref_baseline * 1.0
                            THEN CAST((CAST(raw_val AS BIGINT) % 1000000) * 1000 AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 1000000.0) BETWEEN ref_baseline * 0.3 AND ref_baseline * 1.2
                            THEN CAST(FLOOR(raw_val / 1000000.0) AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 1000.0) BETWEEN ref_baseline * 0.3 AND ref_baseline * 1.2
                            THEN CAST(FLOOR(raw_val / 1000.0) AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 100.0) BETWEEN ref_baseline * 0.3 AND ref_baseline * 1.2
                            THEN CAST(FLOOR(raw_val / 100.0) AS DECIMAL(38,6))
                            WHEN FLOOR(raw_val / 10.0) BETWEEN ref_baseline * 0.3 AND ref_baseline * 1.2
                            THEN CAST(FLOOR(raw_val / 10.0) AS DECIMAL(38,6))
                            WHEN CAST(ref_baseline AS BIGINT) >= 1000000
                                 AND starts_with(CAST(CAST(raw_val AS BIGINT) AS VARCHAR), CAST(CAST(ref_baseline AS BIGINT) AS VARCHAR))
                            THEN ref_baseline
                            ELSE raw_val
                        END
                    )
                    WHEN ref_baseline IS NULL
                         AND raw_val > 10000000000000
                         AND (CAST(FLOOR(raw_val / 1000.0) AS BIGINT) % 1000) = 0
                         AND FLOOR(raw_val / 1000.0) <= 500000000000
                    THEN CAST(FLOOR(raw_val / 1000.0) AS DECIMAL(38,6))
                    ELSE raw_val
                END
            );
        """)
        for name, ddl in json.loads(
            Path(__file__).with_name("private_schema.json").read_text()
        ).items():
            db.execute(f"CREATE TABLE business.{name} ({ddl})")
        has_not_loc = db.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema=? AND table_name='notice_location'",
            [effective_source],
        ).fetchone()[0]
        context = Context(
            rel_pkg=f"{effective_source}.package_revision",
            rel_not=f"{effective_source}.notice_revision",
            rel_res=f"{effective_source}.result_revision",
            rel_obs=f"{effective_source}.observation",
            rel_award_lot=f"{effective_source}.award_lot",
            rel_award_ctr=f"{effective_source}.award_contractor",
            rel_pkg_loc=f"{effective_source}.package_location",
            rel_plan=f"{effective_source}.plan_revision",
            rel_open_rev=f"{effective_source}.opening_revision",
            rel_open_sub=f"{effective_source}.opening_submission",
            rel_contract_rev=f"{effective_source}.contract_revision",
            rel_contract_perf=f"{effective_source}.contract_performance_revision",
            rel_not_loc=f"{effective_source}.notice_location" if has_not_loc else "",
            release_config=BusinessConfig(),
            ev_start=plan.event_start.isoformat(),
            ev_end=plan.event_end.isoformat(),
            build_ts_iso=datetime.combine(
                plan.observation_day, datetime.min.time(), tzinfo=UTC
            ).isoformat(),
        )
        # Temp SQL tables have connection lifetime; failed/repeated calculations
        # must not inherit old candidate rows. Names are internal, not catalog roots.
        for (name,) in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog='temp' AND starts_with(table_name,'_gtmp_')"
        ).fetchall():
            db.execute(f"DROP TABLE {identifier(name)}")
        for step in STEPS:
            step.execute(db, context)
            if on_step:
                on_step(step.__name__.rsplit(".", 1)[-1])
        for name, day in (
            ("fact_procurement_demand", "package_published_date_key"),
            ("fact_tender_notice", "published_date_key"),
            ("fact_award", "award_published_date_key"),
            ("fact_savings", "comparison_date_key"),
            ("fact_selection_round", "first_notice_date_key"),
            ("fact_bidding_competition", "report_date"),
        ):
            db.execute(
                f"UPDATE business.{name} SET is_in_event_period=coalesce({day} BETWEEN ? AND ?,FALSE)",
                [plan.event_start, plan.event_end],
            )
        for fact, key in (
            ("fact_procurement_demand", "package_key"),
            ("fact_tender_notice", "notice_business_key"),
            ("fact_award", "award_business_key"),
            ("fact_savings", "comparison_scope_key"),
            ("fact_selection_round", "round_business_key"),
            ("fact_bidding_competition", "opening_key"),
        ):
            bad = db.execute(
                f"SELECT count(*) FROM (SELECT {key} FROM business.{fact} GROUP BY 1 HAVING count(*)>1 OR {key} IS NULL)"
            ).fetchone()[0]
            owners = db.execute(
                f"SELECT count(*) FROM business.{fact} WHERE package_key IS NULL OR trim(package_key)=''"
            ).fetchone()[0]
            if bad or owners:
                raise ValueError("Business grain or retractable owner failure: " + fact)
        for (name,) in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog='temp' AND starts_with(table_name,'_gtmp_')"
        ).fetchall():
            db.execute(f"DROP TABLE {identifier(name)}")
        return {
            n: db.execute("SELECT count(*) FROM business." + n).fetchone()[0]
            for n in (
                "fact_procurement_demand",
                "fact_tender_notice",
                "fact_award",
                "fact_savings",
                "fact_selection_round",
                "fact_bidding_competition",
            )
        }
    finally:
        if effective_source != source:
            db.execute(f"DROP SCHEMA IF EXISTS {effective_source} CASCADE")
