"""Explicit ordered business computation over eight local frozen source tables.

SQL was ported, not rederived from memory. Private detailed relations and bridges
are temporary computation/evidence, never the 16-table public contract.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from procurement.gold.domain.models import INPUT_TABLES, RunPlan, identifier

from . import (
    award,
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


STEPS = (dimensions_base, selection, dimensions_business, demand, notice, award, savings, duration)


def run_business(db, plan: RunPlan, source="frozen", on_step=None):
    identifier(source)
    for name in INPUT_TABLES:
        db.execute(f"SELECT * FROM {source}.{name} LIMIT 0")
    # Validate the exact relationships this business engine relies on.
    duplicate = db.execute(
        f"SELECT count(*) FROM (SELECT observation_id FROM {source}.observation GROUP BY 1 HAVING count(*)>1 OR observation_id IS NULL)"
    ).fetchone()[0]
    if duplicate:
        raise ValueError("Null/duplicate observation identity")
    for name in ("package_revision", "notice_revision", "result_revision", "plan_revision"):
        bad = db.execute(
            f"SELECT count(*) FROM {source}.{name} r LEFT JOIN {source}.observation o USING(observation_id) WHERE o.observation_id IS NULL OR o.observed_at IS NULL"
        ).fetchone()[0]
        if bad:
            raise ValueError("Missing observed lineage: " + name)
    db.execute("DROP SCHEMA IF EXISTS business CASCADE")
    db.execute("CREATE SCHEMA business")
    for name, ddl in json.loads(
        Path(__file__).with_name("private_schema.json").read_text()
    ).items():
        db.execute(f"CREATE TABLE business.{name} ({ddl})")
    context = Context(
        rel_pkg=f"{source}.package_revision",
        rel_not=f"{source}.notice_revision",
        rel_res=f"{source}.result_revision",
        rel_obs=f"{source}.observation",
        rel_award_lot=f"{source}.award_lot",
        rel_award_ctr=f"{source}.award_contractor",
        rel_pkg_loc=f"{source}.package_location",
        rel_plan=f"{source}.plan_revision",
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
        )
    }
