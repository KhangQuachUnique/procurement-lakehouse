"""Ported demand rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .references import reference_join
from .sql import execute_sql


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    rel_pkg_loc = context.rel_pkg_loc
    release_config = context.release_config
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    execute_sql(
        conn,
        "demand_01.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": release_config.mapping_version,
            "p3": release_id,
            "p4": build_ts_iso,
            "p5": reference_join(
                "p.plan_id", "p.plan_no", "_gtmp_cand_plan", "plan_no", "t.raw_plan_id", "pl"
            ),
        },
    )

    execute_sql(
        conn,
        "demand_02.sql",
        {"p0": schema_prefix, "p1": release_id, "p2": schema_prefix, "p3": release_id},
    )

    if rel_pkg_loc:
        execute_sql(
            conn, "demand_03.sql", {"p0": schema_prefix, "p1": release_id, "p2": rel_pkg_loc}
        )

    execute_sql(conn, "demand_04.sql", {"p0": schema_prefix, "p1": release_id})
