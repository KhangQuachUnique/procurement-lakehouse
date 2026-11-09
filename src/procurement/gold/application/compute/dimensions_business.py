"""Ported dimensions_business rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .references import reference_join
from .sql import execute_sql


def execute(conn, context):
    rel_award_lot = context.rel_award_lot
    release_config = context.release_config
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    execute_sql(
        conn,
        "dimensions_business_01.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": reference_join(
                "p.plan_id", "p.plan_no", "_gtmp_cand_plan", "plan_no", "t.raw_plan_id", "pl"
            ),
            "p3": schema_prefix,
            "p4": release_id,
            "p5": schema_prefix,
            "p6": release_id,
        },
    )

    execute_sql(
        conn,
        "dimensions_business_02.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": schema_prefix,
            "p3": release_id,
            "p4": release_id,
            "p5": rel_award_lot,
        },
    )

    execute_sql(
        conn,
        "dimensions_business_03.sql",
        {"p0": schema_prefix, "p1": release_id, "p2": rel_award_lot},
    )

    execute_sql(
        conn,
        "dimensions_business_04.sql",
        {
            "p0": schema_prefix,
            "p1": release_config.mapping_version,
            "p2": schema_prefix,
            "p3": release_config.mapping_version,
            "p4": schema_prefix,
            "p5": release_config.mapping_version,
            "p6": release_config.mapping_version,
            "p7": schema_prefix,
            "p8": release_config.mapping_version,
            "p9": release_config.mapping_version,
        },
    )
