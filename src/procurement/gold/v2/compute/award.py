"""Ported award rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .sql import execute_sql


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    rel_award_ctr = context.rel_award_ctr
    rel_award_lot = context.rel_award_lot
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    execute_sql(conn, "award_01.sql", {"p0": rel_award_lot, "p1": rel_award_lot})

    execute_sql(conn, "award_02.sql", {"p0": rel_award_ctr, "p1": rel_award_ctr})

    execute_sql(conn, "award_03.sql", {})

    execute_sql(conn, "award_04.sql", {})

    execute_sql(
        conn,
        "award_05.sql",
        {
            "p0": release_id,
            "p1": release_id,
            "p2": release_id,
            "p3": release_id,
            "p4": release_id,
            "p5": schema_prefix,
            "p6": release_id,
        },
    )

    execute_sql(
        conn,
        "award_06.sql",
        {"p0": schema_prefix, "p1": release_id, "p2": release_id, "p3": release_id},
    )

    execute_sql(
        conn,
        "award_07.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": release_id,
            "p3": release_id,
            "p4": release_id,
        },
    )

    execute_sql(
        conn,
        "award_08.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": release_id,
            "p3": release_id,
            "p4": build_ts_iso,
            "p5": schema_prefix,
            "p6": release_id,
        },
    )

    execute_sql(conn, "award_09.sql", {"p0": schema_prefix, "p1": release_id})
