"""Ported duration rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .sql import execute_sql


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    execute_sql(
        conn,
        "duration_01.sql",
        {"p0": schema_prefix, "p1": schema_prefix, "p2": release_id, "p3": release_id},
    )

    execute_sql(conn, "duration_02.sql", {"p0": schema_prefix, "p1": release_id})

    execute_sql(
        conn,
        "duration_03.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": build_ts_iso,
            "p3": schema_prefix,
            "p4": release_id,
        },
    )

    execute_sql(
        conn,
        "duration_04.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": release_id,
            "p3": schema_prefix,
            "p4": release_id,
        },
    )
