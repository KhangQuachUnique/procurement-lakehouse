"""Ported notice rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .sql import execute_sql


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    release_config = context.release_config
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    execute_sql(
        conn,
        "notice_01.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": release_config.mapping_version,
            "p3": release_id,
            "p4": release_id,
            "p5": build_ts_iso,
        },
    )

    execute_sql(conn, "notice_02.sql", {"p0": schema_prefix, "p1": release_id})
