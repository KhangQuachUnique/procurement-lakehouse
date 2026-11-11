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

    rel_open_rev = getattr(context, "rel_open_rev", None) or "frozen.opening_revision"
    rel_contract_rev = getattr(context, "rel_contract_rev", None) or "frozen.contract_revision"
    rel_contract_perf = getattr(context, "rel_contract_perf", None) or "frozen.contract_performance_revision"

    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_round_opening_endpoints AS
        SELECT
            'RND:NOT:VN:' || o.notify_no AS round_business_key,
            MIN(CAST(COALESCE(o.actual_open_date, o.scheduled_open_date) AS DATE)) AS open_date,
            MIN(COALESCE(o.actual_open_date, o.scheduled_open_date)) AS open_at
        FROM {rel_open_rev} o
        WHERE o.notify_no IS NOT NULL
        GROUP BY 'RND:NOT:VN:' || o.notify_no
    """)

    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_round_contract_endpoints AS
        SELECT
            'RND:NOT:VN:' || c.notice_no AS round_business_key,
            MIN(cp.public_date) AS contract_complete_date,
            MIN(cp.planned_completion_date) AS planned_completion_date,
            MAX(cp.actual_completion_date) AS actual_completion_date
        FROM {rel_contract_perf} cp
        JOIN {rel_contract_rev} c ON cp.contract_code = c.contract_code
        WHERE c.notice_no IS NOT NULL
        GROUP BY 'RND:NOT:VN:' || c.notice_no
    """)

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
