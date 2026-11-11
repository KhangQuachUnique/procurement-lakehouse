"""Ported savings rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .sql import execute_sql


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    ev_end = context.ev_end
    ev_start = context.ev_start
    rel_obs = context.rel_obs
    rel_pkg = context.rel_pkg
    release_config = context.release_config
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    execute_sql(
        conn,
        "savings_01.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": schema_prefix,
            "p3": release_id,
            "p4": schema_prefix,
            "p5": release_id,
            "p6": rel_obs,
            "p7": rel_pkg,
            "p8": rel_obs,
        },
    )

    execute_sql(conn, "savings_02.sql", {"p0": schema_prefix, "p1": release_id, "p2": release_id})

    execute_sql(conn, "savings_03.sql", {})

    rel_contract_rev = getattr(context, "rel_contract_rev", None) or "frozen.contract_revision"
    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_contract_savings AS
        SELECT
            r.revision_id AS result_revision_id,
            SUM(c.price_after_tax) AS contract_price
        FROM {rel_contract_rev} c
        JOIN _gtmp_cand_result r ON c.result_source_id = r.bid_id OR c.notice_no = r.notify_no
        WHERE c.price_after_tax > 0
        GROUP BY r.revision_id
    """)

    execute_sql(
        conn,
        "savings_04.sql",
        {
            "p0": schema_prefix,
            "p1": release_id,
            "p2": ev_start,
            "p3": ev_end,
            "p4": ev_start,
            "p5": ev_end,
            "p6": ev_end,
            "p7": ev_start,
            "p8": release_config.comparison_rule_version,
            "p9": build_ts_iso,
            "p10": schema_prefix,
            "p11": release_id,
        },
    )

    execute_sql(
        conn,
        "savings_05.sql",
        {"p0": schema_prefix, "p1": release_id, "p2": schema_prefix, "p3": release_id},
    )
