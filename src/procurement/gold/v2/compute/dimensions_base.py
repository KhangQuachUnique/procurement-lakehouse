"""Ported dimensions_base rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .sql import execute_sql


def execute(conn, context):
    ev_end = context.ev_end
    ev_start = context.ev_start
    rel_award_lot = context.rel_award_lot
    rel_not = context.rel_not
    rel_pkg = context.rel_pkg
    rel_pkg_loc = context.rel_pkg_loc
    rel_plan = context.rel_plan
    rel_res = context.rel_res
    release_config = context.release_config
    schema_prefix = context.schema_prefix
    execute_sql(
        conn,
        "dimensions_base_01.sql",
        {
            "p0": rel_pkg,
            "p1": rel_not,
            "p2": rel_res,
            "p3": rel_not,
            "p4": rel_res,
            "p5": rel_not,
            "p6": "UNION SELECT public_date FROM " + rel_plan + " WHERE public_date IS NOT NULL"
            if rel_plan
            else "",
        },
    )

    execute_sql(
        conn,
        "dimensions_base_02.sql",
        {"p0": schema_prefix, "p1": ev_start, "p2": ev_end, "p3": schema_prefix},
    )

    execute_sql(conn, "dimensions_base_03.sql", {})

    standard_currencies = [
        ("VND", "VND", True, False),
        ("USD", "USD", True, False),
        ("EUR", "EUR", True, False),
        ("CUR_UNKNOWN", "UNKNOWN", False, True),
    ]

    for c_key, c_code, c_supp, c_unk in standard_currencies:
        execute_sql(
            conn,
            "dimensions_base_04.sql",
            {
                "p0": schema_prefix,
                "p1": c_key,
                "p2": c_code,
                "p3": c_supp,
                "p4": c_unk,
                "p5": c_key,
                "p6": schema_prefix,
            },
        )

    execute_sql(
        conn,
        "dimensions_base_05.sql",
        {
            "p0": schema_prefix,
            "p1": rel_pkg,
            "p2": rel_not,
            "p3": rel_res,
            "p4": rel_award_lot,
            "p5": rel_not,
            "p6": rel_not,
            "p7": rel_not,
            "p8": schema_prefix,
        },
    )

    execute_sql(
        conn,
        "dimensions_base_06.sql",
        {
            "p0": schema_prefix,
            "p1": release_config.geography_version,
            "p2": schema_prefix,
            "p3": release_config.geography_version,
            "p4": schema_prefix,
            "p5": release_config.geography_version,
            "p6": schema_prefix,
            "p7": release_config.geography_version,
        },
    )

    if rel_pkg_loc:
        execute_sql(
            conn,
            "dimensions_base_07.sql",
            {
                "p0": schema_prefix,
                "p1": release_config.geography_version,
                "p2": rel_pkg_loc,
                "p3": schema_prefix,
                "p4": rel_pkg_loc,
                "p5": release_config.geography_version,
            },
        )

    execute_sql(
        conn,
        "dimensions_base_08.sql",
        {
            "p0": schema_prefix,
            "p1": release_config.taxonomy_version,
            "p2": schema_prefix,
            "p3": release_config.taxonomy_version,
            "p4": schema_prefix,
            "p5": release_config.taxonomy_version,
            "p6": rel_pkg,
            "p7": rel_not,
            "p8": schema_prefix,
            "p9": release_config.taxonomy_version,
        },
    )

    for vb in release_config.value_bands:
        max_str = (
            f"CAST('{vb.max_amount}' AS DECIMAL(38,6))" if vb.max_amount is not None else "NULL"
        )
        execute_sql(
            conn,
            "dimensions_base_09.sql",
            {
                "p0": schema_prefix,
                "p1": vb.band_key,
                "p2": vb.currency_key,
                "p3": release_config.policy_version,
                "p4": vb.min_amount,
                "p5": max_str,
                "p6": vb.band_label,
                "p7": vb.band_order,
                "p8": schema_prefix,
                "p9": vb.band_key,
                "p10": vb.currency_key,
                "p11": release_config.policy_version,
            },
        )

    execute_sql(
        conn,
        "dimensions_base_10.sql",
        {
            "p0": schema_prefix,
            "p1": release_config.policy_version,
            "p2": schema_prefix,
            "p3": release_config.policy_version,
        },
    )
