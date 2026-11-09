"""Ported selection rules; SQL assets preserved from vetted baseline, not runtime legacy imports."""

from .references import reference_join
from .sql import execute_sql


def execute(conn, context):
    obs_cutoff_pred = context.obs_cutoff_pred
    rel_not = context.rel_not
    rel_obs = context.rel_obs
    rel_pkg = context.rel_pkg
    rel_pkg_loc = context.rel_pkg_loc
    rel_plan = context.rel_plan
    rel_res = context.rel_res
    obs_join = f"LEFT JOIN {rel_obs} o ON p.observation_id = o.observation_id" if rel_obs else ""

    ver_expr = (
        "COALESCE(TRY_CAST(regexp_extract(o.source_version, '([0-9]+)', 1) AS BIGINT), 0)"
        if rel_obs
        else "0"
    )

    obs_time_expr = "o.observed_at" if rel_obs else "NULL"

    execute_sql(
        conn,
        "selection_01.sql",
        {
            "p0": ver_expr,
            "p1": obs_time_expr,
            "p2": ver_expr,
            "p3": obs_time_expr,
            "p4": ver_expr,
            "p5": obs_time_expr,
            "p6": rel_pkg,
            "p7": obs_join,
            "p8": obs_cutoff_pred,
        },
    )

    if rel_pkg_loc:
        execute_sql(conn, "selection_02.sql", {"p0": rel_pkg_loc})
    else:
        execute_sql(conn, "selection_03.sql", {})

    if rel_plan:
        plan_obs_join = (
            f"LEFT JOIN {rel_obs} o ON pl.observation_id = o.observation_id" if rel_obs else ""
        )
        execute_sql(
            conn, "selection_04.sql", {"p0": rel_plan, "p1": plan_obs_join, "p2": obs_cutoff_pred}
        )
    else:
        execute_sql(conn, "selection_05.sql", {})

    not_obs_join = (
        f"LEFT JOIN {rel_obs} o ON n.observation_id = o.observation_id" if rel_obs else ""
    )

    not_obs_time = "o.observed_at" if rel_obs else "NULL"

    execute_sql(
        conn,
        "selection_06.sql",
        {"p0": not_obs_time, "p1": rel_not, "p2": not_obs_join, "p3": obs_cutoff_pred},
    )

    res_obs_join = (
        f"LEFT JOIN {rel_obs} o ON r.observation_id = o.observation_id" if rel_obs else ""
    )

    res_obs_time = "o.observed_at" if rel_obs else "NULL"

    execute_sql(
        conn,
        "selection_07.sql",
        {
            "p0": res_obs_time,
            "p1": res_obs_time,
            "p2": rel_res,
            "p3": res_obs_join,
            "p4": obs_cutoff_pred,
        },
    )

    for source, id_expr, code_expr, output in [
        ("_gtmp_cand_result", "r.bid_id", "r.package_no", "_gtmp_result_package_match"),
        ("_gtmp_cand_notice", "r.package_id", "r.package_no", "_gtmp_notice_package_match"),
    ]:
        execute_sql(
            conn,
            "selection_08.sql",
            {
                "p0": output,
                "p1": id_expr,
                "p2": output,
                "p3": output,
                "p4": source,
                "p5": reference_join(
                    id_expr,
                    code_expr,
                    "_gtmp_cand_package",
                    "package_no",
                    "t.raw_package_id",
                    "resolved",
                ),
            },
        )
