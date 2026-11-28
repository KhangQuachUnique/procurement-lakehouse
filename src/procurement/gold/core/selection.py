"""Current-revision selection and cardinality-safe entity linkage on Spark SQL.

Reads directly from ``silver_*`` TempViews (no synthetic ``observation`` table) and
registers reusable TempViews:
  - ``_gtmp_cand_package``, ``_gtmp_cand_plan``, ``_gtmp_cand_notice``, ``_gtmp_cand_result``
  - ``_gtmp_pkg_geography``, ``_gtmp_notice_geography``
  - ``_gtmp_result_package_match``, ``_gtmp_notice_package_match``
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def reference_join_sql(
    source_id: str,
    source_code: str,
    target: str,
    code_column: str,
    raw_id: str,
    alias: str,
) -> str:
    """Cardinality-safe relational join: exact ID first, unique business code second, never ambiguous."""
    return f"""
    LEFT JOIN (
        SELECT reference_id, COUNT(DISTINCT entity_id) AS cnt, MIN(entity_id) AS entity_id
        FROM (
            SELECT t.entity_id AS reference_id, t.entity_id FROM {target} t
            UNION ALL
            SELECT {raw_id} AS reference_id, t.entity_id FROM {target} t
        ) identifiers
        WHERE NULLIF(TRIM(reference_id), '') IS NOT NULL
        GROUP BY reference_id
    ) {alias}_id ON {alias}_id.reference_id = {source_id}
    LEFT JOIN (
        SELECT t.{code_column} AS business_code, COUNT(DISTINCT t.entity_id) AS cnt,
               MIN(t.entity_id) AS entity_id
        FROM {target} t
        WHERE NULLIF(TRIM(t.{code_column}), '') IS NOT NULL
        GROUP BY t.{code_column}
    ) {alias}_code ON {alias}_code.business_code = {source_code}
    LEFT JOIN {target} {alias} ON {alias}.entity_id = CASE
        WHEN {alias}_id.cnt = 1 THEN {alias}_id.entity_id
        WHEN COALESCE({alias}_id.cnt, 0) = 0 AND {alias}_code.cnt = 1 THEN {alias}_code.entity_id
        ELSE NULL
    END
    """


def _build_geography_rollup(spark: SparkSession, source_table: str, view_name: str) -> DataFrame:
    df = spark.sql(
        f"""
        WITH normalized AS (
            SELECT
                loc.revision_id,
                COALESCE(pm.is_multi, FALSE) AS is_multi,
                COALESCE(
                    pm.canonical_code,
                    CASE WHEN pm.raw_code IS NULL THEN UPPER(TRIM(loc.province_code)) ELSE NULL END
                ) AS resolved_code
            FROM {source_table} loc
            LEFT JOIN _gtmp_province_map pm ON UPPER(TRIM(loc.province_code)) = pm.raw_code
            WHERE loc.province_code IS NOT NULL AND TRIM(loc.province_code) != ''
        )
        SELECT
            revision_id,
            CASE
                WHEN MAX(CAST(is_multi AS INT)) = 1 THEN CAST(2 AS BIGINT)
                ELSE COUNT(DISTINCT resolved_code)
            END AS prov_count,
            MIN(resolved_code) AS single_province_code,
            CASE
                WHEN SIZE(COLLECT_SET(resolved_code)) > 0
                THEN ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(resolved_code)), ',')
                WHEN MAX(CAST(is_multi AS INT)) = 1 THEN 'MULTI_824'
                ELSE 'UNKNOWN'
            END AS loc_hash
        FROM normalized
        GROUP BY revision_id
        """
    )
    df.createOrReplaceTempView(view_name)
    return df


def build_selection_views(
    spark: SparkSession,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Deduplicate current Silver revisions and resolve package linkage."""
    pkg = f"{view_prefix}package_revision"
    pkg_loc = f"{view_prefix}package_location"
    plan = f"{view_prefix}plan_revision"
    not_tbl = f"{view_prefix}notice_revision"
    not_loc = f"{view_prefix}notice_location"
    res_tbl = f"{view_prefix}result_revision"

    # 1. Current package_revision per entity_id
    cand_package = spark.sql(
        f"""
        WITH ranked AS (
            SELECT
                p.*,
                COALESCE(NULLIF(TRIM(p.source_id), ''), p.entity_id) AS raw_package_id,
                CAST(0 AS BIGINT) AS numeric_ver,
                p.ingested_at AS observed_at_provenance,
                p.source_id AS evidence_source_id,
                p.source_date AS evidence_source_date,
                ROW_NUMBER() OVER (
                    PARTITION BY p.entity_id
                    ORDER BY
                        p.ingested_at DESC NULLS LAST,
                        p.public_date DESC NULLS LAST,
                        p.revision_id DESC
                ) AS rn,
                COUNT(*) OVER (
                    PARTITION BY p.entity_id, p.ingested_at
                ) AS tie_count,
                (
                    MIN(p.bid_price) OVER (PARTITION BY p.entity_id)
                    != MAX(p.bid_price) OVER (PARTITION BY p.entity_id)
                ) AS price_conflict
            FROM {pkg} p
        )
        SELECT
            *,
            CASE
                WHEN tie_count > 1 AND COALESCE(price_conflict, FALSE) THEN 'tie_conflict'
                ELSE 'resolved_deterministic'
            END AS package_resolution_state
        FROM ranked
        WHERE rn = 1
        """
    )
    cand_package.createOrReplaceTempView("_gtmp_cand_package")

    # 2. Package & Notice geography rollups
    pkg_geo = _build_geography_rollup(spark, pkg_loc, "_gtmp_pkg_geography")
    not_geo = _build_geography_rollup(spark, not_loc, "_gtmp_notice_geography")

    # 3. Current plan_revision per entity_id
    cand_plan = spark.sql(
        f"""
        WITH ranked AS (
            SELECT
                pl.*,
                NULLIF(TRIM(pl.source_id), '') AS raw_plan_id,
                ROW_NUMBER() OVER (
                    PARTITION BY pl.entity_id
                    ORDER BY pl.public_date DESC NULLS LAST, pl.revision_id DESC
                ) AS rn
            FROM {plan} pl
        )
        SELECT * FROM ranked WHERE rn = 1
        """
    )
    cand_plan.createOrReplaceTempView("_gtmp_cand_plan")

    # 4. Current notice_revision per (source_family, notify_no)
    cand_notice = spark.sql(
        f"""
        WITH ranked AS (
            SELECT
                n.*,
                COALESCE(n.source_family, 'VN') AS notice_family_clean,
                COALESCE(TRY_CAST(REGEXP_EXTRACT(COALESCE(n.notify_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) AS numeric_ver,
                n.ingested_at AS obs_time,
                n.source_version AS evidence_source_version,
                n.source_date AS evidence_source_date,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(n.source_family, 'VN'), COALESCE(n.notify_no, n.entity_id)
                    ORDER BY
                        COALESCE(TRY_CAST(REGEXP_EXTRACT(COALESCE(n.notify_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) DESC,
                        n.published_at DESC NULLS LAST,
                        n.public_date DESC NULLS LAST,
                        n.revision_id DESC
                ) AS rn
            FROM {not_tbl} n
        )
        SELECT
            *,
            CONCAT('NOT:', notice_family_clean, ':', COALESCE(notify_no, entity_id)) AS notice_business_key,
            CONCAT('RND:NOT:', notice_family_clean, ':', COALESCE(notify_no, entity_id)) AS round_business_key
        FROM ranked
        WHERE rn = 1
        """
    )
    cand_notice.createOrReplaceTempView("_gtmp_cand_notice")

    # 5. Current result_revision per (result_id, entity_id)
    cand_result = spark.sql(
        f"""
        WITH ranked AS (
            SELECT
                r.*,
                COALESCE(TRY_CAST(REGEXP_EXTRACT(COALESCE(r.result_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) AS numeric_ver,
                r.ingested_at AS obs_time,
                r.source_version AS evidence_source_version,
                r.source_date AS evidence_source_date,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(r.result_id, r.entity_id)
                    ORDER BY
                        COALESCE(TRY_CAST(REGEXP_EXTRACT(COALESCE(r.result_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) DESC,
                        r.ingested_at DESC NULLS LAST,
                        r.public_date DESC NULLS LAST,
                        r.revision_id DESC
                ) AS rn
            FROM {res_tbl} r
        )
        SELECT * FROM ranked WHERE rn = 1
        """
    )
    cand_result.createOrReplaceTempView("_gtmp_cand_result")

    # 6. Cardinality-safe package resolution for Results and Notices
    matches: dict[str, DataFrame] = {}
    for src_view, id_expr, code_expr, out_view, prefix in (
        ("_gtmp_cand_result", "r.bid_id", "r.package_no", "_gtmp_result_package_match", "RESULT"),
        ("_gtmp_cand_notice", "r.package_id", "r.package_no", "_gtmp_notice_package_match", "NOTICE"),
    ):
        join_sql = reference_join_sql(
            id_expr,
            code_expr,
            "_gtmp_cand_package",
            "package_no",
            "t.raw_package_id",
            "resolved",
        )
        match_df = spark.sql(
            f"""
            SELECT
                r.revision_id,
                r.entity_id AS result_entity_id,
                {id_expr} AS bid_id,
                r.package_no AS res_package_no,
                r.notify_no AS res_notify_no,
                CASE
                    WHEN resolved.entity_id IS NOT NULL THEN CONCAT('PKG:', resolved.entity_id)
                    WHEN COALESCE(resolved_id.cnt, 0) > 1 OR COALESCE(resolved_code.cnt, 0) > 1
                    THEN CONCAT('PKG_AMBIGUOUS:{prefix}:', r.entity_id)
                    ELSE CONCAT('PKG_UNRESOLVED:{prefix}:', r.entity_id)
                END AS package_key,
                CASE
                    WHEN COALESCE(resolved_id.cnt, 0) = 1 THEN 'resolved_exact_source_id'
                    WHEN COALESCE(resolved_id.cnt, 0) > 1 THEN 'ambiguous_source_id'
                    WHEN COALESCE(resolved_code.cnt, 0) = 1 THEN 'resolved_unique_business_code'
                    WHEN COALESCE(resolved_code.cnt, 0) > 1 THEN 'ambiguous_business_code'
                    ELSE 'unresolved_package'
                END AS package_resolution_state,
                CASE WHEN COALESCE(resolved_id.cnt, 0) = 1 THEN resolved.entity_id END AS direct_package_entity_id,
                CASE WHEN COALESCE(resolved_id.cnt, 0) = 0 AND COALESCE(resolved_code.cnt, 0) = 1 THEN resolved.entity_id END AS unique_code_package_entity_id,
                resolved.entity_id AS resolved_entity_id
            FROM {src_view} r
            {join_sql}
            """
        )
        match_df.createOrReplaceTempView(out_view)
        matches[out_view] = match_df

    return {
        "_gtmp_cand_package": cand_package,
        "_gtmp_pkg_geography": pkg_geo,
        "_gtmp_notice_geography": not_geo,
        "_gtmp_cand_plan": cand_plan,
        "_gtmp_cand_notice": cand_notice,
        "_gtmp_cand_result": cand_result,
        **matches,
    }
