"""Tier 1 Atomic Fact: ``fact_bidding_competition`` (grain = opening_key) with VND-only monetary guard."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement_spark.gold.core.dimensions_base import GoldBuildConfig
from procurement_spark.gold.functions import norm_bid_price_sql


def build_competition(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Compute ``core_fact_bidding_competition`` from opening_revision + opening_submission."""
    open_rev = f"{view_prefix}opening_revision"
    open_sub = f"{view_prefix}opening_submission"
    rel_id = config.release_id
    build_ts = f"{config.observation_day.isoformat()} 00:00:00"

    # 1. Deduplicate opening_revision per (notify_no / entity_id)
    spark.sql(
        f"""
        WITH ranked AS (
            SELECT
                o.*,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(o.notify_no, o.entity_id)
                    ORDER BY o.public_date DESC NULLS LAST, o.revision_id DESC
                ) AS rn
            FROM {open_rev} o
        )
        SELECT * FROM ranked WHERE rn = 1
        """
    ).createOrReplaceTempView("_gtmp_cand_opening")

    # 2. Resolve opening context (package, notice, geography, sector, method, ref_baseline)
    spark.sql(
        """
        WITH best_notice AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY notify_no
                        ORDER BY numeric_ver DESC, published_at DESC NULLS LAST, public_date DESC NULLS LAST, revision_id DESC
                    ) AS _notice_rn
                FROM _gtmp_cand_notice
                WHERE notify_no IS NOT NULL
            )
            WHERE _notice_rn = 1
        ),
        dp_id AS (
            SELECT source_id, MIN(package_key) AS package_key
            FROM core_dim_package
            WHERE source_id IS NOT NULL
            GROUP BY source_id
        ),
        dp_no AS (
            SELECT package_no, MIN(package_key) AS package_key
            FROM core_dim_package
            WHERE package_no IS NOT NULL
            GROUP BY package_no
            HAVING COUNT(*) = 1
        )
        SELECT
            o.revision_id,
            COALESCE(npm.package_key, dem_pkg.package_key, CONCAT('PKG_UNRESOLVED:OPENING:', o.entity_id)) AS package_key,
            COALESCE(n.round_business_key, CONCAT('RND:NOT:VN:', COALESCE(o.notify_no, o.entity_id))) AS round_business_key,
            COALESCE(
                CASE WHEN n.reporting_geography_key != 'GEO_UNKNOWN' THEN n.reporting_geography_key END,
                dem_pkg.reporting_geography_key,
                n.reporting_geography_key,
                'GEO_UNKNOWN'
            ) AS geography_key,
            COALESCE(
                CASE WHEN n.sector_key != 'SEC_UNKNOWN' THEN n.sector_key END,
                dem_pkg.sector_key,
                n.sector_key,
                'SEC_UNKNOWN'
            ) AS sector_key,
            COALESCE(
                CASE WHEN n.method_key != 'METHOD_UNKNOWN' THEN n.method_key END,
                dem_pkg.method_key,
                n.method_key,
                'METHOD_UNKNOWN'
            ) AS method_key,
            COALESCE(n.package_value_band_key, dem_pkg.package_value_band_key, 'BAND_RAW') AS value_band_key,
            CASE
                WHEN dem_pkg.planned_package_amount IS NOT NULL AND dem_pkg.planned_package_amount > 0
                THEN dem_pkg.planned_package_amount
                WHEN cn.bid_price IS NOT NULL AND cn.bid_price > 0
                THEN CAST(cn.bid_price AS DECIMAL(38,6))
                ELSE CAST(NULL AS DECIMAL(38,6))
            END AS ref_baseline
        FROM _gtmp_cand_opening o
        LEFT JOIN best_notice cn ON o.notify_no = cn.notify_no
        LEFT JOIN _gtmp_notice_package_match npm ON cn.revision_id = npm.revision_id
        LEFT JOIN core_fact_tender_notice n ON cn.notice_business_key = n.notice_business_key
        LEFT JOIN dp_id ON dp_id.source_id = o.package_id
        LEFT JOIN dp_no ON dp_no.package_no = o.package_no
        LEFT JOIN core_fact_procurement_demand dem_pkg
          ON dem_pkg.package_key = COALESCE(npm.package_key, dp_id.package_key, dp_no.package_key)
        """
    ).createOrReplaceTempView("_gtmp_opening_pkg_ctx")

    # 3. Aggregate submissions (VND/NULL-only monetary sums via norm_bid_price_sql; counts keep all currencies)
    norm_bid = norm_bid_price_sql("CAST(sub.bid_price AS DECIMAL(38,6))", "ctx.ref_baseline")
    norm_final = norm_bid_price_sql("CAST(sub.final_bid_price AS DECIMAL(38,6))", "ctx.ref_baseline")
    spark.sql(
        f"""
        WITH normalized_sub AS (
            SELECT
                sub.revision_id,
                sub.submission_id,
                sub.contractor_code,
                sub.contractor_name,
                sub.tech_result,
                sub.discount_rate,
                CASE
                    WHEN sub.currency IS NULL OR TRIM(sub.currency) = '' OR UPPER(TRIM(sub.currency)) = 'VND'
                    THEN {norm_bid}
                    ELSE CAST(NULL AS DECIMAL(38,6))
                END AS clean_bid_price,
                CASE
                    WHEN sub.currency IS NULL OR TRIM(sub.currency) = '' OR UPPER(TRIM(sub.currency)) = 'VND'
                    THEN {norm_final}
                    ELSE CAST(NULL AS DECIMAL(38,6))
                END AS clean_final_bid_price
            FROM {open_sub} sub
            LEFT JOIN _gtmp_opening_pkg_ctx ctx ON sub.revision_id = ctx.revision_id
        )
        SELECT
            revision_id,
            COUNT(DISTINCT COALESCE(contractor_code, contractor_name, submission_id)) AS total_bidders,
            COUNT(CASE WHEN tech_result = '1' THEN 1 END) AS tech_passed_count,
            COUNT(CASE WHEN tech_result = '0' THEN 1 END) AS tech_failed_count,
            COUNT(
                CASE
                    WHEN discount_rate > 0
                         OR (clean_bid_price IS NOT NULL AND clean_final_bid_price IS NOT NULL AND clean_bid_price > clean_final_bid_price)
                    THEN 1
                END
            ) AS discounted_bidder_count,
            CAST(COALESCE(SUM(COALESCE(clean_bid_price, CAST(0 AS DECIMAL(38,6)))), 0) AS DECIMAL(38,6)) AS total_bid_amount,
            CAST(
                COALESCE(
                    SUM(
                        CASE
                            WHEN clean_bid_price IS NOT NULL AND clean_final_bid_price IS NOT NULL
                                 AND clean_bid_price > clean_final_bid_price
                            THEN clean_bid_price - clean_final_bid_price
                            ELSE CAST(0 AS DECIMAL(38,6))
                        END
                    ),
                    0
                ) AS DECIMAL(38,6)
            ) AS total_discount_amount,
            CAST(
                COALESCE(SUM(COALESCE(clean_final_bid_price, clean_bid_price, CAST(0 AS DECIMAL(38,6)))), 0)
                AS DECIMAL(38,6)
            ) AS total_final_bid_amount
        FROM normalized_sub
        GROUP BY revision_id
        """
    ).createOrReplaceTempView("_gtmp_opening_sub_agg")

    # 4. fact_bidding_competition
    fact_comp = spark.sql(
        f"""
        SELECT
            '{rel_id}' AS release_id,
            CONCAT('OPN:', o.revision_id) AS opening_key,
            ctx.package_key,
            ctx.round_business_key,
            COALESCE(CAST(o.actual_open_date AS DATE), o.public_date) AS report_date,
            ctx.geography_key,
            ctx.sector_key,
            ctx.method_key,
            ctx.value_band_key,
            CAST(1 AS BIGINT) AS opening_count,
            CAST(COALESCE(sub.total_bidders, o.total_bidders, 0) AS BIGINT) AS total_bidders,
            CAST(CASE WHEN COALESCE(sub.total_bidders, o.total_bidders, 0) = 1 THEN 1 ELSE 0 END AS BIGINT) AS single_bidder_count,
            CAST(CASE WHEN COALESCE(sub.total_bidders, o.total_bidders, 0) >= 2 THEN 1 ELSE 0 END AS BIGINT) AS multi_bidder_count,
            CAST(COALESCE(sub.tech_passed_count, 0) AS BIGINT) AS tech_passed_count,
            CAST(COALESCE(sub.tech_failed_count, 0) AS BIGINT) AS tech_failed_count,
            CAST(COALESCE(sub.discounted_bidder_count, 0) AS BIGINT) AS discounted_bidder_count,
            COALESCE(sub.total_bid_amount, CAST(0 AS DECIMAL(38,6))) AS total_bid_amount,
            COALESCE(sub.total_discount_amount, CAST(0 AS DECIMAL(38,6))) AS total_discount_amount,
            COALESCE(sub.total_final_bid_amount, CAST(0 AS DECIMAL(38,6))) AS total_final_bid_amount,
            CAST('{build_ts}' AS TIMESTAMP) AS created_at
        FROM _gtmp_cand_opening o
        LEFT JOIN _gtmp_opening_pkg_ctx ctx ON o.revision_id = ctx.revision_id
        LEFT JOIN _gtmp_opening_sub_agg sub ON o.revision_id = sub.revision_id
        """
    )
    fact_comp.createOrReplaceTempView("core_fact_bidding_competition")

    return {"fact_bidding_competition": fact_comp}
