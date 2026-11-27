"""Ported competition rules for bid opening analysis."""


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    rel_open_rev = getattr(context, "rel_open_rev", None) or "frozen.opening_revision"
    rel_open_sub = getattr(context, "rel_open_sub", None) or "frozen.opening_submission"

    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_cand_opening AS
        WITH ranked AS (
            SELECT
                o.*,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(o.notify_no, o.entity_id)
                    ORDER BY o.public_date DESC NULLS LAST, o.revision_id DESC
                ) AS rn
            FROM {rel_open_rev} o
        )
        SELECT * FROM ranked WHERE rn = 1
    """)

    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_opening_pkg_ctx AS
        SELECT
            o.revision_id,
            COALESCE(npm.package_key, dem_pkg.package_key, 'PKG_UNRESOLVED:OPENING:' || o.entity_id) AS package_key,
            COALESCE(n.round_business_key, 'RND:NOT:VN:' || COALESCE(o.notify_no, o.entity_id)) AS round_business_key,
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
                THEN cn.bid_price
                ELSE NULL
            END AS ref_baseline
        FROM _gtmp_cand_opening o
        LEFT JOIN (
            SELECT * FROM _gtmp_cand_notice
            WHERE notify_no IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY notify_no
                ORDER BY numeric_ver DESC, published_at DESC NULLS LAST, public_date DESC NULLS LAST, revision_id DESC
            ) = 1
        ) cn ON o.notify_no = cn.notify_no
        LEFT JOIN _gtmp_notice_package_match npm ON cn.revision_id = npm.revision_id
        LEFT JOIN {schema_prefix}fact_tender_notice n
          ON cn.notice_business_key = n.notice_business_key AND n.release_id = '{release_id}'
        LEFT JOIN (
            SELECT source_id, MIN(package_key) AS package_key
            FROM {schema_prefix}dim_package
            WHERE release_id = '{release_id}' AND source_id IS NOT NULL
            GROUP BY source_id
        ) dp_id ON dp_id.source_id = o.package_id
        LEFT JOIN (
            SELECT package_no, MIN(package_key) AS package_key
            FROM {schema_prefix}dim_package
            WHERE release_id = '{release_id}' AND package_no IS NOT NULL
            GROUP BY package_no
            HAVING COUNT(*) = 1
        ) dp_no ON dp_no.package_no = o.package_no
        LEFT JOIN {schema_prefix}fact_procurement_demand dem_pkg
          ON dem_pkg.release_id = '{release_id}'
          AND dem_pkg.package_key = COALESCE(npm.package_key, dp_id.package_key, dp_no.package_key)
    """)

    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_opening_sub_agg AS
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
                    THEN norm_bid_price(sub.bid_price, ctx.ref_baseline)
                    ELSE NULL
                END AS clean_bid_price,
                CASE
                    WHEN sub.currency IS NULL OR TRIM(sub.currency) = '' OR UPPER(TRIM(sub.currency)) = 'VND'
                    THEN norm_bid_price(sub.final_bid_price, ctx.ref_baseline)
                    ELSE NULL
                END AS clean_final_bid_price
            FROM {rel_open_sub} sub
            LEFT JOIN _gtmp_opening_pkg_ctx ctx ON sub.revision_id = ctx.revision_id
        )
        SELECT
            revision_id,
            COUNT(DISTINCT COALESCE(contractor_code, contractor_name, submission_id)) AS total_bidders,
            COUNT(CASE WHEN tech_result = '1' THEN 1 END) AS tech_passed_count,
            COUNT(CASE WHEN tech_result = '0' THEN 1 END) AS tech_failed_count,
            COUNT(CASE WHEN discount_rate > 0 OR (clean_bid_price IS NOT NULL AND clean_final_bid_price IS NOT NULL AND clean_bid_price > clean_final_bid_price) THEN 1 END) AS discounted_bidder_count,
            COALESCE(SUM(COALESCE(clean_bid_price, 0)), 0) AS total_bid_amount,
            COALESCE(SUM(CASE WHEN clean_bid_price IS NOT NULL AND clean_final_bid_price IS NOT NULL AND clean_bid_price > clean_final_bid_price THEN clean_bid_price - clean_final_bid_price ELSE 0 END), 0) AS total_discount_amount,
            COALESCE(SUM(COALESCE(clean_final_bid_price, clean_bid_price, 0)), 0) AS total_final_bid_amount
        FROM normalized_sub
        GROUP BY revision_id
    """)

    conn.execute(f"""
        INSERT INTO {schema_prefix}fact_bidding_competition (
            release_id, opening_key, package_key, round_business_key,
            report_date, geography_key, sector_key, method_key, value_band_key,
            opening_count, total_bidders, single_bidder_count, multi_bidder_count,
            tech_passed_count, tech_failed_count, discounted_bidder_count,
            total_bid_amount, total_discount_amount, total_final_bid_amount,
            created_at, is_in_event_period
        )
        SELECT
            '{release_id}',
            'OPN:' || o.revision_id AS opening_key,
            ctx.package_key,
            ctx.round_business_key,
            COALESCE(CAST(o.actual_open_date AS DATE), o.public_date) AS report_date,
            ctx.geography_key,
            ctx.sector_key,
            ctx.method_key,
            ctx.value_band_key,
            1 AS opening_count,
            COALESCE(sub.total_bidders, o.total_bidders, 0) AS total_bidders,
            CASE WHEN COALESCE(sub.total_bidders, o.total_bidders, 0) = 1 THEN 1 ELSE 0 END AS single_bidder_count,
            CASE WHEN COALESCE(sub.total_bidders, o.total_bidders, 0) >= 2 THEN 1 ELSE 0 END AS multi_bidder_count,
            COALESCE(sub.tech_passed_count, 0) AS tech_passed_count,
            COALESCE(sub.tech_failed_count, 0) AS tech_failed_count,
            COALESCE(sub.discounted_bidder_count, 0) AS discounted_bidder_count,
            COALESCE(sub.total_bid_amount, 0) AS total_bid_amount,
            COALESCE(sub.total_discount_amount, 0) AS total_discount_amount,
            COALESCE(sub.total_final_bid_amount, 0) AS total_final_bid_amount,
            TIMESTAMPTZ '{build_ts_iso}' AS created_at,
            FALSE AS is_in_event_period
        FROM _gtmp_cand_opening o
        LEFT JOIN _gtmp_opening_pkg_ctx ctx ON o.revision_id = ctx.revision_id
        LEFT JOIN _gtmp_opening_sub_agg sub ON o.revision_id = sub.revision_id
    """)
