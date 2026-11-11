"""Ported competition rules for bid opening analysis."""


def execute(conn, context):
    build_ts_iso = context.build_ts_iso
    release_id = context.release_id
    schema_prefix = context.schema_prefix
    rel_open_rev = getattr(context, "rel_open_rev", None) or "frozen.opening_revision"
    rel_open_sub = getattr(context, "rel_open_sub", None) or "frozen.opening_submission"

    conn.execute(f"""
        CREATE TEMP TABLE _gtmp_opening_sub_agg AS
        SELECT
            sub.revision_id,
            COUNT(DISTINCT COALESCE(sub.contractor_code, sub.contractor_name, sub.submission_id)) AS total_bidders,
            COUNT(CASE WHEN sub.tech_result = '1' THEN 1 END) AS tech_passed_count,
            COUNT(CASE WHEN sub.tech_result = '0' THEN 1 END) AS tech_failed_count,
            COUNT(CASE WHEN sub.discount_rate > 0 OR (sub.bid_price IS NOT NULL AND sub.final_bid_price IS NOT NULL AND sub.bid_price > sub.final_bid_price) THEN 1 END) AS discounted_bidder_count,
            COALESCE(SUM(COALESCE(sub.bid_price, 0)), 0) AS total_bid_amount,
            COALESCE(SUM(CASE WHEN sub.bid_price IS NOT NULL AND sub.final_bid_price IS NOT NULL AND sub.bid_price > sub.final_bid_price THEN sub.bid_price - sub.final_bid_price ELSE 0 END), 0) AS total_discount_amount,
            COALESCE(SUM(COALESCE(sub.final_bid_price, sub.bid_price, 0)), 0) AS total_final_bid_amount
        FROM {rel_open_sub} sub
        GROUP BY sub.revision_id
    """)

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
            COALESCE(npm.package_key, 'PKG_UNRESOLVED:OPENING:' || o.entity_id) AS package_key,
            COALESCE(n.round_business_key, 'RND:NOT:VN:' || COALESCE(o.notify_no, o.entity_id)) AS round_business_key,
            COALESCE(CAST(o.actual_open_date AS DATE), o.public_date) AS report_date,
            COALESCE(n.reporting_geography_key, 'GEO_UNKNOWN') AS geography_key,
            COALESCE(n.sector_key, 'SEC_UNKNOWN') AS sector_key,
            COALESCE(n.method_key, 'METHOD_UNKNOWN') AS method_key,
            COALESCE(n.package_value_band_key, 'BAND_RAW') AS value_band_key,
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
        LEFT JOIN _gtmp_opening_sub_agg sub ON o.revision_id = sub.revision_id
        LEFT JOIN _gtmp_cand_notice cn ON o.notify_no = cn.notify_no
        LEFT JOIN _gtmp_notice_package_match npm ON cn.revision_id = npm.revision_id
        LEFT JOIN {schema_prefix}fact_tender_notice n ON cn.notice_business_key = n.notice_business_key AND n.release_id = '{release_id}'
    """)
