SELECT
    owner_key,
    report_date,
    geography_key,
    sector_key,
    method_key,
    value_band_key,
    opening_count,
    total_bidders,
    single_bidder_count,
    multi_bidder_count,
    tech_passed_count,
    tech_failed_count,
    discounted_bidder_count,
    total_bid_amount,
    total_discount_amount,
    total_final_bid_amount
FROM (
    SELECT
        package_key AS owner_key,
        report_date,
        geography_key,
        sector_key,
        method_key,
        value_band_key,
        CAST(COALESCE(opening_count, 0) AS BIGINT) AS opening_count,
        CAST(COALESCE(total_bidders, 0) AS BIGINT) AS total_bidders,
        CAST(COALESCE(single_bidder_count, 0) AS BIGINT) AS single_bidder_count,
        CAST(COALESCE(multi_bidder_count, 0) AS BIGINT) AS multi_bidder_count,
        CAST(COALESCE(tech_passed_count, 0) AS BIGINT) AS tech_passed_count,
        CAST(COALESCE(tech_failed_count, 0) AS BIGINT) AS tech_failed_count,
        CAST(COALESCE(discounted_bidder_count, 0) AS BIGINT) AS discounted_bidder_count,
        CAST(COALESCE(total_bid_amount, 0) AS DECIMAL(38,6)) AS total_bid_amount,
        CAST(COALESCE(total_discount_amount, 0) AS DECIMAL(38,6)) AS total_discount_amount,
        CAST(COALESCE(total_final_bid_amount, 0) AS DECIMAL(38,6)) AS total_final_bid_amount
    FROM business.fact_bidding_competition
    WHERE (is_in_event_period OR report_date IS NULL)
) projected
