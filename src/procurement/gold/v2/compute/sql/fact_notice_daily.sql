SELECT owner_key,report_date,geography_key,sector_key,method_key,notice_family,notice_count,cancelled_notice_count FROM (SELECT package_key AS owner_key,
published_date_key AS report_date,
reporting_geography_key AS geography_key,
sector_key AS sector_key,
method_key AS method_key,
notice_family AS notice_family,
CAST(coalesce((1),0) AS BIGINT) AS notice_count,
CAST(coalesce((CASE WHEN cancellation_state='cancelled' THEN 1 ELSE 0 END),0) AS BIGINT) AS cancelled_notice_count FROM business.fact_tender_notice WHERE (is_in_event_period OR published_date_key IS NULL)) projected
