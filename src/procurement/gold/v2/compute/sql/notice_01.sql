INSERT INTO {p0}fact_tender_notice (
    release_id, notice_business_key, package_key, scope_key, sector_key, method_key,
    package_value_band_key, investor_party_key, procurer_party_key,
    published_date_key, published_at, close_date_key, close_at, original_published_at,
    preparation_basis, notice_budget_amount, notice_currency_key, initial_reoffer_amount,
    reoffer_currency_key, guarantee_amount, guarantee_currency_key, guarantee_form,
    contract_duration_value, contract_duration_unit, contract_duration_raw,
    preparation_hours, is_preparation_eligible, is_guarantee_eligible,
    round_business_key, notice_family, cancellation_state, cancel_date_key,
    cancel_reason_raw, cancel_reason_category, reporting_geography_key,
    location_set_key, evidence_set_key, created_at
)
SELECT
    '{p1}',
    n.notice_business_key,
    npm.package_key AS package_key,
    npm.package_key || ':PACKAGE' AS scope_key,
    'SEC_' || COALESCE(NULLIF(TRIM(n.bid_field), ''), 'UNKNOWN') AS sector_key,
    'MTH:' || MD5(
        COALESCE(n.bid_form, '') || '|' ||
        COALESCE(n.bid_mode, '') || '|' ||
        COALESCE(n.is_domestic::VARCHAR, '') || '|' ||
        COALESCE(n.is_internet::VARCHAR, '') || '|' ||
        COALESCE(n.contract_type, '') || '|' ||
        '{p2}'
    ) AS method_key,
    'BAND_RAW' AS package_value_band_key,
    'PARTY_UNRESOLVED:{p3}:NOTICE_INVESTOR:' || n.entity_id AS investor_party_key,
    CASE WHEN NULLIF(TRIM(n.procuring_entity_code), '') IS NOT NULL THEN 'PARTY_PROCURER:' || TRIM(n.procuring_entity_code) ELSE 'PARTY_UNRESOLVED:{p4}:NOTICE_PROCURER:' || n.entity_id END AS procurer_party_key,
    n.public_date AS published_date_key,
    n.published_at,
    CAST(n.bid_close_date AS DATE) AS close_date_key,
    n.bid_close_date AS close_at,
    n.original_published_at,
    'notice_revision_timestamp' AS preparation_basis,
    n.bid_price AS notice_budget_amount,
    CASE
        WHEN n.currency IS NULL OR TRIM(n.currency) IN ('', 'UNKNOWN', 'None', 'CUR_UNKNOWN') THEN 'CUR_UNKNOWN'
        ELSE UPPER(TRIM(n.currency))
    END AS notice_currency_key,
    n.initial_price AS initial_reoffer_amount,
    n.reoffer_currency AS reoffer_currency_key,
    n.guarantee_amount,
    COALESCE(NULLIF(UPPER(TRIM(json_extract_string(n.root_json, '$.bidGuaranteeUnit'))), ''),
        CASE WHEN n.source_family = 'standard' THEN NULLIF(UPPER(TRIM(json_extract_string(n.root_json, '$.bidPriceUnit'))), '') END,
        'CUR_UNKNOWN') AS guarantee_currency_key,
    n.guarantee_form,
    NULL::BIGINT AS contract_duration_value,
    n.contract_duration_unit,
    n.contract_duration_raw,
    CASE
        WHEN n.published_at IS NOT NULL AND n.bid_close_date IS NOT NULL AND n.bid_close_date >= n.published_at
        THEN CAST(ROUND(date_diff('second', n.published_at, n.bid_close_date) / 3600.0, 2) AS DECIMAL(38,6))
        ELSE NULL
    END AS preparation_hours,
    (n.published_at IS NOT NULL AND n.bid_close_date IS NOT NULL AND n.bid_close_date >= n.published_at) AS is_preparation_eligible,
    (n.guarantee_amount IS NOT NULL AND n.guarantee_amount > 0 AND COALESCE(NULLIF(UPPER(TRIM(json_extract_string(n.root_json, '$.bidGuaranteeUnit'))), ''),
        CASE WHEN n.source_family = 'standard' THEN NULLIF(UPPER(TRIM(json_extract_string(n.root_json, '$.bidPriceUnit'))), '') END,
        'CUR_UNKNOWN') != 'CUR_UNKNOWN') AS is_guarantee_eligible,
    n.round_business_key,
    n.notice_family_clean AS notice_family,
    CASE
        WHEN n.cancelled_at IS NOT NULL OR n.cancel_reason IS NOT NULL OR UPPER(n.status_raw) IN ('CANCELLED', 'REVOKED', 'HUY_THAU', 'HUY', 'ĐÃ HỦY THẦU')
        THEN 'cancelled'
        ELSE 'none'
    END AS cancellation_state,
    CAST(n.cancelled_at AS DATE) AS cancel_date_key,
    n.cancel_reason AS cancel_reason_raw,
    CASE
        WHEN n.cancel_reason IS NOT NULL THEN 'explicit_notice_cancellation'
        ELSE NULL
    END AS cancel_reason_category,
    'GEO_UNKNOWN' AS reporting_geography_key,
    'LOC_SET:UNKNOWN' AS location_set_key,
    'EVD:NOTICE:' || n.revision_id AS evidence_set_key,
    TIMESTAMPTZ '{p5}' AS created_at
FROM _gtmp_cand_notice n
JOIN _gtmp_notice_package_match npm ON npm.revision_id = n.revision_id;
