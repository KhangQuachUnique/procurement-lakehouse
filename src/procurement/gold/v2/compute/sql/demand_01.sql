INSERT INTO {p0}fact_procurement_demand (
    release_id, package_key, sector_key, method_key, package_value_band_key,
    investor_party_key, procurer_party_key, currency_key,
    package_published_date_key, plan_published_date_key,
    planned_start_from, planned_start_to, planned_start_precision,
    reporting_geography_key, location_set_key,
    planned_package_amount, is_amount_eligible,
    notice_observation_state, award_observation_state, relationship_state,
    plan_business_id, project_business_id, evidence_set_key, created_at
)
SELECT
    '{p1}' AS release_id,
    'PKG:' || p.entity_id AS package_key,
    'SEC_' || COALESCE(NULLIF(TRIM(p.bid_field), ''), 'UNKNOWN') AS sector_key,
    'MTH:' || MD5(
        COALESCE(p.bid_form, '') || '|' ||
        COALESCE(p.bid_mode, '') || '|' ||
        COALESCE(p.is_domestic::VARCHAR, '') || '|' ||
        COALESCE(p.is_internet::VARCHAR, '') || '|' ||
        COALESCE(p.contract_type, '') || '|' ||
        '{p2}'
    ) AS method_key,
    'BAND_RAW' AS package_value_band_key,
    COALESCE('PARTY_UNRESOLVED:{p3}:PLAN_INVESTOR:' || pl.entity_id, 'PARTY_INVESTOR:UNKNOWN') AS investor_party_key,
    'PARTY_BUYER:UNKNOWN' AS procurer_party_key,
    CASE
        WHEN p.currency IS NULL OR TRIM(p.currency) IN ('', 'UNKNOWN', 'None', 'CUR_UNKNOWN') THEN 'CUR_UNKNOWN'
        ELSE UPPER(TRIM(p.currency))
    END AS currency_key,
    p.public_date AS package_published_date_key,
    pl.public_date AS plan_published_date_key,
    CASE
        WHEN p.bid_start_year IS NOT NULL AND p.bid_start_month IS NOT NULL
        THEN make_date(p.bid_start_year::INT, p.bid_start_month::INT, 1)
        WHEN p.bid_start_year IS NOT NULL
        THEN make_date(p.bid_start_year::INT, 1, 1)
        ELSE NULL
    END AS planned_start_from,
    CASE
        WHEN p.bid_start_year IS NOT NULL AND p.bid_start_month IS NOT NULL
        THEN last_day(make_date(p.bid_start_year::INT, p.bid_start_month::INT, 1))
        WHEN p.bid_start_year IS NOT NULL
        THEN make_date(p.bid_start_year::INT, 12, 31)
        ELSE NULL
    END AS planned_start_to,
    CASE
        WHEN p.bid_start_month IS NOT NULL THEN 'month'
        WHEN p.bid_start_quarter IS NOT NULL THEN 'quarter'
        WHEN p.bid_start_year IS NOT NULL THEN 'year'
        ELSE 'unresolved'
    END AS planned_start_precision,
    CASE
        WHEN geo.prov_count = 1 THEN 'GEO_' || geo.single_province_code
        WHEN geo.prov_count > 1 THEN 'GEO_MULTI_UNALLOCATED'
        ELSE 'GEO_UNKNOWN'
    END AS reporting_geography_key,
    'LOC_SET:' || COALESCE(geo.loc_hash, 'UNKNOWN') AS location_set_key,
    p.bid_price AS planned_package_amount,
    (p.bid_price IS NOT NULL AND p.bid_price > 0 AND p.currency IS NOT NULL AND p.currency NOT IN ('', 'UNKNOWN')) AS is_amount_eligible,
    CASE WHEN not_sub.pkg_id IS NOT NULL THEN 'observed' ELSE 'not_observed' END AS notice_observation_state,
    CASE WHEN res_sub.pkg_id IS NOT NULL THEN 'observed' ELSE 'not_observed' END AS award_observation_state,
    p.package_resolution_state AS relationship_state,
    p.plan_no AS plan_business_id,
    pl.project_no AS project_business_id,
    'EVD:DEMAND:' || p.revision_id AS evidence_set_key,
    TIMESTAMPTZ '{p4}' AS created_at
FROM _gtmp_cand_package p
{p5}
LEFT JOIN _gtmp_pkg_geography geo ON p.revision_id = geo.revision_id
LEFT JOIN (
    SELECT DISTINCT resolved_entity_id AS pkg_id FROM _gtmp_notice_package_match
    WHERE resolved_entity_id IS NOT NULL
) not_sub ON p.entity_id = not_sub.pkg_id
LEFT JOIN (
    SELECT DISTINCT rpm2.direct_package_entity_id AS pkg_id FROM _gtmp_result_package_match rpm2 WHERE direct_package_entity_id IS NOT NULL
    UNION
    SELECT DISTINCT rpm3.unique_code_package_entity_id FROM _gtmp_result_package_match rpm3 WHERE unique_code_package_entity_id IS NOT NULL
) res_sub ON p.entity_id = res_sub.pkg_id;
