CREATE TEMP TABLE _gtmp_round_endpoints AS
SELECT
    n.round_business_key,
    -- Unmatched notices still have a stable unresolved/ambiguous
    -- owner. A nullable demand-side key cannot be retracted from
    -- the incremental contribution ledger on a later correction.
    MIN(n.package_key) AS package_key,
    MIN(dem.sector_key) AS sector_key,
    MIN(dem.method_key) AS method_key,
    MIN(dem.package_value_band_key) AS package_value_band_key,
    MIN(dem.investor_party_key) AS investor_party_key,
    MIN(dem.procurer_party_key) AS procurer_party_key,
    MIN(dem.reporting_geography_key) AS reporting_geography_key,
    MIN(dem.location_set_key) AS location_set_key,
    MIN(dem.package_published_date_key) AS plan_ref_date,
    MIN(n.published_date_key) AS first_notice_date,
    MIN(n.published_at) AS first_notice_published_at,
    -- Differing lot close times lead to ambiguous round close date -> NULL
    CASE
        WHEN COUNT(DISTINCT n.close_date_key) = 1 THEN MIN(n.close_date_key)
        ELSE NULL
    END AS close_date,
    CASE
        WHEN COUNT(DISTINCT n.close_at) = 1 THEN MIN(n.close_at)
        ELSE NULL
    END AS close_at,
    BOOL_OR(n.cancellation_state = 'cancelled') AS is_cancelled,
    MIN(n.cancel_date_key) AS cancel_date,
    MIN(n.cancel_reason_raw) AS cancel_reason,
    MIN(n.evidence_set_key) AS evidence_set_key
FROM {p0}fact_tender_notice n
LEFT JOIN {p1}fact_procurement_demand dem
  ON dem.release_id = '{p2}' AND dem.package_key = n.package_key
WHERE n.release_id = '{p3}'
GROUP BY n.round_business_key;
