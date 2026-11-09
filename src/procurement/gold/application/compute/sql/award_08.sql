INSERT INTO {p0}fact_award (
    release_id, award_business_key, scope_key, package_key, sector_key, method_key,
    package_value_band_key, investor_party_key, procurer_party_key, currency_key,
    award_published_date_key, award_decision_date_key, award_amount, award_state,
    is_amount_eligible, round_business_key, recipient_set_key, recipient_allocation_state,
    reporting_geography_key, location_set_key, evidence_set_key, created_at
)
SELECT
    '{p1}',
    ai.award_business_key,
    ai.scope_key,
    ai.package_key,
    COALESCE(dem.sector_key, 'SEC_UNKNOWN') AS sector_key,
    COALESCE(dem.method_key, 'METHOD_UNKNOWN') AS method_key,
    COALESCE(dem.package_value_band_key, 'BAND_RAW') AS package_value_band_key,
    COALESCE(dem.investor_party_key, 'PARTY_INVESTOR:UNKNOWN') AS investor_party_key,
    COALESCE(dem.procurer_party_key, 'PARTY_BUYER:UNKNOWN') AS procurer_party_key,
    CASE
        WHEN ai.currency IS NULL OR TRIM(ai.currency) IN ('', 'UNKNOWN', 'None', 'CUR_UNKNOWN') THEN 'CUR_UNKNOWN'
        ELSE UPPER(TRIM(ai.currency))
    END AS currency_key,
    ai.award_published_date AS award_published_date_key,
    ai.award_decision_date AS award_decision_date_key,
    -- Amount is set to NULL if not eligible or no confirmed winner
    CASE WHEN ai.is_amount_eligible THEN ai.award_amount ELSE NULL END AS award_amount,
    ai.award_state,
    ai.is_amount_eligible,
    CASE
        WHEN not_sub.round_business_key IS NOT NULL THEN not_sub.round_business_key
        ELSE NULL
    END AS round_business_key,
    CASE
        WHEN rs.winner_count = 0 OR rs.winner_count IS NULL THEN 'REC_NO_WINNER'
        WHEN rs.has_consortium THEN 'CON:' || COALESCE(NULLIF(TRIM(rs.sample_venture_code), ''), 'CONSORTIUM_' || '{p2}' || '_' || ai.award_business_key)
        WHEN rs.winner_count = 1 THEN (
            CASE
                WHEN COALESCE(NULLIF(TRIM(rs.sample_contractor_code), ''), NULLIF(TRIM(rs.sample_tax_code), '')) IS NOT NULL
                THEN 'PARTY:' || COALESCE(NULLIF(TRIM(rs.sample_contractor_code), ''), NULLIF(TRIM(rs.sample_tax_code), ''))
                ELSE 'PARTY_UNRESOLVED:' || '{p3}' || ':' || ai.award_business_key
            END
        )
        ELSE 'REC_MULTIPLE_INDEPENDENT'
    END AS recipient_set_key,
    CASE
        WHEN rs.winner_count = 0 OR rs.winner_count IS NULL THEN 'no_winner'
        WHEN rs.has_consortium THEN 'unallocated_consortium'
        WHEN rs.winner_count = 1 THEN 'single_recipient'
        ELSE 'independent_winners'
    END AS recipient_allocation_state,
    COALESCE(dem.reporting_geography_key, 'GEO_UNKNOWN') AS reporting_geography_key,
    COALESCE(dem.location_set_key, 'LOC_SET:UNKNOWN') AS location_set_key,
    'EVD:AWARD:' || ai.result_revision_id AS evidence_set_key,
    TIMESTAMPTZ '{p4}' AS created_at
FROM _gtmp_award_items_resolved ai
LEFT JOIN {p5}fact_procurement_demand dem
  ON dem.release_id = '{p6}' AND dem.package_key = ai.package_key
LEFT JOIN _gtmp_recipient_summary rs ON ai.award_business_key = rs.award_business_key
LEFT JOIN (
    SELECT notify_no, MIN(round_business_key) AS round_business_key
    FROM _gtmp_cand_notice GROUP BY notify_no HAVING COUNT(*) = 1
) not_sub ON ai.notify_no = not_sub.notify_no;
