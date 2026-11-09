INSERT INTO {p0}bridge_award_recipient (
    release_id, award_business_key, scope_key, party_key, attributed_amount, attribution_share, allocation_state
)
-- Single recipient: 100% verified allocation
SELECT DISTINCT
    '{p1}',
    w.award_business_key,
    w.scope_key,
    CASE
        WHEN COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), '')) IS NOT NULL
        THEN 'PARTY:' || COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), ''))
        ELSE 'PARTY_UNRESOLVED:' || '{p2}' || ':' || w.award_contractor_id
    END AS party_key,
    ai.award_amount AS attributed_amount,
    1.0 AS attribution_share,
    'verified_amount' AS allocation_state
FROM _gtmp_award_contractor_winners w
JOIN _gtmp_recipient_summary rs ON w.award_business_key = rs.award_business_key
JOIN _gtmp_award_items_resolved ai ON w.award_business_key = ai.award_business_key
WHERE rs.winner_count = 1 AND NOT rs.has_consortium

UNION ALL

-- Consortium members: unknown allocation remains unallocated, never assign full scope amount to each member!
SELECT DISTINCT
    '{p3}',
    w.award_business_key,
    w.scope_key,
    CASE
        WHEN COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), '')) IS NOT NULL
        THEN 'PARTY:' || COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), ''))
        ELSE 'PARTY_UNRESOLVED:' || '{p4}' || ':' || w.award_contractor_id
    END AS party_key,
    NULL AS attributed_amount,
    NULL AS attribution_share,
    'unallocated' AS allocation_state
FROM _gtmp_award_contractor_winners w
JOIN _gtmp_recipient_summary rs ON w.award_business_key = rs.award_business_key
WHERE rs.has_consortium OR rs.winner_count > 1;
