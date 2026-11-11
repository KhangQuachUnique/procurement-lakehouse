INSERT INTO {p0}fact_savings (
    release_id, comparison_scope_key, package_key, scope_key, round_business_key,
    sector_key, method_key, package_value_band_key, investor_party_key, procurer_party_key,
    currency_key, reporting_geography_key, location_set_key, comparison_date_key,
    baseline_date_key, first_award_published_date_key, last_award_published_date_key,
    baseline_amount, baseline_currency_key, award_amount,
    descriptive_difference_amount, is_descriptive_eligible, descriptive_exclusion_reason, descriptive_basis,
    verified_saving_amount, is_verified_comparable, verified_exclusion_reason, tax_basis_state,
    saving_amount, is_comparable, primary_exclusion_reason,
    baseline_basis, baseline_revision_ref, baseline_evidence_set_key, award_evidence_set_key,
    comparison_rule_version, award_state, scope_completeness, is_baseline_valid,
    evidence_set_key, created_at,
    contract_amount, contract_saving_amount, is_contract_eligible
)
SELECT
    '{p1}',
    'CMP:' || fa.scope_key AS comparison_scope_key,
    fa.package_key,
    fa.scope_key,
    fa.round_business_key,
    fa.sector_key,
    fa.method_key,
    fa.package_value_band_key,
    fa.investor_party_key,
    fa.procurer_party_key,
    fa.currency_key,
    fa.reporting_geography_key,
    fa.location_set_key,
    fa.award_published_date_key AS comparison_date_key,
    sb.baseline_date AS baseline_date_key,
    fa.award_published_date_key AS first_award_published_date_key,
    fa.award_published_date_key AS last_award_published_date_key,
    sb.baseline_amount,
    sb.baseline_currency_key,
    fa.award_amount,
    -- 1. Descriptive difference amount (source-declared, negative differences allowed)
    CASE
        WHEN (
            fa.award_state = 'effective'
            AND fa.is_amount_eligible
            AND sb.baseline_amount IS NOT NULL
            AND sb.baseline_amount > 0
            AND sb.baseline_currency_key NOT IN ('CUR_UNKNOWN', 'UNKNOWN')
            AND fa.currency_key NOT IN ('CUR_UNKNOWN', 'UNKNOWN')
            AND sb.baseline_currency_key = fa.currency_key
            AND sb.baseline_date IS NOT NULL
            AND fa.award_published_date_key IS NOT NULL
            AND fa.award_published_date_key >= DATE '{p2}'
            AND fa.award_published_date_key <= DATE '{p3}'
            AND sb.baseline_date <= fa.award_published_date_key
            AND NOT (fa.scope_key LIKE '%:PACKAGE' AND COALESCE(plc.lot_count, 0) > 0)
        )
        THEN sb.baseline_amount - fa.award_amount
        ELSE NULL
    END AS descriptive_difference_amount,
    COALESCE(
        fa.award_state = 'effective'
        AND fa.is_amount_eligible
        AND sb.baseline_amount IS NOT NULL
        AND sb.baseline_amount > 0
        AND sb.baseline_currency_key NOT IN ('CUR_UNKNOWN', 'UNKNOWN')
        AND fa.currency_key NOT IN ('CUR_UNKNOWN', 'UNKNOWN')
        AND sb.baseline_currency_key = fa.currency_key
        AND sb.baseline_date IS NOT NULL
        AND fa.award_published_date_key IS NOT NULL
        AND fa.award_published_date_key >= DATE '{p4}'
        AND fa.award_published_date_key <= DATE '{p5}'
        AND sb.baseline_date <= fa.award_published_date_key
        AND NOT (fa.scope_key LIKE '%:PACKAGE' AND COALESCE(plc.lot_count, 0) > 0),
        FALSE
    ) AS is_descriptive_eligible,
    CASE
        WHEN fa.award_published_date_key IS NULL
        THEN 'unknown_award_date'
        WHEN fa.award_published_date_key > DATE '{p6}' OR fa.award_published_date_key < DATE '{p7}'
        THEN 'result_outside_period'
        WHEN sb.baseline_amount IS NULL OR sb.baseline_amount <= 0
        THEN 'missing_baseline'
        WHEN sb.baseline_date IS NULL
        THEN 'unknown_baseline_date'
        WHEN sb.baseline_date > fa.award_published_date_key
        THEN 'baseline_after_award'
        WHEN fa.scope_key LIKE '%:PACKAGE' AND COALESCE(plc.lot_count, 0) > 0
        THEN 'lot_partition_precedence'
        WHEN fa.award_state = 'cancelled'
        THEN 'cancelled_award'
        WHEN fa.award_state != 'effective'
        THEN 'unresolved_award'
        WHEN fa.currency_key IN ('CUR_UNKNOWN', 'UNKNOWN')
        THEN 'unknown_currency'
        WHEN sb.baseline_currency_key IN ('CUR_UNKNOWN', 'UNKNOWN')
        THEN 'unknown_baseline_currency'
        WHEN sb.baseline_currency_key != fa.currency_key
        THEN 'currency_mismatch'
        WHEN NOT fa.is_amount_eligible
        THEN 'invalid_award_amount'
        ELSE NULL
    END AS descriptive_exclusion_reason,
    sb.baseline_basis AS descriptive_basis,
    -- 2. Verified Savings: Mandatory product decision -> NULL without certified evidence!
    NULL::DECIMAL(38,6) AS verified_saving_amount,
    FALSE AS is_verified_comparable,
    'unverified_tax_basis_and_scope' AS verified_exclusion_reason,
    'unknown' AS tax_basis_state,
    -- Backward compatibility fields: aligned with verified savings
    NULL::DECIMAL(38,6) AS saving_amount,
    FALSE AS is_comparable,
    'unverified_tax_basis_and_scope' AS primary_exclusion_reason,
    sb.baseline_basis,
    sb.baseline_revision_ref,
    sb.baseline_evidence_set_key,
    fa.evidence_set_key AS award_evidence_set_key,
    '{p8}' AS comparison_rule_version,
    fa.award_state,
    CASE
        WHEN fa.scope_key LIKE '%:PACKAGE' AND COALESCE(plc.lot_count, 0) > 0 THEN 'partial'
        ELSE 'unverified'
    END AS scope_completeness,
    (sb.baseline_amount IS NOT NULL AND sb.baseline_amount > 0) AS is_baseline_valid,
    fa.evidence_set_key,
    TIMESTAMPTZ '{p9}' AS created_at,
    cs.contract_price AS contract_amount,
    CASE
        WHEN sb.baseline_amount IS NOT NULL AND sb.baseline_amount > 0 AND cs.contract_price IS NOT NULL AND cs.contract_price > 0
        THEN sb.baseline_amount - cs.contract_price
        ELSE NULL
    END AS contract_saving_amount,
    COALESCE(sb.baseline_amount IS NOT NULL AND sb.baseline_amount > 0 AND cs.contract_price IS NOT NULL AND cs.contract_price > 0, FALSE) AS is_contract_eligible
FROM {p10}fact_award fa
JOIN _gtmp_scope_baselines sb ON fa.award_business_key = sb.award_business_key
LEFT JOIN _gtmp_package_lot_counts plc ON fa.package_key = plc.package_key AND split_part(fa.scope_key, ':ROUND:', 2) = plc.scope_round_key
LEFT JOIN _gtmp_contract_savings cs ON split_part(fa.award_business_key, ':', 2) = cs.result_revision_id
WHERE fa.release_id = '{p11}';
