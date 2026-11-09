INSERT INTO {p0}fact_selection_round (
    release_id, round_business_key, package_key, sector_key, method_key,
    package_value_band_key, investor_party_key, procurer_party_key,
    reporting_geography_key, location_set_key,
    plan_reference_date_key, first_notice_date_key, close_date_key, first_notice_at, close_at,
    first_award_decision_date_key, complete_award_decision_date_key,
    first_award_published_date_key, complete_award_published_date_key,
    cancel_date_key,
    plan_to_notice_days, is_plan_to_notice_eligible,
    notice_to_close_hours, is_notice_to_close_eligible,
    close_to_first_award_days, is_close_to_first_award_eligible,
    close_to_complete_award_days, is_close_to_complete_award_eligible,
    planned_start_from, planned_start_to, start_timing_status,
    round_state, award_completion_state, cancellation_state,
    cancel_reason_raw, cancel_reason_category, evidence_set_key, created_at
)
SELECT
    '{p1}',
    re.round_business_key,
    re.package_key,
    re.sector_key,
    re.method_key,
    re.package_value_band_key,
    re.investor_party_key,
    re.procurer_party_key,
    re.reporting_geography_key,
    re.location_set_key,
    re.plan_ref_date,
    re.first_notice_date,
    re.close_date,
    re.first_notice_published_at,
    re.close_at,
    ra.first_award_decision_date,
    ra.complete_award_decision_date,
    ra.first_award_pub_date,
    ra.complete_award_pub_date,
    re.cancel_date,
    -- plan_to_notice_days
    CASE
        WHEN re.plan_ref_date IS NOT NULL AND re.first_notice_date IS NOT NULL AND re.first_notice_date >= re.plan_ref_date
        THEN date_diff('day', re.plan_ref_date, re.first_notice_date)
        ELSE NULL
    END,
    (re.plan_ref_date IS NOT NULL AND re.first_notice_date IS NOT NULL AND re.first_notice_date >= re.plan_ref_date),
    -- notice_to_close_hours (only matched endpoints; differing lot close times -> null)
    CASE
        WHEN re.first_notice_published_at IS NOT NULL AND re.close_at IS NOT NULL AND re.close_at >= re.first_notice_published_at
        THEN CAST(ROUND(date_diff('second', re.first_notice_published_at, re.close_at) / 3600.0, 2) AS DECIMAL(38,6))
        ELSE NULL
    END,
    (re.first_notice_published_at IS NOT NULL AND re.close_at IS NOT NULL AND re.close_at >= re.first_notice_published_at),
    -- close_to_first_award_days
    CASE
        WHEN re.close_date IS NOT NULL AND ra.first_award_pub_date IS NOT NULL AND ra.first_award_pub_date >= re.close_date AND (ra.first_award_decision_date IS NULL OR ra.first_award_decision_date >= re.close_date)
        THEN date_diff('day', re.close_date, ra.first_award_pub_date)
        ELSE NULL
    END,
    (re.close_date IS NOT NULL AND ra.first_award_pub_date IS NOT NULL AND ra.first_award_pub_date >= re.close_date AND (ra.first_award_decision_date IS NULL OR ra.first_award_decision_date >= re.close_date)),
    -- close_to_complete_award_days
    CASE
        WHEN re.close_date IS NOT NULL AND ra.complete_award_pub_date IS NOT NULL AND ra.complete_award_pub_date >= re.close_date AND (ra.complete_award_decision_date IS NULL OR ra.complete_award_decision_date >= re.close_date)
        THEN date_diff('day', re.close_date, ra.complete_award_pub_date)
        ELSE NULL
    END,
    (re.close_date IS NOT NULL AND ra.complete_award_pub_date IS NOT NULL AND ra.complete_award_pub_date >= re.close_date AND (ra.complete_award_decision_date IS NULL OR ra.complete_award_decision_date >= re.close_date)),
    dem.planned_start_from,
    dem.planned_start_to,
    CASE
        WHEN dem.planned_start_from IS NULL THEN 'unresolved'
        WHEN re.first_notice_date >= dem.planned_start_from AND re.first_notice_date <= dem.planned_start_to THEN 'within'
        WHEN re.first_notice_date < dem.planned_start_from THEN 'before'
        ELSE 'after'
    END AS start_timing_status,
    CASE
        WHEN re.is_cancelled THEN 'cancelled'
        WHEN ra.award_count > 0 THEN 'in_progress'  -- Completion remains unverified until expected scope is established
        ELSE 'in_progress'
    END AS round_state,
    CASE
        WHEN ra.award_count > 0 AND ra.all_effective THEN 'partially_observed'
        WHEN ra.award_count > 0 THEN 'unresolved'
        ELSE 'not_observed'  -- No observed result != no_winner
    END AS award_completion_state,
    CASE
        WHEN re.is_cancelled THEN 'cancelled'
        ELSE 'none'
    END AS cancellation_state,
    re.cancel_reason,
    CASE WHEN re.is_cancelled THEN 'explicit_round_cancellation' ELSE NULL END,
    re.evidence_set_key,
    TIMESTAMPTZ '{p2}' AS created_at
FROM _gtmp_round_endpoints re
LEFT JOIN _gtmp_round_award_endpoints ra ON re.round_business_key = ra.round_business_key
LEFT JOIN {p3}fact_procurement_demand dem
  ON dem.release_id = '{p4}' AND dem.package_key = re.package_key;
