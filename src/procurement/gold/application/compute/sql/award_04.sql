CREATE TEMP TABLE _gtmp_award_items_resolved AS
WITH scope_decisions_raw AS (
    SELECT
        ai.result_revision_id,
        ai.package_key,
        ai.scope_key,
        ai.award_lot_id,
        ai.award_amount AS raw_award_amount,
        ai.candidate_lot_price,
        ai.lot_currency,
        ai.currency,
        ai.award_published_date,
        ai.award_decision_date,
        ai.award_business_key,
        ai.status_raw,
        ai.notify_no,
        CASE
            WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
            THEN ai.candidate_lot_price
            WHEN ai.award_lot_id IS NULL
                 AND dem.planned_package_amount IS NOT NULL
                 AND dem.planned_package_amount > 0
            THEN dem.planned_package_amount
            ELSE NULL
        END AS ref_baseline,
        COALESCE(rs.winner_count, 0) AS winner_count,
        COUNT(*) OVER (PARTITION BY ai.package_key, ai.scope_key) AS decisions_count
    FROM _gtmp_award_items ai
    LEFT JOIN _gtmp_recipient_summary rs ON ai.award_business_key = rs.award_business_key
    LEFT JOIN {p0}fact_procurement_demand dem
      ON dem.release_id = '{p1}' AND dem.package_key = ai.package_key
),
scope_decisions AS (
    SELECT
        sdr.* EXCLUDE (raw_award_amount, ref_baseline),
        sdr.ref_baseline,
        recover_award_amount(sdr.raw_award_amount, sdr.ref_baseline) AS award_amount
    FROM scope_decisions_raw sdr
)
SELECT
    sd.* EXCLUDE (ref_baseline),
    CASE
        WHEN UPPER(COALESCE(sd.status_raw, '')) IN ('CANCELLED', 'REVOKED', 'HUY_THAU', 'HUY', 'HỦY KẾT QUẢ')
        THEN 'cancelled'
        WHEN sd.decisions_count > 1
        THEN 'unresolved_overlap'
        WHEN sd.winner_count = 0
        THEN 'no_confirmed_winner'
        WHEN UPPER(COALESCE(sd.status_raw, '')) IN ('IS_PUBLISH', 'PUBLISHED', 'EFFECTIVE', 'HOAT_DONG')
        THEN 'effective'
        ELSE 'unresolved_status'
    END AS award_state,
    -- Tier 3 Eligibility Gate: Amount is eligible ONLY IF effective, amount > 0, currency valid, confirmed winner exists,
    -- AND not an unrecovered extreme overrun (> 2x baseline or > 50,000 Bn VND single scope cap)
    (
        UPPER(COALESCE(sd.status_raw, '')) IN ('IS_PUBLISH', 'PUBLISHED', 'EFFECTIVE', 'HOAT_DONG')
        AND sd.winner_count > 0
        AND sd.award_amount IS NOT NULL
        AND sd.award_amount > 0
        AND sd.currency IS NOT NULL
        AND TRIM(sd.currency) NOT IN ('', 'UNKNOWN', 'CUR_UNKNOWN')
        AND sd.decisions_count = 1
        AND (sd.ref_baseline IS NULL OR sd.award_amount <= sd.ref_baseline * 2)
        AND sd.award_amount <= 50000000000000
    ) AS is_amount_eligible
FROM scope_decisions sd;
