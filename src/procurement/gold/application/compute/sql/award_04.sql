CREATE TEMP TABLE _gtmp_award_items_resolved AS
WITH scope_decisions AS (
    SELECT
        ai.*,
        COALESCE(rs.winner_count, 0) AS winner_count,
        COUNT(*) OVER (PARTITION BY ai.package_key, ai.scope_key) AS decisions_count
    FROM _gtmp_award_items ai
    LEFT JOIN _gtmp_recipient_summary rs ON ai.award_business_key = rs.award_business_key
)
SELECT
    sd.*,
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
    -- Amount is eligible ONLY IF effective, amount > 0, currency valid, and confirmed winner exists
    (
        UPPER(COALESCE(sd.status_raw, '')) IN ('IS_PUBLISH', 'PUBLISHED', 'EFFECTIVE', 'HOAT_DONG')
        AND sd.winner_count > 0
        AND sd.award_amount IS NOT NULL
        AND sd.award_amount > 0
        AND sd.currency IS NOT NULL
        AND TRIM(sd.currency) NOT IN ('', 'UNKNOWN', 'CUR_UNKNOWN')
        AND sd.decisions_count = 1
    ) AS is_amount_eligible
FROM scope_decisions sd;
