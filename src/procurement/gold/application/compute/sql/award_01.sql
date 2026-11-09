CREATE TEMP TABLE _gtmp_award_items AS
-- 1. Lot-level award items (scoped with source_lot_id)
SELECT
    r.revision_id AS result_revision_id,
    rpm.package_key,
    rpm.package_key || ':LOT:' || al.lot_no || ':' || COALESCE(NULLIF(TRIM(json_extract_string(al.raw_json, '$.id')), ''), 'REVISION:' || al.award_lot_id) || ':ROUND:' || COALESCE((SELECT MIN(nr.round_business_key) FROM _gtmp_cand_notice nr WHERE nr.notify_no = r.notify_no HAVING COUNT(*) = 1), 'RESULT:' || r.entity_id) AS scope_key,
    al.award_lot_id,
    al.winning_price AS award_amount,
    al.lot_price AS candidate_lot_price,
    al.currency AS lot_currency,
    COALESCE(al.currency, r.currency) AS currency,
    r.public_date AS award_published_date,
    r.decision_date AS award_decision_date,
    'AWARD:' || r.revision_id || ':LOT:' || al.award_lot_id AS award_business_key,
    r.status_raw,
    r.notify_no
FROM _gtmp_cand_result r
JOIN _gtmp_result_package_match rpm ON r.revision_id = rpm.revision_id
JOIN {p0} al ON r.revision_id = al.revision_id
WHERE al.lot_no IS NOT NULL AND TRIM(al.lot_no) != ''

UNION ALL

-- 2. Whole package award items when no award_lot exists
SELECT
    r.revision_id AS result_revision_id,
    rpm.package_key,
    rpm.package_key || ':PACKAGE:ROUND:' || COALESCE((SELECT MIN(nr.round_business_key) FROM _gtmp_cand_notice nr WHERE nr.notify_no = r.notify_no HAVING COUNT(*) = 1), 'RESULT:' || r.entity_id) AS scope_key,
    NULL AS award_lot_id,
    r.total_winning_price AS award_amount,
    NULL::DECIMAL(38,6) AS candidate_lot_price,  -- Never use r.amount as budget fallback!
    r.currency AS lot_currency,
    r.currency,
    r.public_date AS award_published_date,
    r.decision_date AS award_decision_date,
    'AWARD:' || r.revision_id || ':PKG' AS award_business_key,
    r.status_raw,
    r.notify_no
FROM _gtmp_cand_result r
JOIN _gtmp_result_package_match rpm ON r.revision_id = rpm.revision_id
WHERE r.revision_id NOT IN (SELECT DISTINCT revision_id FROM {p1});
