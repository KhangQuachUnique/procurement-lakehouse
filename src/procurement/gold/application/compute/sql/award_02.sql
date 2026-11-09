CREATE TEMP TABLE _gtmp_award_contractor_winners AS
SELECT
    ai.award_business_key,
    ai.scope_key,
    c.award_contractor_id,
    c.contractor_code,
    c.contractor_name,
    c.tax_code,
    c.is_consortium,
    c.venture_code,
    c.venture_name,
    c.awarded_price,
    c.role
FROM _gtmp_award_items ai
JOIN {p0} c
  ON c.revision_id = ai.result_revision_id AND c.award_lot_id = ai.award_lot_id
WHERE ai.award_lot_id IS NOT NULL AND c.bid_result = 1
UNION ALL
SELECT ai.award_business_key, ai.scope_key, c.award_contractor_id,
       c.contractor_code, c.contractor_name, c.tax_code, c.is_consortium,
       c.venture_code, c.venture_name, c.awarded_price, c.role
FROM _gtmp_award_items ai
JOIN {p1} c ON c.revision_id = ai.result_revision_id
WHERE ai.award_lot_id IS NULL AND c.bid_result = 1;
