INSERT INTO {p0}bridge_savings_award (
    release_id, comparison_scope_key, award_business_key, scope_key, included_award_amount
)
SELECT
    '{p1}',
    'CMP:' || fa.scope_key,
    fa.award_business_key,
    fa.scope_key,
    COALESCE(fa.award_amount, 0)
FROM {p2}fact_award fa
WHERE fa.release_id = '{p3}';
