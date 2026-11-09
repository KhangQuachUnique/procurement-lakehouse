INSERT INTO {p0}bridge_round_document (
    release_id, round_business_key, document_role, document_business_key, revision_id, observation_id
)
SELECT DISTINCT
    '{p1}',
    n.round_business_key,
    'notice',
    n.notice_business_key,
    n.revision_id,
    n.observation_id
FROM _gtmp_cand_notice n
UNION ALL
SELECT DISTINCT
    '{p2}',
    fa.round_business_key,
    'award_result',
    fa.award_business_key,
    ai.result_revision_id,
    result_source.observation_id
FROM {p3}fact_award fa
JOIN _gtmp_award_items_resolved ai ON fa.award_business_key = ai.award_business_key
JOIN _gtmp_cand_result result_source ON result_source.revision_id = ai.result_revision_id
WHERE fa.release_id = '{p4}' AND fa.round_business_key IS NOT NULL;
