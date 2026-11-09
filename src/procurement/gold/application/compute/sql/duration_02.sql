CREATE TEMP TABLE _gtmp_round_award_endpoints AS
SELECT
    round_business_key,
    MIN(award_decision_date_key) AS first_award_decision_date,
    MAX(award_decision_date_key) AS complete_award_decision_date,
    MIN(award_published_date_key) AS first_award_pub_date,
    MAX(award_published_date_key) AS complete_award_pub_date,
    COUNT(*) AS award_count,
    BOOL_AND(award_state = 'effective') AS all_effective
FROM {p0}fact_award
WHERE release_id = '{p1}' AND round_business_key IS NOT NULL
GROUP BY round_business_key;
