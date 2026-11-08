CREATE TEMP TABLE _gtmp_recipient_summary AS
SELECT
    award_business_key,
    COUNT(DISTINCT CASE
        WHEN COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL
        THEN 'PARTY:' || COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), ''))
        ELSE 'UNRESOLVED:' || award_contractor_id
    END) AS winner_count,
    BOOL_OR(is_consortium OR NULLIF(TRIM(venture_code), '') IS NOT NULL) AS has_consortium,
    MIN(venture_code) AS sample_venture_code,
    MIN(contractor_code) AS sample_contractor_code,
    MIN(tax_code) AS sample_tax_code,
    MIN(contractor_name) AS sample_contractor_name
FROM _gtmp_award_contractor_winners
GROUP BY award_business_key;
