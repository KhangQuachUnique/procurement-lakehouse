INSERT INTO {p0}bridge_party_member (
    release_id, party_key, member_party_key, member_role, member_tax_code, member_name
)
WITH memberships AS (
    SELECT
        'CON:' || COALESCE(NULLIF(TRIM(venture_code), ''), 'CONSORTIUM_' || '{p1}' || '_' || award_business_key) AS party_key,
        CASE
            WHEN COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL
            THEN 'PARTY:' || COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), ''))
            ELSE 'PARTY_UNRESOLVED:' || '{p2}' || ':' || award_contractor_id
        END AS member_party_key,
        role, tax_code, contractor_name
    FROM _gtmp_award_contractor_winners
    WHERE is_consortium = TRUE OR NULLIF(TRIM(venture_code), '') IS NOT NULL
)
SELECT
    '{p3}', party_key, member_party_key,
    CASE WHEN COUNT(DISTINCT role) = 1 THEN MIN(role) END,
    CASE WHEN COUNT(DISTINCT tax_code) = 1 THEN MIN(tax_code) END,
    CASE WHEN COUNT(DISTINCT contractor_name) = 1 THEN MIN(contractor_name) END
FROM memberships
GROUP BY party_key, member_party_key;
