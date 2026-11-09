CREATE TEMP TABLE _gtmp_all_parties AS
-- Contractor winners with code/tax_code
SELECT
    'PARTY:' || COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) AS party_key,
    'ORGANIZATION' AS party_kind,
    contractor_code AS party_code,
    contractor_name AS party_name,
    tax_code,
    'silver.award_contractor' AS source_namespace
FROM _gtmp_award_contractor_winners
WHERE COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL

UNION ALL

-- Unresolved contractors
SELECT
    'PARTY_UNRESOLVED:' || '{p0}' || ':' || award_contractor_id AS party_key,
    'UNKNOWN' AS party_kind,
    NULL AS party_code,
    contractor_name AS party_name,
    NULL AS tax_code,
    'silver.award_contractor' AS source_namespace
FROM _gtmp_award_contractor_winners
WHERE COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NULL

UNION ALL

-- Consortiums
SELECT
    'CON:' || COALESCE(NULLIF(TRIM(venture_code), ''), 'CONSORTIUM_' || '{p1}' || '_' || award_business_key) AS party_key,
    'CONSORTIUM' AS party_kind,
    venture_code AS party_code,
    COALESCE(venture_name, 'Liên danh ' || award_business_key) AS party_name,
    NULL AS tax_code,
    'silver.award_contractor' AS source_namespace
FROM _gtmp_award_contractor_winners
WHERE is_consortium = TRUE OR venture_code IS NOT NULL

UNION ALL

-- Plan investors
SELECT
    'PARTY_UNRESOLVED:{p2}:PLAN_INVESTOR:' || entity_id AS party_key,
    'UNKNOWN' AS party_kind,
    NULL AS party_code,
    investor_name AS party_name,
    NULL AS tax_code,
    'silver.plan_revision' AS source_namespace
FROM _gtmp_cand_plan

UNION ALL

-- Notice investors
SELECT
    'PARTY_UNRESOLVED:{p3}:NOTICE_INVESTOR:' || entity_id AS party_key,
    'UNKNOWN' AS party_kind,
    NULL AS party_code,
    investor_name AS party_name,
    NULL AS tax_code,
    'silver.notice_revision' AS source_namespace
FROM _gtmp_cand_notice

UNION ALL

-- Notice procurers
SELECT
    CASE WHEN NULLIF(TRIM(procuring_entity_code), '') IS NOT NULL THEN 'PARTY_PROCURER:' || TRIM(procuring_entity_code) ELSE 'PARTY_UNRESOLVED:{p4}:NOTICE_PROCURER:' || entity_id END AS party_key,
    CASE WHEN NULLIF(TRIM(procuring_entity_code), '') IS NOT NULL THEN 'ORGANIZATION' ELSE 'UNKNOWN' END AS party_kind,
    procuring_entity_code AS party_code,
    procuring_entity_name AS party_name,
    NULL AS tax_code,
    'silver.notice_revision' AS source_namespace
FROM _gtmp_cand_notice

UNION ALL

-- System placeholders
SELECT 'PARTY_BUYER:UNKNOWN' AS party_key, 'UNKNOWN' AS party_kind, NULL AS party_code, 'Unknown Buyer' AS party_name, NULL AS tax_code, 'system' AS source_namespace
UNION ALL
SELECT 'PARTY_INVESTOR:UNKNOWN', 'UNKNOWN', NULL, 'Unknown Investor', NULL, 'system'
UNION ALL
SELECT 'PARTY_PROCURER:UNKNOWN', 'UNKNOWN', NULL, 'Unknown Procurer', NULL, 'system'
UNION ALL
SELECT 'REC_NO_WINNER', 'UNKNOWN', 'NO_WINNER', 'Không có nhà thầu trúng thầu', NULL, 'system'
UNION ALL
SELECT 'REC_MULTIPLE_INDEPENDENT', 'UNKNOWN', 'INDEPENDENT_WINNERS', 'Nhiều nhà thầu độc lập trúng thầu', NULL, 'system';

INSERT INTO {p5}dim_party (
    release_id, party_key, party_kind, party_code, party_name, tax_code, source_namespace
)
SELECT
    '{p6}',
    party_key,
    FIRST(party_kind ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST, tax_code NULLS LAST, party_kind),
    FIRST(party_code ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST, tax_code NULLS LAST, party_kind),
    FIRST(party_name ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST, tax_code NULLS LAST, party_kind),
    FIRST(tax_code ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST, tax_code NULLS LAST, party_kind),
    FIRST(source_namespace ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST, tax_code NULLS LAST, party_kind)
FROM _gtmp_all_parties
GROUP BY party_key;
