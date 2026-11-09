INSERT INTO {p0}dim_procurement_method (
    method_key, mapping_version, bid_form_raw, bid_mode_raw, is_domestic, is_internet, contract_type_raw, method_category
)
SELECT 'METHOD_UNKNOWN', '{p1}', NULL, NULL, NULL, NULL, NULL, 'Chưa xác định'
WHERE NOT EXISTS (
    SELECT 1 FROM {p2}dim_procurement_method
    WHERE method_key = 'METHOD_UNKNOWN' AND mapping_version = '{p3}'
);

CREATE TEMP TABLE _gtmp_all_methods AS
SELECT bid_form, bid_mode, is_domestic, is_internet, contract_type FROM _gtmp_cand_package
UNION
SELECT bid_form, bid_mode, is_domestic, is_internet, contract_type FROM _gtmp_cand_notice;

INSERT INTO {p4}dim_procurement_method (
    method_key, mapping_version, bid_form_raw, bid_mode_raw, is_domestic, is_internet, contract_type_raw, method_category
)
SELECT DISTINCT
    'MTH:' || MD5(
        COALESCE(m.bid_form, '') || '|' ||
        COALESCE(m.bid_mode, '') || '|' ||
        COALESCE(m.is_domestic::VARCHAR, '') || '|' ||
        COALESCE(m.is_internet::VARCHAR, '') || '|' ||
        COALESCE(m.contract_type, '') || '|' ||
        '{p5}'
    ) AS method_key,
    '{p6}' AS mapping_version,
    m.bid_form,
    m.bid_mode,
    m.is_domestic,
    m.is_internet,
    m.contract_type,
    COALESCE(m.bid_form, 'Chưa xác định') AS method_category
FROM _gtmp_all_methods m
WHERE NOT EXISTS (
    SELECT 1 FROM {p7}dim_procurement_method dm
    WHERE dm.method_key = 'MTH:' || MD5(
        COALESCE(m.bid_form, '') || '|' ||
        COALESCE(m.bid_mode, '') || '|' ||
        COALESCE(m.is_domestic::VARCHAR, '') || '|' ||
        COALESCE(m.is_internet::VARCHAR, '') || '|' ||
        COALESCE(m.contract_type, '') || '|' ||
        '{p8}'
    ) AND dm.mapping_version = '{p9}'
);
