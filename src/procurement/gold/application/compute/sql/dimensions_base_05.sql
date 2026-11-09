INSERT INTO {p0}dim_currency
SELECT currency_key, currency_key, FALSE, FALSE FROM (
    SELECT DISTINCT UPPER(TRIM(currency)) AS currency_key FROM (
        SELECT currency FROM {p1} UNION ALL SELECT currency FROM {p2}
        UNION ALL SELECT currency FROM {p3} UNION ALL SELECT currency FROM {p4}
        UNION ALL SELECT reoffer_currency AS currency FROM {p5}
        UNION ALL SELECT json_extract_string(root_json, '$.bidGuaranteeUnit') AS currency FROM {p6}
        UNION ALL SELECT json_extract_string(root_json, '$.bidPriceUnit') AS currency FROM {p7}
    ) units WHERE currency IS NOT NULL
) units WHERE currency_key NOT IN ('', 'NONE', 'UNKNOWN', 'CUR_UNKNOWN')
  AND NOT EXISTS (SELECT 1 FROM {p8}dim_currency existing WHERE existing.currency_key=units.currency_key);
