CREATE TEMP TABLE _gtmp_pkg_geography AS
SELECT
    pl.revision_id,
    CASE
        WHEN BOOL_OR(COALESCE(pm.is_multi, FALSE)) THEN 2
        ELSE COUNT(DISTINCT COALESCE(pm.canonical_code, CASE WHEN pm.raw_code IS NULL THEN UPPER(TRIM(pl.province_code)) ELSE NULL END))
    END AS prov_count,
    MIN(COALESCE(pm.canonical_code, CASE WHEN pm.raw_code IS NULL THEN UPPER(TRIM(pl.province_code)) ELSE NULL END)) AS single_province_code,
    COALESCE(
        string_agg(
            DISTINCT COALESCE(pm.canonical_code, CASE WHEN pm.raw_code IS NULL THEN UPPER(TRIM(pl.province_code)) ELSE NULL END),
            ',' ORDER BY COALESCE(pm.canonical_code, CASE WHEN pm.raw_code IS NULL THEN UPPER(TRIM(pl.province_code)) ELSE NULL END)
        ),
        CASE WHEN BOOL_OR(COALESCE(pm.is_multi, FALSE)) THEN 'MULTI_824' ELSE 'UNKNOWN' END
    ) AS loc_hash
FROM {p0} pl
LEFT JOIN _gtmp_province_map pm ON UPPER(TRIM(pl.province_code)) = pm.raw_code
WHERE pl.province_code IS NOT NULL AND TRIM(pl.province_code) != ''
GROUP BY pl.revision_id;
