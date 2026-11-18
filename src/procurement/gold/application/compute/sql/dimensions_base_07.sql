INSERT INTO {p0}dim_geography (geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown)
SELECT
    'GEO_' || UPPER(TRIM(pl.province_code)),
    '{p1}',
    UPPER(TRIM(pl.province_code)),
    CASE WHEN COUNT(DISTINCT NULLIF(TRIM(pl.province_name), '')) = 1 THEN MIN(NULLIF(TRIM(pl.province_name), '')) ELSE NULL END,
    NULL,
    FALSE,
    FALSE
FROM {p2} pl
LEFT JOIN _gtmp_province_map pm ON UPPER(TRIM(pl.province_code)) = pm.raw_code
WHERE pl.province_code IS NOT NULL AND TRIM(pl.province_code) != ''
  AND pm.raw_code IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM {p3}dim_geography g
    WHERE g.geography_key = 'GEO_' || UPPER(TRIM(pl.province_code))
      AND g.geography_version = '{p5}'
  )
GROUP BY UPPER(TRIM(pl.province_code));
