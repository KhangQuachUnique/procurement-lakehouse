INSERT INTO {p0}dim_geography (geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown)
SELECT
    'GEO_' || UPPER(TRIM(province_code)),
    '{p1}',
    UPPER(TRIM(province_code)),
    CASE WHEN COUNT(DISTINCT NULLIF(TRIM(province_name), '')) = 1 THEN MIN(NULLIF(TRIM(province_name), '')) ELSE NULL END,
    NULL,
    FALSE,
    FALSE
FROM {p2}
WHERE province_code IS NOT NULL AND TRIM(province_code) != ''
  AND NOT EXISTS (
    SELECT 1 FROM {p3}dim_geography g
    WHERE g.geography_key = 'GEO_' || UPPER(TRIM({p4}.province_code))
      AND g.geography_version = '{p5}'
  )
GROUP BY UPPER(TRIM(province_code));
