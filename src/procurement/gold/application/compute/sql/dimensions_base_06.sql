INSERT INTO {p0}dim_geography (geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown)
SELECT
    cg.geography_key,
    '{p1}',
    cg.province_code,
    cg.province_name,
    cg.region_name,
    cg.is_multi_geography,
    cg.is_unknown
FROM _gtmp_canonical_geography cg
WHERE NOT EXISTS (
    SELECT 1 FROM {p2}dim_geography d
    WHERE d.geography_key = cg.geography_key AND d.geography_version = '{p3}'
);
