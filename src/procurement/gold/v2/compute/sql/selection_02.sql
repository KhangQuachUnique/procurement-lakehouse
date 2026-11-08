CREATE TEMP TABLE _gtmp_pkg_geography AS
SELECT
    revision_id,
    COUNT(DISTINCT province_code) AS prov_count,
    MIN(UPPER(TRIM(province_code))) AS single_province_code,
    COALESCE(string_agg(DISTINCT UPPER(TRIM(province_code)), ',' ORDER BY UPPER(TRIM(province_code))), 'UNKNOWN') AS loc_hash
FROM {p0}
WHERE province_code IS NOT NULL AND TRIM(province_code) != ''
GROUP BY revision_id;
