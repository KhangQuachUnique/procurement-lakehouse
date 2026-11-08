INSERT INTO {p0}bridge_location (
    release_id, location_set_key, geography_key, location_type, attribution_weight
)
SELECT DISTINCT
    '{p1}',
    'LOC_SET:' || geo.loc_hash,
    'GEO_' || UPPER(TRIM(ploc.province_code)),
    'package_location',
    NULL
FROM {p2} ploc
JOIN _gtmp_pkg_geography geo ON ploc.revision_id = geo.revision_id
WHERE ploc.province_code IS NOT NULL AND TRIM(ploc.province_code) != '';
