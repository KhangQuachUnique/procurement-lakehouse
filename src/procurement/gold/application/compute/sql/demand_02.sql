INSERT INTO {p0}bridge_location (
    release_id, location_set_key, geography_key, location_type, attribution_weight
)
SELECT '{p1}', 'LOC_SET:UNKNOWN', 'GEO_UNKNOWN', 'unknown', NULL
WHERE NOT EXISTS (
    SELECT 1 FROM {p2}bridge_location
    WHERE release_id = '{p3}' AND location_set_key = 'LOC_SET:UNKNOWN'
);
