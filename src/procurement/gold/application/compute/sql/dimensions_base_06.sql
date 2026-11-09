INSERT INTO {p0}dim_geography (geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown)
SELECT 'GEO_MULTI_UNALLOCATED', '{p1}', NULL, 'Đa địa bàn chưa phân bổ', 'Đa vùng', TRUE, FALSE
WHERE NOT EXISTS (
    SELECT 1 FROM {p2}dim_geography
    WHERE geography_key = 'GEO_MULTI_UNALLOCATED' AND geography_version = '{p3}'
);

INSERT INTO {p4}dim_geography (geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown)
SELECT 'GEO_UNKNOWN', '{p5}', NULL, 'Chưa xác định', 'Chưa xác định', FALSE, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM {p6}dim_geography
    WHERE geography_key = 'GEO_UNKNOWN' AND geography_version = '{p7}'
);
