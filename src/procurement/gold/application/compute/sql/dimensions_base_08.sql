INSERT INTO {p0}dim_sector (sector_key, taxonomy_version, sector_code, sector_name, sector_group)
SELECT 'SEC_UNKNOWN', '{p1}', 'UNKNOWN', 'Chưa xác định', NULL
WHERE NOT EXISTS (
    SELECT 1 FROM {p2}dim_sector
    WHERE sector_key = 'SEC_UNKNOWN' AND taxonomy_version = '{p3}'
);

INSERT INTO {p4}dim_sector (sector_key, taxonomy_version, sector_code, sector_name, sector_group)
SELECT DISTINCT
    'SEC_' || COALESCE(NULLIF(TRIM(bid_field), ''), 'UNKNOWN'),
    '{p5}',
    COALESCE(NULLIF(TRIM(bid_field), ''), 'UNKNOWN'),
    CASE UPPER(COALESCE(NULLIF(TRIM(bid_field), ''), 'UNKNOWN'))
        WHEN 'HH' THEN 'Hàng hóa'
        WHEN 'XL' THEN 'Xây lắp'
        WHEN 'TV' THEN 'Tư vấn'
        WHEN 'PTV' THEN 'Phi tư vấn'
        WHEN 'HON_HOP' THEN 'Hỗn hợp'
        WHEN 'UNKNOWN' THEN 'Chưa xác định'
        ELSE COALESCE(NULLIF(TRIM(bid_field), ''), 'Chưa xác định')
    END,
    CASE UPPER(COALESCE(NULLIF(TRIM(bid_field), ''), 'UNKNOWN'))
        WHEN 'HH' THEN 'Mua sắm hàng hóa'
        WHEN 'XL' THEN 'Đầu tư xây dựng'
        WHEN 'TV' THEN 'Dịch vụ tư vấn'
        WHEN 'PTV' THEN 'Dịch vụ phi tư vấn'
        WHEN 'HON_HOP' THEN 'Đa lĩnh vực'
        WHEN 'UNKNOWN' THEN NULL
        ELSE 'Chưa phân nhóm'
    END
FROM (SELECT bid_field FROM {p6} UNION SELECT bid_field FROM {p7}) observed
WHERE NOT EXISTS (
    SELECT 1 FROM {p8}dim_sector s
    WHERE s.sector_key = 'SEC_' || COALESCE(NULLIF(TRIM(observed.bid_field), ''), 'UNKNOWN')
      AND s.taxonomy_version = '{p9}'
);
