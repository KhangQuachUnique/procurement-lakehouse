INSERT INTO {p0}dim_value_band (band_key, currency_key, policy_version, min_amount, max_amount, band_label, band_order)
SELECT 'BAND_RAW', 'VND', '{p1}', 0, NULL, 'Chưa phân nhóm quy mô', 99
WHERE NOT EXISTS (
    SELECT 1 FROM {p2}dim_value_band
    WHERE band_key = 'BAND_RAW' AND currency_key = 'VND' AND policy_version = '{p3}'
);
