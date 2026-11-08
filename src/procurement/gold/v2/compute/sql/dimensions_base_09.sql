INSERT INTO {p0}dim_value_band (band_key, currency_key, policy_version, min_amount, max_amount, band_label, band_order)
SELECT '{p1}', '{p2}', '{p3}', CAST('{p4}' AS DECIMAL(38,6)), {p5}, '{p6}', {p7}
WHERE NOT EXISTS (
    SELECT 1 FROM {p8}dim_value_band
    WHERE band_key = '{p9}' AND currency_key = '{p10}' AND policy_version = '{p11}'
);
