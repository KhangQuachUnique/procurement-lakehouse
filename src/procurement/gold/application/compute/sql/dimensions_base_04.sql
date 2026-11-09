INSERT INTO {p0}dim_currency (currency_key, currency_code, is_supported, is_unknown)
SELECT '{p1}', '{p2}', {p3}, {p4}
WHERE '{p5}' NOT IN (SELECT currency_key FROM {p6}dim_currency);
