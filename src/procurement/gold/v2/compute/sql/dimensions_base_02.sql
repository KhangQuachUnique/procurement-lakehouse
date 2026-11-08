INSERT INTO {p0}dim_date
WITH bounds AS (
    SELECT
        COALESCE(MIN(d), DATE '{p1}') AS min_d,
        COALESCE(MAX(d), DATE '{p2}') AS max_d
    FROM _gtmp_all_dates
),
day_series AS (
    SELECT
        b.min_d + CAST(i AS INTEGER) AS d
    FROM bounds b, range(0, (SELECT date_diff('day', min_d, max_d) + 1 FROM bounds)) t(i)
)
SELECT
    d AS date_key,
    EXTRACT(year FROM d)::INTEGER AS year,
    EXTRACT(quarter FROM d)::INTEGER AS quarter,
    EXTRACT(month FROM d)::INTEGER AS month,
    EXTRACT(day FROM d)::INTEGER AS day,
    EXTRACT(dayofweek FROM d)::INTEGER AS day_of_week,
    (EXTRACT(dayofweek FROM d) IN (0, 6))::BOOLEAN AS is_weekend,
    EXTRACT(year FROM d) || '-Q' || EXTRACT(quarter FROM d) AS year_quarter,
    strftime(d, '%Y-%m') AS year_month
FROM day_series
WHERE d NOT IN (SELECT date_key FROM {p3}dim_date);
