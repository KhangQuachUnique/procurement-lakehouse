INSERT INTO {p0}dim_date
WITH in_scope_bounds AS (
    SELECT
        DATE '{p1}' AS scope_start,
        GREATEST(DATE '{p2}', COALESCE(MAX(d), DATE '{p2}')) AS scope_end
    FROM _gtmp_all_dates
),
dense_days AS (
    SELECT
        b.scope_start + CAST(i AS INTEGER) AS d
    FROM in_scope_bounds b,
         range(0, (SELECT date_diff('day', scope_start, scope_end) + 1 FROM in_scope_bounds)) t(i)
),
sparse_historical_days AS (
    SELECT DISTINCT d
    FROM _gtmp_all_dates
    WHERE d < DATE '{p1}'
),
all_target_days AS (
    SELECT d FROM dense_days
    UNION
    SELECT d FROM sparse_historical_days
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
FROM all_target_days
WHERE d NOT IN (SELECT date_key FROM {p3}dim_date);
