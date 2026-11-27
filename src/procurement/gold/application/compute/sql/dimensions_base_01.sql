CREATE TEMP TABLE _gtmp_all_dates AS
SELECT d FROM (
    SELECT public_date AS d FROM {p0} WHERE public_date IS NOT NULL
    UNION
    SELECT public_date FROM {p1} WHERE public_date IS NOT NULL
    UNION
    SELECT public_date FROM {p2} WHERE public_date IS NOT NULL
    UNION
    SELECT CAST(bid_close_date AS DATE) FROM {p3} WHERE bid_close_date IS NOT NULL
    UNION
    SELECT decision_date FROM {p4} WHERE decision_date IS NOT NULL
    UNION
    SELECT CAST(cancelled_at AS DATE) FROM {p5} WHERE cancelled_at IS NOT NULL
    {p6}
) raw_dates
WHERE d >= DATE '2000-01-01' AND d <= DATE '2035-12-31';
