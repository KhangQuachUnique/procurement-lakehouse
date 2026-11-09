CREATE TEMP TABLE _gtmp_cand_plan AS
WITH ranked AS (
    SELECT
        pl.*,
        json_extract_string(pl.root_json, '$.id') AS raw_plan_id,
        ROW_NUMBER() OVER (
            PARTITION BY pl.entity_id
            ORDER BY pl.public_date DESC NULLS LAST, pl.revision_id DESC
        ) AS rn
    FROM {p0} pl
    {p1}
    WHERE {p2}
)
SELECT * FROM ranked WHERE rn = 1;
