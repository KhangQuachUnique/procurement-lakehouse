CREATE TEMP TABLE _gtmp_cand_result AS
WITH ranked AS (
    SELECT
        r.*,
        COALESCE(TRY_CAST(regexp_extract(COALESCE(r.result_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) AS numeric_ver,
        {p0} AS obs_time,
        o.source_id AS evidence_source_id,
        o.source_version AS evidence_source_version,
        o.source_date AS evidence_source_date,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(r.result_id, r.entity_id)
            ORDER BY
                COALESCE(TRY_CAST(regexp_extract(COALESCE(r.result_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) DESC,
                {p1} DESC NULLS LAST,
                r.public_date DESC NULLS LAST,
                r.revision_id DESC
        ) AS rn
    FROM {p2} r
    {p3}
    WHERE {p4}
)
SELECT *
FROM ranked
WHERE rn = 1;
