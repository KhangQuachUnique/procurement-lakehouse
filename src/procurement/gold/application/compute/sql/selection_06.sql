CREATE TEMP TABLE _gtmp_cand_notice AS
WITH ranked AS (
    SELECT
        n.*,
        COALESCE(n.source_family, 'VN') AS notice_family_clean,
        COALESCE(TRY_CAST(regexp_extract(COALESCE(n.notify_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) AS numeric_ver,
        {p0} AS obs_time,
        o.source_id AS evidence_source_id,
        o.source_version AS evidence_source_version,
        o.source_date AS evidence_source_date,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(n.source_family, 'VN'), COALESCE(n.notify_no, n.entity_id)
            ORDER BY
                COALESCE(TRY_CAST(regexp_extract(COALESCE(n.notify_version, '0'), '([0-9]+)', 1) AS BIGINT), 0) DESC,
                n.published_at DESC NULLS LAST,
                n.public_date DESC NULLS LAST,
                n.revision_id DESC
        ) AS rn
    FROM {p1} n
    {p2}
    WHERE {p3}
)
SELECT
    *,
    'NOT:' || notice_family_clean || ':' || COALESCE(notify_no, entity_id) AS notice_business_key,
    'RND:NOT:' || notice_family_clean || ':' || COALESCE(notify_no, entity_id) AS round_business_key
FROM ranked
WHERE rn = 1;
