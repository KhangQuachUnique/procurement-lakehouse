CREATE TEMP TABLE _gtmp_cand_package AS
WITH ranked AS (
    SELECT
        p.*,
        COALESCE(json_extract_string(p.root_json, '$.id'), p.entity_id) AS raw_package_id,
        {p0} AS numeric_ver,
        {p1} AS observed_at_provenance,
        o.source_id AS evidence_source_id,
        o.source_version AS evidence_source_version,
        o.source_date AS evidence_source_date,
        ROW_NUMBER() OVER (
            PARTITION BY p.entity_id
            ORDER BY
                {p2} DESC,
                {p3} DESC NULLS LAST,
                p.public_date DESC NULLS LAST,
                p.revision_id DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY p.entity_id, {p4}, {p5}
        ) AS tie_count,
        (MIN(p.bid_price) OVER (PARTITION BY p.entity_id) != MAX(p.bid_price) OVER (PARTITION BY p.entity_id)) AS price_conflict
    FROM {p6} p
    {p7}
    WHERE {p8}
)
SELECT *,
    CASE
        WHEN tie_count > 1 AND price_conflict THEN 'tie_conflict'
        ELSE 'resolved_deterministic'
    END AS package_resolution_state
FROM ranked
WHERE rn = 1;
