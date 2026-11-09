INSERT INTO {p0}dim_package (
    release_id, package_key, package_no, package_name,
    plan_id, plan_no, project_id, project_no,
    source_id, source_revision_id, source_version, resolution_state
)
SELECT
    '{p1}',
    'PKG:' || p.entity_id AS package_key,
    p.package_no,
    p.title AS package_name,
    p.plan_id,
    p.plan_no,
    pl.project_id,
    pl.project_no,
    p.raw_package_id AS source_id,
    p.revision_id AS source_revision_id,
    p.numeric_ver::VARCHAR AS source_version,
    p.package_resolution_state
FROM _gtmp_cand_package p
{p2};

-- Context-scoped placeholders for unmatched results
INSERT INTO {p3}dim_package (
    release_id, package_key, package_no, package_name,
    plan_id, plan_no, project_id, project_no,
    source_id, source_revision_id, source_version, resolution_state
)
SELECT DISTINCT
    '{p4}',
    rpm.package_key,
    rpm.res_package_no,
    'Gói thầu chưa giải quyết cho kết quả ' || rpm.result_entity_id,
    NULL, NULL, NULL, NULL,
    rpm.bid_id,
    rpm.revision_id,
    NULL,
    rpm.package_resolution_state
FROM (SELECT * FROM _gtmp_result_package_match UNION ALL SELECT * FROM _gtmp_notice_package_match) rpm
WHERE rpm.package_key NOT IN (SELECT package_key FROM {p5}dim_package WHERE release_id = '{p6}');
