INSERT INTO {p0}dim_scope (
    release_id, scope_key, package_key, scope_type, lot_no, lot_name, source_lot_id, resolution_state
)
-- Package level scope
SELECT
    '{p1}',
    package_key || ':PACKAGE' AS scope_key,
    package_key,
    'PACKAGE',
    NULL,
    'Toàn gói thầu',
    NULL,
    'resolved_package_scope'
FROM {p2}dim_package
WHERE release_id = '{p3}'
UNION ALL
-- Lot level scopes from award_lot (scoped by package AND source_lot_id!)
SELECT DISTINCT
    '{p4}',
    rpm.package_key || ':LOT:' || al.lot_no || ':' || COALESCE(NULLIF(TRIM(json_extract_string(al.raw_json, '$.id')), ''), 'REVISION:' || al.award_lot_id) || ':ROUND:' || COALESCE((SELECT MIN(nr.round_business_key) FROM _gtmp_cand_notice nr WHERE nr.notify_no = r.notify_no HAVING COUNT(*) = 1), 'RESULT:' || r.entity_id) AS scope_key,
    rpm.package_key,
    'LOT',
    al.lot_no,
    al.lot_name,
    NULLIF(TRIM(json_extract_string(al.raw_json, '$.id')), ''),
    CASE WHEN NULLIF(TRIM(json_extract_string(al.raw_json, '$.id')), '') IS NOT NULL THEN 'resolved_raw_source_lot' ELSE 'unresolved_revision_scoped_lot' END
FROM {p5} al
JOIN _gtmp_cand_result r ON al.revision_id = r.revision_id
JOIN _gtmp_result_package_match rpm ON r.revision_id = rpm.revision_id
WHERE al.lot_no IS NOT NULL AND TRIM(al.lot_no) != '';
