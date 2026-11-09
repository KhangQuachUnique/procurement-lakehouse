CREATE TEMP TABLE {p0} AS
SELECT r.revision_id, r.entity_id AS result_entity_id,
    {p1} AS bid_id, r.package_no AS res_package_no, r.notify_no AS res_notify_no,
    CASE WHEN resolved.entity_id IS NOT NULL THEN 'PKG:' || resolved.entity_id
         WHEN COALESCE(resolved_id.count, 0) > 1 OR COALESCE(resolved_code.count, 0) > 1
         THEN 'PKG_AMBIGUOUS:' || '{p2}:' || r.entity_id
         ELSE 'PKG_UNRESOLVED:' || '{p3}:' || r.entity_id END AS package_key,
    CASE WHEN COALESCE(resolved_id.count, 0) = 1 THEN 'resolved_exact_source_id'
         WHEN COALESCE(resolved_id.count, 0) > 1 THEN 'ambiguous_source_id'
         WHEN COALESCE(resolved_code.count, 0) = 1 THEN 'resolved_unique_business_code'
         WHEN COALESCE(resolved_code.count, 0) > 1 THEN 'ambiguous_business_code'
         ELSE 'unresolved_package' END AS package_resolution_state,
    CASE WHEN COALESCE(resolved_id.count, 0) = 1 THEN resolved.entity_id END AS direct_package_entity_id,
    CASE WHEN COALESCE(resolved_id.count, 0) = 0 AND COALESCE(resolved_code.count, 0) = 1 THEN resolved.entity_id END AS unique_code_package_entity_id,
    resolved.entity_id AS resolved_entity_id
FROM {p4} r
{p5};
