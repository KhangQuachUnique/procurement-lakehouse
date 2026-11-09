INSERT INTO {p0}dim_scope
SELECT DISTINCT '{p1}', rpm.package_key || ':PACKAGE:ROUND:' || COALESCE(n.round_business_key, 'RESULT:' || r.entity_id),
    rpm.package_key, 'PACKAGE', NULL, 'Toàn gói thầu', NULL,
    CASE WHEN n.round_business_key IS NULL THEN 'unresolved_result_round' ELSE 'resolved_package_round' END
FROM _gtmp_cand_result r
JOIN _gtmp_result_package_match rpm ON rpm.revision_id=r.revision_id
LEFT JOIN (SELECT notify_no, MIN(round_business_key) AS round_business_key FROM _gtmp_cand_notice GROUP BY notify_no HAVING COUNT(*)=1) n ON n.notify_no=r.notify_no
WHERE NOT EXISTS (SELECT 1 FROM {p2} al WHERE al.revision_id=r.revision_id);
