CREATE TEMP TABLE _gtmp_package_lot_counts AS
SELECT package_key, split_part(scope_key, ':ROUND:', 2) AS scope_round_key, COUNT(*) AS lot_count
FROM _gtmp_award_items_resolved
WHERE scope_key LIKE '%:LOT:%'
GROUP BY package_key, scope_round_key;
