INSERT INTO {p0}bridge_evidence (
    release_id, evidence_set_key, evidence_role, source_table, source_revision_id, source_id, source_version, observed_at, source_date
)
SELECT
    '{p1}',
    sb.baseline_evidence_set_key,
    'source_declared_lot_price_baseline',
    'award_lot',
    sb.baseline_revision_ref,
    sb.award_lot_id,
    sb.lot_source_version,
    sb.result_obs_time,
    sb.lot_source_date
FROM _gtmp_scope_baselines sb
WHERE sb.baseline_basis = 'source_declared_lot_price'
  AND sb.baseline_evidence_set_key IS NOT NULL
UNION ALL
SELECT
    '{p2}',
    sb.baseline_evidence_set_key,
    'planned_package_baseline',
    'package_revision',
    sb.baseline_revision_ref,
    sb.pkg_source_id,
    sb.pkg_source_version,
    sb.pkg_obs_time,
    sb.pkg_source_date
FROM _gtmp_scope_baselines sb
WHERE sb.baseline_basis = 'planned_package_bid_price'
  AND sb.baseline_evidence_set_key IS NOT NULL;
