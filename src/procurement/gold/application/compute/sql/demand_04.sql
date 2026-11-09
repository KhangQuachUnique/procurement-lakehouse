INSERT INTO {p0}bridge_evidence (
    release_id, evidence_set_key, evidence_role, source_table, source_revision_id, source_id, source_version, observed_at, source_date
)
SELECT
    '{p1}',
    'EVD:DEMAND:' || p.revision_id,
    'demand_source',
    'package_revision',
    p.revision_id,
    p.evidence_source_id,
    p.evidence_source_version,
    p.observed_at_provenance,
    p.evidence_source_date
FROM _gtmp_cand_package p;
