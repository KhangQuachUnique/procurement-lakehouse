INSERT INTO {p0}bridge_evidence (
    release_id, evidence_set_key, evidence_role, source_table, source_revision_id, source_id, source_version, observed_at, source_date
)
SELECT DISTINCT
    '{p1}',
    'EVD:AWARD:' || r.revision_id,
    'award_source',
    'result_revision',
    r.revision_id,
    r.evidence_source_id,
    r.evidence_source_version,
    r.obs_time,
    r.evidence_source_date
FROM _gtmp_cand_result r;
