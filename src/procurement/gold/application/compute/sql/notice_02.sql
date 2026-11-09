INSERT INTO {p0}bridge_evidence (
    release_id, evidence_set_key, evidence_role, source_table, source_revision_id, source_id, source_version, observed_at, source_date
)
SELECT
    '{p1}',
    'EVD:NOTICE:' || n.revision_id,
    'notice_source',
    'notice_revision',
    n.revision_id,
    n.evidence_source_id,
    n.evidence_source_version,
    n.obs_time,
    n.evidence_source_date
FROM _gtmp_cand_notice n;
