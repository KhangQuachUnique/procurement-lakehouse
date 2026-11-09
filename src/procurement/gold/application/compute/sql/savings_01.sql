CREATE TEMP TABLE _gtmp_scope_baselines AS
SELECT
    ai.award_business_key,
    ai.package_key,
    ai.scope_key,
    ai.currency AS award_currency,
    ai.award_published_date,
    ai.award_decision_date,
    CASE
        WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
        THEN ai.candidate_lot_price
        WHEN sc.scope_type = 'PACKAGE'
             AND dem.relationship_state = 'resolved_deterministic'
             AND dem.planned_package_amount IS NOT NULL
             AND dem.planned_package_amount > 0
        THEN dem.planned_package_amount
        ELSE NULL
    END AS baseline_amount,
    CASE
        WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
        THEN 'source_declared_lot_price'
        WHEN sc.scope_type = 'PACKAGE'
             AND dem.relationship_state = 'resolved_deterministic'
             AND dem.planned_package_amount IS NOT NULL
             AND dem.planned_package_amount > 0
        THEN 'planned_package_bid_price'
        ELSE 'unavailable_baseline'
    END AS baseline_basis,
    CASE
        WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
        THEN ai.award_published_date
        WHEN sc.scope_type = 'PACKAGE'
             AND dem.relationship_state = 'resolved_deterministic'
             AND dem.planned_package_amount IS NOT NULL
             AND dem.planned_package_amount > 0
        THEN dem.package_published_date_key
        ELSE NULL
    END AS baseline_date,
    CASE
        WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
        THEN CASE
            WHEN ai.lot_currency IS NULL OR TRIM(ai.lot_currency) = '' OR UPPER(TRIM(ai.lot_currency)) IN ('UNKNOWN', 'CUR_UNKNOWN', 'NONE')
            THEN 'CUR_UNKNOWN'
            ELSE UPPER(TRIM(ai.lot_currency))
        END
        WHEN sc.scope_type = 'PACKAGE'
             AND dem.relationship_state = 'resolved_deterministic'
             AND dem.planned_package_amount IS NOT NULL
             AND dem.planned_package_amount > 0
        THEN CASE
            WHEN dem.currency_key IS NULL OR TRIM(dem.currency_key) = '' OR UPPER(TRIM(dem.currency_key)) IN ('UNKNOWN', 'CUR_UNKNOWN', 'NONE')
            THEN 'CUR_UNKNOWN'
            ELSE UPPER(TRIM(dem.currency_key))
        END
        ELSE 'CUR_UNKNOWN'
    END AS baseline_currency_key,
    CASE
        WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
        THEN ai.result_revision_id
        WHEN sc.scope_type = 'PACKAGE'
             AND dem.relationship_state = 'resolved_deterministic'
             AND dem.planned_package_amount IS NOT NULL
             AND dem.planned_package_amount > 0
        THEN dp.source_revision_id
        ELSE NULL
    END AS baseline_revision_ref,
    CASE
        WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
        THEN 'EVD:BASELINELOT:' || ai.award_business_key
        WHEN sc.scope_type = 'PACKAGE'
             AND dem.relationship_state = 'resolved_deterministic'
             AND dem.planned_package_amount IS NOT NULL
             AND dem.planned_package_amount > 0
        THEN dem.evidence_set_key
        ELSE NULL
    END AS baseline_evidence_set_key,
    ai.award_lot_id,
    r.obs_time AS result_obs_time,
    ro_lot.source_date AS lot_source_date,
    ro_pkg.source_date AS pkg_source_date,
    ro_pkg.observed_at AS pkg_obs_time,
    ro_pkg.source_id AS pkg_source_id,
    ro_pkg.source_version AS pkg_source_version,
    ro_lot.source_version AS lot_source_version
FROM _gtmp_award_items_resolved ai
LEFT JOIN {p0}dim_scope sc
  ON sc.release_id = '{p1}' AND sc.scope_key = ai.scope_key
LEFT JOIN {p2}fact_procurement_demand dem
  ON dem.release_id = '{p3}' AND dem.package_key = ai.package_key
LEFT JOIN {p4}dim_package dp
  ON dp.release_id = '{p5}' AND dp.package_key = ai.package_key
LEFT JOIN _gtmp_cand_result r
  ON r.revision_id = ai.result_revision_id
LEFT JOIN {p6} ro_lot
  ON ro_lot.observation_id = r.observation_id
LEFT JOIN {p7} p_baseline
  ON p_baseline.revision_id = dp.source_revision_id
LEFT JOIN {p8} ro_pkg
  ON ro_pkg.observation_id = p_baseline.observation_id;
