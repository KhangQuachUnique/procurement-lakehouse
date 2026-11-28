"""Tier 1 Atomic Fact: ``fact_savings`` (grain = comparison_scope_key) + ``bridge_savings_award``.

Includes the single-lot contract matching fix so contracts linked to single-lot awards
(``%:LOT:%`` with ``lot_count = 1`` and ``scope_count = 1``) are properly attributed while
multi-lot awards remain excluded from ambiguous package-level contract attribution.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement.gold.core.dimensions_base import GoldBuildConfig


def build_savings(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
    comparison_rule_version: str = "v1",
) -> dict[str, DataFrame]:
    """Compute ``core_fact_savings`` and ``core_bridge_savings_award``."""
    contract_rev = f"{view_prefix}contract_revision"
    rel_id = config.release_id
    ev_start = config.event_start.isoformat()
    ev_end = config.event_end.isoformat()
    build_ts = f"{config.observation_day.isoformat()} 00:00:00"

    # 1. Baseline per award_business_key
    scope_baselines = spark.sql(
        """
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
                ELSE CAST(NULL AS DECIMAL(38,6))
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
                ELSE CAST(NULL AS DATE)
            END AS baseline_date,
            CASE
                WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
                THEN CASE
                    WHEN ai.lot_currency IS NULL OR TRIM(ai.lot_currency) = ''
                         OR UPPER(TRIM(ai.lot_currency)) IN ('UNKNOWN', 'CUR_UNKNOWN', 'NONE')
                    THEN 'CUR_UNKNOWN'
                    ELSE UPPER(TRIM(ai.lot_currency))
                END
                WHEN sc.scope_type = 'PACKAGE'
                     AND dem.relationship_state = 'resolved_deterministic'
                     AND dem.planned_package_amount IS NOT NULL
                     AND dem.planned_package_amount > 0
                THEN CASE
                    WHEN dem.currency_key IS NULL OR TRIM(dem.currency_key) = ''
                         OR UPPER(TRIM(dem.currency_key)) IN ('UNKNOWN', 'CUR_UNKNOWN', 'NONE')
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
                ELSE CAST(NULL AS STRING)
            END AS baseline_revision_ref,
            CASE
                WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
                THEN CONCAT('EVD:BASELINELOT:', ai.award_business_key)
                WHEN sc.scope_type = 'PACKAGE'
                     AND dem.relationship_state = 'resolved_deterministic'
                     AND dem.planned_package_amount IS NOT NULL
                     AND dem.planned_package_amount > 0
                THEN dem.evidence_set_key
                ELSE CAST(NULL AS STRING)
            END AS baseline_evidence_set_key
        FROM _gtmp_award_items_resolved ai
        LEFT JOIN core_dim_scope sc ON sc.scope_key = ai.scope_key
        LEFT JOIN core_fact_procurement_demand dem ON dem.package_key = ai.package_key
        LEFT JOIN core_dim_package dp ON dp.package_key = ai.package_key
        """
    )
    scope_baselines.createOrReplaceTempView("_gtmp_scope_baselines")

    # 2. Lot counts per (package_key, scope_round_key)
    spark.sql(
        """
        SELECT
            package_key,
            SPLIT_PART(scope_key, ':ROUND:', 2) AS scope_round_key,
            COUNT(*) AS lot_count
        FROM _gtmp_award_items_resolved
        WHERE scope_key LIKE '%:LOT:%'
        GROUP BY package_key, SPLIT_PART(scope_key, ':ROUND:', 2)
        """
    ).createOrReplaceTempView("_gtmp_package_lot_counts")

    # 3. Matched contract prices per result_revision_id
    spark.sql(
        f"""
        WITH matched_contracts AS (
            SELECT r.revision_id AS result_revision_id, c.entity_id AS contract_entity_id, c.price_after_tax
            FROM {contract_rev} c
            JOIN _gtmp_cand_result r ON c.result_source_id = r.bid_id
            WHERE c.price_after_tax > 0 AND c.result_source_id IS NOT NULL
            UNION
            SELECT r.revision_id AS result_revision_id, c.entity_id AS contract_entity_id, c.price_after_tax
            FROM {contract_rev} c
            JOIN _gtmp_cand_result r ON c.result_source_id = r.result_id
            WHERE c.price_after_tax > 0 AND c.result_source_id IS NOT NULL
            UNION
            SELECT r.revision_id AS result_revision_id, c.entity_id AS contract_entity_id, c.price_after_tax
            FROM {contract_rev} c
            JOIN _gtmp_cand_result r ON c.notice_no = r.notify_no
            WHERE c.price_after_tax > 0 AND c.notice_no IS NOT NULL
        )
        SELECT
            result_revision_id,
            CAST(SUM(price_after_tax) AS DECIMAL(38,6)) AS contract_price
        FROM matched_contracts
        GROUP BY result_revision_id
        """
    ).createOrReplaceTempView("_gtmp_contract_savings")

    # 4. fact_savings (with single-lot contract savings fix)
    fact_savings = spark.sql(
        f"""
        WITH result_scope_counts AS (
            SELECT
                SPLIT_PART(award_business_key, ':', 2) AS result_revision_id,
                COUNT(*) AS scope_count
            FROM core_fact_award
            GROUP BY SPLIT_PART(award_business_key, ':', 2)
        ),
        enriched AS (
            SELECT
                fa.*,
                sb.baseline_date,
                sb.baseline_amount,
                sb.baseline_currency_key,
                sb.baseline_basis,
                sb.baseline_revision_ref,
                sb.baseline_evidence_set_key,
                COALESCE(plc.lot_count, 0) AS lot_count,
                COALESCE(rsc.scope_count, 0) AS scope_count,
                cs.contract_price,
                COALESCE(
                    fa.award_state = 'effective'
                    AND fa.is_amount_eligible
                    AND sb.baseline_amount IS NOT NULL
                    AND sb.baseline_amount > 0
                    AND sb.baseline_currency_key NOT IN ('CUR_UNKNOWN', 'UNKNOWN')
                    AND fa.currency_key NOT IN ('CUR_UNKNOWN', 'UNKNOWN')
                    AND sb.baseline_currency_key = fa.currency_key
                    AND sb.baseline_date IS NOT NULL
                    AND fa.award_published_date_key IS NOT NULL
                    AND fa.award_published_date_key >= DATE '{ev_start}'
                    AND fa.award_published_date_key <= DATE '{ev_end}'
                    AND sb.baseline_date <= fa.award_published_date_key
                    AND NOT (fa.scope_key LIKE '%:PACKAGE' AND COALESCE(plc.lot_count, 0) > 0)
                    AND fa.award_amount <= sb.baseline_amount * 2,
                    FALSE
                ) AS desc_elig,
                COALESCE(
                    fa.award_state = 'effective'
                    AND (
                        (fa.scope_key LIKE '%:PACKAGE%' AND COALESCE(plc.lot_count, 0) = 0)
                        OR (fa.scope_key LIKE '%:LOT:%' AND COALESCE(plc.lot_count, 0) = 1 AND COALESCE(rsc.scope_count, 0) = 1)
                    )
                    AND sb.baseline_amount IS NOT NULL
                    AND sb.baseline_amount > 0
                    AND cs.contract_price IS NOT NULL
                    AND cs.contract_price > 0
                    AND cs.contract_price <= sb.baseline_amount * 2,
                    FALSE
                ) AS ctr_elig
            FROM core_fact_award fa
            JOIN _gtmp_scope_baselines sb ON fa.award_business_key = sb.award_business_key
            LEFT JOIN _gtmp_package_lot_counts plc
              ON fa.package_key = plc.package_key
              AND SPLIT_PART(fa.scope_key, ':ROUND:', 2) = plc.scope_round_key
            LEFT JOIN result_scope_counts rsc
              ON SPLIT_PART(fa.award_business_key, ':', 2) = rsc.result_revision_id
            LEFT JOIN _gtmp_contract_savings cs
              ON SPLIT_PART(fa.award_business_key, ':', 2) = cs.result_revision_id
        )
        SELECT
            '{rel_id}' AS release_id,
            CONCAT('CMP:', scope_key) AS comparison_scope_key,
            package_key,
            scope_key,
            round_business_key,
            sector_key,
            method_key,
            package_value_band_key,
            investor_party_key,
            procurer_party_key,
            currency_key,
            reporting_geography_key,
            location_set_key,
            award_published_date_key AS comparison_date_key,
            baseline_date AS baseline_date_key,
            award_published_date_key AS first_award_published_date_key,
            award_published_date_key AS last_award_published_date_key,
            baseline_amount,
            baseline_currency_key,
            award_amount,
            CASE WHEN desc_elig THEN CAST(baseline_amount - award_amount AS DECIMAL(38,6)) ELSE CAST(NULL AS DECIMAL(38,6)) END AS descriptive_difference_amount,
            desc_elig AS is_descriptive_eligible,
            CASE
                WHEN award_published_date_key IS NULL THEN 'unknown_award_date'
                WHEN award_published_date_key > DATE '{ev_end}' OR award_published_date_key < DATE '{ev_start}' THEN 'result_outside_period'
                WHEN baseline_amount IS NULL OR baseline_amount <= 0 THEN 'missing_baseline'
                WHEN baseline_date IS NULL THEN 'unknown_baseline_date'
                WHEN baseline_date > award_published_date_key THEN 'baseline_after_award'
                WHEN scope_key LIKE '%:PACKAGE' AND lot_count > 0 THEN 'lot_partition_precedence'
                WHEN award_state = 'cancelled' THEN 'cancelled_award'
                WHEN award_state != 'effective' THEN 'unresolved_award'
                WHEN currency_key IN ('CUR_UNKNOWN', 'UNKNOWN') THEN 'unknown_currency'
                WHEN baseline_currency_key IN ('CUR_UNKNOWN', 'UNKNOWN') THEN 'unknown_baseline_currency'
                WHEN baseline_currency_key != currency_key THEN 'currency_mismatch'
                WHEN NOT is_amount_eligible THEN 'invalid_award_amount'
                WHEN award_amount > baseline_amount * 2 THEN 'extreme_overrun_anomaly'
                ELSE CAST(NULL AS STRING)
            END AS descriptive_exclusion_reason,
            baseline_basis AS descriptive_basis,
            CAST(NULL AS DECIMAL(38,6)) AS verified_saving_amount,
            FALSE AS is_verified_comparable,
            'unverified_tax_basis_and_scope' AS verified_exclusion_reason,
            'unknown' AS tax_basis_state,
            CAST(NULL AS DECIMAL(38,6)) AS saving_amount,
            FALSE AS is_comparable,
            'unverified_tax_basis_and_scope' AS primary_exclusion_reason,
            baseline_basis,
            baseline_revision_ref,
            baseline_evidence_set_key,
            evidence_set_key AS award_evidence_set_key,
            '{comparison_rule_version}' AS comparison_rule_version,
            award_state,
            CASE WHEN scope_key LIKE '%:PACKAGE' AND lot_count > 0 THEN 'partial' ELSE 'unverified' END AS scope_completeness,
            CAST(baseline_amount IS NOT NULL AND baseline_amount > 0 AS BOOLEAN) AS is_baseline_valid,
            evidence_set_key,
            CAST('{build_ts}' AS TIMESTAMP) AS created_at,
            CASE WHEN ctr_elig THEN contract_price ELSE CAST(NULL AS DECIMAL(38,6)) END AS contract_amount,
            CASE WHEN ctr_elig THEN CAST(baseline_amount - contract_price AS DECIMAL(38,6)) ELSE CAST(NULL AS DECIMAL(38,6)) END AS contract_saving_amount,
            ctr_elig AS is_contract_eligible
        FROM enriched
        """
    )
    fact_savings.createOrReplaceTempView("core_fact_savings")

    # 5. bridge_savings_award
    bridge_savings_award = spark.sql(
        f"""
        SELECT
            '{rel_id}' AS release_id,
            CONCAT('CMP:', scope_key) AS comparison_scope_key,
            award_business_key,
            scope_key,
            CAST(COALESCE(award_amount, 0) AS DECIMAL(38,6)) AS included_award_amount
        FROM core_fact_award
        """
    )
    bridge_savings_award.createOrReplaceTempView("core_bridge_savings_award")

    return {
        "fact_savings": fact_savings,
        "bridge_savings_award": bridge_savings_award,
    }
