"""Tier 1 Atomic Fact: ``fact_award`` + ``dim_party`` + ``bridge_party_member`` + ``bridge_award_recipient``.

Includes the DQ fix mapping both ``'CANCELED'`` and ``'CANCELLED'`` cleanly to ``'cancelled'``
and uses the native Spark 4.0 SQL UDF ``recover_award_amount(...)``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement.gold.core.dimensions_base import GoldBuildConfig
from procurement.gold.functions import recover_award_amount_sql


def build_award(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Compute award intermediate views, ``core_dim_party``, bridges, and ``core_fact_award``."""
    award_lot = f"{view_prefix}award_lot"
    award_ctr = f"{view_prefix}award_contractor"
    rel_id = config.release_id
    build_ts = f"{config.observation_day.isoformat()} 00:00:00"

    # 1. Unified lot-level + whole-package award items
    award_items = spark.sql(
        f"""
        WITH unique_notice_round AS (
            SELECT notify_no, MIN(round_business_key) AS round_business_key
            FROM _gtmp_cand_notice
            WHERE notify_no IS NOT NULL
            GROUP BY notify_no
            HAVING COUNT(*) = 1
        )
        SELECT
            r.revision_id AS result_revision_id,
            rpm.package_key,
            CONCAT(
                rpm.package_key, ':LOT:', al.lot_no,
                ':REVISION:', al.award_lot_id,
                ':ROUND:', COALESCE(n.round_business_key, CONCAT('RESULT:', r.entity_id))
            ) AS scope_key,
            al.award_lot_id,
            CAST(al.winning_price AS DECIMAL(38,6)) AS award_amount,
            CAST(al.lot_price AS DECIMAL(38,6)) AS candidate_lot_price,
            al.currency AS lot_currency,
            COALESCE(al.currency, r.currency) AS currency,
            r.public_date AS award_published_date,
            r.decision_date AS award_decision_date,
            CONCAT('AWARD:', r.revision_id, ':LOT:', al.award_lot_id) AS award_business_key,
            r.status_raw,
            r.notify_no
        FROM _gtmp_cand_result r
        JOIN _gtmp_result_package_match rpm ON r.revision_id = rpm.revision_id
        JOIN {award_lot} al ON r.revision_id = al.revision_id
        LEFT JOIN unique_notice_round n ON n.notify_no = r.notify_no
        WHERE al.lot_no IS NOT NULL AND TRIM(al.lot_no) != ''

        UNION ALL

        SELECT
            r.revision_id AS result_revision_id,
            rpm.package_key,
            CONCAT(
                rpm.package_key, ':PACKAGE:ROUND:',
                COALESCE(n.round_business_key, CONCAT('RESULT:', r.entity_id))
            ) AS scope_key,
            CAST(NULL AS STRING) AS award_lot_id,
            CAST(r.total_winning_price AS DECIMAL(38,6)) AS award_amount,
            CAST(NULL AS DECIMAL(38,6)) AS candidate_lot_price,
            r.currency AS lot_currency,
            r.currency,
            r.public_date AS award_published_date,
            r.decision_date AS award_decision_date,
            CONCAT('AWARD:', r.revision_id, ':PKG') AS award_business_key,
            r.status_raw,
            r.notify_no
        FROM _gtmp_cand_result r
        JOIN _gtmp_result_package_match rpm ON r.revision_id = rpm.revision_id
        LEFT JOIN unique_notice_round n ON n.notify_no = r.notify_no
        LEFT ANTI JOIN {award_lot} al ON al.revision_id = r.revision_id
        """
    )
    award_items.createOrReplaceTempView("_gtmp_award_items")

    # 2. Winning contractors (bid_result = 1)
    winners = spark.sql(
        f"""
        SELECT
            ai.award_business_key,
            ai.scope_key,
            c.award_contractor_id,
            c.contractor_code,
            c.contractor_name,
            c.tax_code,
            c.is_consortium,
            c.venture_code,
            c.venture_name,
            c.awarded_price,
            c.role
        FROM _gtmp_award_items ai
        JOIN {award_ctr} c
          ON c.revision_id = ai.result_revision_id AND c.award_lot_id = ai.award_lot_id
        WHERE ai.award_lot_id IS NOT NULL AND c.bid_result = 1

        UNION ALL

        SELECT
            ai.award_business_key,
            ai.scope_key,
            c.award_contractor_id,
            c.contractor_code,
            c.contractor_name,
            c.tax_code,
            c.is_consortium,
            c.venture_code,
            c.venture_name,
            c.awarded_price,
            c.role
        FROM _gtmp_award_items ai
        JOIN {award_ctr} c ON c.revision_id = ai.result_revision_id
        WHERE ai.award_lot_id IS NULL AND c.bid_result = 1
        """
    )
    winners.createOrReplaceTempView("_gtmp_award_contractor_winners")

    # 3. Recipient summary per award_business_key
    recipient_summary = spark.sql(
        """
        SELECT
            award_business_key,
            COUNT(DISTINCT CASE
                WHEN COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL
                THEN CONCAT('PARTY:', COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')))
                ELSE CONCAT('UNRESOLVED:', award_contractor_id)
            END) AS winner_count,
            CAST(
                MAX(CASE WHEN COALESCE(is_consortium, FALSE) OR NULLIF(TRIM(venture_code), '') IS NOT NULL THEN 1 ELSE 0 END) = 1
                AS BOOLEAN
            ) AS has_consortium,
            MIN(venture_code) AS sample_venture_code,
            MIN(contractor_code) AS sample_contractor_code,
            MIN(tax_code) AS sample_tax_code,
            MIN(contractor_name) AS sample_contractor_name
        FROM _gtmp_award_contractor_winners
        GROUP BY award_business_key
        """
    )
    recipient_summary.createOrReplaceTempView("_gtmp_recipient_summary")

    # 4. Award amount recovery + state classification + Tier 3 eligibility gate
    recover_expr = recover_award_amount_sql("sdr.raw_award_amount", "sdr.ref_baseline")
    award_resolved = spark.sql(
        f"""
        WITH scope_decisions_raw AS (
            SELECT
                ai.result_revision_id,
                ai.package_key,
                ai.scope_key,
                ai.award_lot_id,
                ai.award_amount AS raw_award_amount,
                ai.candidate_lot_price,
                ai.lot_currency,
                ai.currency,
                ai.award_published_date,
                ai.award_decision_date,
                ai.award_business_key,
                ai.status_raw,
                ai.notify_no,
                CASE
                    WHEN ai.candidate_lot_price IS NOT NULL AND ai.candidate_lot_price > 0
                    THEN ai.candidate_lot_price
                    WHEN ai.award_lot_id IS NULL
                         AND dem.planned_package_amount IS NOT NULL
                         AND dem.planned_package_amount > 0
                    THEN dem.planned_package_amount
                    ELSE CAST(NULL AS DECIMAL(38,6))
                END AS ref_baseline,
                COALESCE(rs.winner_count, 0) AS winner_count,
                COUNT(*) OVER (PARTITION BY ai.package_key, ai.scope_key) AS decisions_count
            FROM _gtmp_award_items ai
            LEFT JOIN _gtmp_recipient_summary rs ON ai.award_business_key = rs.award_business_key
            LEFT JOIN core_fact_procurement_demand dem ON dem.package_key = ai.package_key
        ),
        scope_decisions AS (
            SELECT
                sdr.* EXCEPT (raw_award_amount, ref_baseline),
                sdr.ref_baseline,
                {recover_expr} AS award_amount
            FROM scope_decisions_raw sdr
        )
        SELECT
            sd.* EXCEPT (ref_baseline),
            CASE
                WHEN UPPER(COALESCE(sd.status_raw, '')) IN ('CANCELED', 'CANCELLED', 'REVOKED', 'HUY_THAU', 'HUY', 'HỦY KẾT QUẢ')
                THEN 'cancelled'
                WHEN sd.decisions_count > 1 THEN 'unresolved_overlap'
                WHEN sd.winner_count = 0 THEN 'no_confirmed_winner'
                WHEN UPPER(COALESCE(sd.status_raw, '')) IN ('IS_PUBLISH', 'PUBLISHED', 'EFFECTIVE', 'HOAT_DONG')
                THEN 'effective'
                ELSE 'unresolved_status'
            END AS award_state,
            CAST(
                UPPER(COALESCE(sd.status_raw, '')) IN ('IS_PUBLISH', 'PUBLISHED', 'EFFECTIVE', 'HOAT_DONG')
                AND sd.winner_count > 0
                AND sd.award_amount IS NOT NULL
                AND sd.award_amount > 0
                AND sd.currency IS NOT NULL
                AND TRIM(sd.currency) NOT IN ('', 'UNKNOWN', 'CUR_UNKNOWN')
                AND sd.decisions_count = 1
                AND (sd.ref_baseline IS NULL OR sd.award_amount <= sd.ref_baseline * 2)
                AND sd.award_amount <= 50000000000000
                AS BOOLEAN
            ) AS is_amount_eligible
        FROM scope_decisions sd
        """
    )
    award_resolved.createOrReplaceTempView("_gtmp_award_items_resolved")

    # 5. dim_party (contractors + consortiums + plan/notice investors + procurers + system placeholders)
    dim_party = spark.sql(
        f"""
        WITH all_parties AS (
            -- Winning individual contractors
            SELECT
                CASE
                    WHEN COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL
                    THEN CONCAT('PARTY:', COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')))
                    ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:', award_contractor_id)
                END AS party_key,
                CASE
                    WHEN COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL
                    THEN 'ORGANIZATION'
                    ELSE 'UNKNOWN'
                END AS party_kind,
                COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) AS party_code,
                contractor_name AS party_name,
                tax_code,
                'silver.award_contractor' AS source_namespace
            FROM _gtmp_award_contractor_winners

            UNION ALL

            -- Consortium entities
            SELECT
                CONCAT(
                    'CON:',
                    COALESCE(NULLIF(TRIM(venture_code), ''), CONCAT('CONSORTIUM_{rel_id}_', award_business_key))
                ) AS party_key,
                'CONSORTIUM' AS party_kind,
                venture_code AS party_code,
                COALESCE(venture_name, CONCAT('Liên danh ', award_business_key)) AS party_name,
                CAST(NULL AS STRING) AS tax_code,
                'silver.award_contractor' AS source_namespace
            FROM _gtmp_award_contractor_winners
            WHERE COALESCE(is_consortium, FALSE) = TRUE OR NULLIF(TRIM(venture_code), '') IS NOT NULL

            UNION ALL

            -- Plan investors
            SELECT
                CASE
                    WHEN NULLIF(TRIM(investor_name), '') IS NOT NULL
                    THEN CONCAT('PARTY_INVESTOR_NAME:', MD5(UPPER(TRIM(investor_name))))
                    ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:PLAN_INVESTOR:', entity_id)
                END AS party_key,
                CASE WHEN NULLIF(TRIM(investor_name), '') IS NOT NULL THEN 'ORGANIZATION' ELSE 'UNKNOWN' END AS party_kind,
                CAST(NULL AS STRING) AS party_code,
                investor_name AS party_name,
                CAST(NULL AS STRING) AS tax_code,
                'silver.plan_revision' AS source_namespace
            FROM _gtmp_cand_plan

            UNION ALL

            -- Notice investors
            SELECT
                CASE
                    WHEN NULLIF(TRIM(investor_code), '') IS NOT NULL
                    THEN CONCAT('PARTY_INVESTOR:', TRIM(investor_code))
                    WHEN NULLIF(TRIM(investor_name), '') IS NOT NULL
                    THEN CONCAT('PARTY_INVESTOR_NAME:', MD5(UPPER(TRIM(investor_name))))
                    ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:NOTICE_INVESTOR:', entity_id)
                END AS party_key,
                CASE
                    WHEN COALESCE(NULLIF(TRIM(investor_code), ''), NULLIF(TRIM(investor_name), '')) IS NOT NULL
                    THEN 'ORGANIZATION'
                    ELSE 'UNKNOWN'
                END AS party_kind,
                NULLIF(TRIM(investor_code), '') AS party_code,
                investor_name AS party_name,
                CAST(NULL AS STRING) AS tax_code,
                'silver.notice_revision' AS source_namespace
            FROM _gtmp_cand_notice

            UNION ALL

            -- Notice procurers
            SELECT
                CASE
                    WHEN NULLIF(TRIM(procuring_entity_code), '') IS NOT NULL
                    THEN CONCAT('PARTY_PROCURER:', TRIM(procuring_entity_code))
                    ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:NOTICE_PROCURER:', entity_id)
                END AS party_key,
                CASE WHEN NULLIF(TRIM(procuring_entity_code), '') IS NOT NULL THEN 'ORGANIZATION' ELSE 'UNKNOWN' END AS party_kind,
                procuring_entity_code AS party_code,
                procuring_entity_name AS party_name,
                CAST(NULL AS STRING) AS tax_code,
                'silver.notice_revision' AS source_namespace
            FROM _gtmp_cand_notice

            UNION ALL
            SELECT 'PARTY_BUYER:UNKNOWN', 'UNKNOWN', CAST(NULL AS STRING), 'Unknown Buyer', CAST(NULL AS STRING), 'system'
            UNION ALL
            SELECT 'PARTY_INVESTOR:UNKNOWN', 'UNKNOWN', CAST(NULL AS STRING), 'Unknown Investor', CAST(NULL AS STRING), 'system'
            UNION ALL
            SELECT 'PARTY_PROCURER:UNKNOWN', 'UNKNOWN', CAST(NULL AS STRING), 'Unknown Procurer', CAST(NULL AS STRING), 'system'
            UNION ALL
            SELECT 'REC_NO_WINNER', 'UNKNOWN', 'NO_WINNER', 'Không có nhà thầu trúng thầu', CAST(NULL AS STRING), 'system'
            UNION ALL
            SELECT 'REC_MULTIPLE_INDEPENDENT', 'UNKNOWN', 'INDEPENDENT_WINNERS', 'Nhiều nhà thầu độc lập trúng thầu', CAST(NULL AS STRING), 'system'
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY party_key
                    ORDER BY
                        source_namespace,
                        party_name NULLS LAST,
                        party_code NULLS LAST,
                        tax_code NULLS LAST,
                        party_kind
                ) AS rn
            FROM all_parties
        )
        SELECT
            '{rel_id}' AS release_id,
            party_key,
            party_kind,
            party_code,
            party_name,
            tax_code,
            source_namespace
        FROM ranked
        WHERE rn = 1
        """
    )
    dim_party.createOrReplaceTempView("core_dim_party")

    # 6. bridge_party_member
    bridge_party_member = spark.sql(
        f"""
        WITH memberships AS (
            SELECT
                CONCAT(
                    'CON:',
                    COALESCE(NULLIF(TRIM(venture_code), ''), CONCAT('CONSORTIUM_{rel_id}_', award_business_key))
                ) AS party_key,
                CASE
                    WHEN COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')) IS NOT NULL
                    THEN CONCAT('PARTY:', COALESCE(NULLIF(TRIM(contractor_code), ''), NULLIF(TRIM(tax_code), '')))
                    ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:', award_contractor_id)
                END AS member_party_key,
                role,
                tax_code,
                contractor_name
            FROM _gtmp_award_contractor_winners
            WHERE COALESCE(is_consortium, FALSE) = TRUE OR NULLIF(TRIM(venture_code), '') IS NOT NULL
        )
        SELECT
            '{rel_id}' AS release_id,
            party_key,
            member_party_key,
            CASE WHEN COUNT(DISTINCT role) = 1 THEN MIN(role) END AS member_role,
            CASE WHEN COUNT(DISTINCT tax_code) = 1 THEN MIN(tax_code) END AS member_tax_code,
            CASE WHEN COUNT(DISTINCT contractor_name) = 1 THEN MIN(contractor_name) END AS member_name
        FROM memberships
        GROUP BY party_key, member_party_key
        """
    )
    bridge_party_member.createOrReplaceTempView("core_bridge_party_member")

    # 7. bridge_award_recipient (single recipient = 100% verified; consortium/multi = unallocated)
    bridge_award_recipient = spark.sql(
        f"""
        SELECT DISTINCT
            '{rel_id}' AS release_id,
            w.award_business_key,
            w.scope_key,
            CASE
                WHEN COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), '')) IS NOT NULL
                THEN CONCAT('PARTY:', COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), '')))
                ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:', w.award_contractor_id)
            END AS party_key,
            CAST(ai.award_amount AS DECIMAL(38,6)) AS attributed_amount,
            CAST(1.0 AS DECIMAL(10,4)) AS attribution_share,
            'verified_amount' AS allocation_state
        FROM _gtmp_award_contractor_winners w
        JOIN _gtmp_recipient_summary rs ON w.award_business_key = rs.award_business_key
        JOIN _gtmp_award_items_resolved ai ON w.award_business_key = ai.award_business_key
        WHERE rs.winner_count = 1 AND NOT rs.has_consortium

        UNION ALL

        SELECT DISTINCT
            '{rel_id}' AS release_id,
            w.award_business_key,
            w.scope_key,
            CASE
                WHEN COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), '')) IS NOT NULL
                THEN CONCAT('PARTY:', COALESCE(NULLIF(TRIM(w.contractor_code), ''), NULLIF(TRIM(w.tax_code), '')))
                ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:', w.award_contractor_id)
            END AS party_key,
            CAST(NULL AS DECIMAL(38,6)) AS attributed_amount,
            CAST(NULL AS DECIMAL(10,4)) AS attribution_share,
            'unallocated' AS allocation_state
        FROM _gtmp_award_contractor_winners w
        JOIN _gtmp_recipient_summary rs ON w.award_business_key = rs.award_business_key
        WHERE rs.has_consortium OR rs.winner_count > 1
        """
    )
    bridge_award_recipient.createOrReplaceTempView("core_bridge_award_recipient")

    # 8. fact_award
    fact_award = spark.sql(
        f"""
        WITH unique_notice_round AS (
            SELECT notify_no, MIN(round_business_key) AS round_business_key
            FROM _gtmp_cand_notice
            WHERE notify_no IS NOT NULL
            GROUP BY notify_no
            HAVING COUNT(*) = 1
        )
        SELECT
            '{rel_id}' AS release_id,
            ai.award_business_key,
            ai.scope_key,
            ai.package_key,
            COALESCE(dem.sector_key, ftn.sector_key, 'SEC_UNKNOWN') AS sector_key,
            COALESCE(dem.method_key, ftn.method_key, 'METHOD_UNKNOWN') AS method_key,
            COALESCE(dem.package_value_band_key, ftn.package_value_band_key, 'BAND_RAW') AS package_value_band_key,
            COALESCE(dem.investor_party_key, ftn.investor_party_key, 'PARTY_INVESTOR:UNKNOWN') AS investor_party_key,
            COALESCE(dem.procurer_party_key, ftn.procurer_party_key, 'PARTY_BUYER:UNKNOWN') AS procurer_party_key,
            CASE
                WHEN ai.currency IS NULL OR TRIM(ai.currency) IN ('', 'UNKNOWN', 'None', 'CUR_UNKNOWN')
                THEN 'CUR_UNKNOWN'
                ELSE UPPER(TRIM(ai.currency))
            END AS currency_key,
            ai.award_published_date AS award_published_date_key,
            ai.award_decision_date AS award_decision_date_key,
            CASE WHEN ai.is_amount_eligible THEN ai.award_amount ELSE CAST(NULL AS DECIMAL(38,6)) END AS award_amount,
            ai.award_state,
            ai.is_amount_eligible,
            not_sub.round_business_key,
            CASE
                WHEN rs.winner_count = 0 OR rs.winner_count IS NULL THEN 'REC_NO_WINNER'
                WHEN rs.has_consortium THEN CONCAT(
                    'CON:',
                    COALESCE(NULLIF(TRIM(rs.sample_venture_code), ''), CONCAT('CONSORTIUM_{rel_id}_', ai.award_business_key))
                )
                WHEN rs.winner_count = 1 THEN (
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(rs.sample_contractor_code), ''), NULLIF(TRIM(rs.sample_tax_code), '')) IS NOT NULL
                        THEN CONCAT('PARTY:', COALESCE(NULLIF(TRIM(rs.sample_contractor_code), ''), NULLIF(TRIM(rs.sample_tax_code), '')))
                        ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:', ai.award_business_key)
                    END
                )
                ELSE 'REC_MULTIPLE_INDEPENDENT'
            END AS recipient_set_key,
            CASE
                WHEN rs.winner_count = 0 OR rs.winner_count IS NULL THEN 'no_winner'
                WHEN rs.has_consortium THEN 'unallocated_consortium'
                WHEN rs.winner_count = 1 THEN 'single_recipient'
                ELSE 'independent_winners'
            END AS recipient_allocation_state,
            COALESCE(
                CASE WHEN dem.reporting_geography_key != 'GEO_UNKNOWN' THEN dem.reporting_geography_key END,
                ftn.reporting_geography_key,
                dem.reporting_geography_key,
                'GEO_UNKNOWN'
            ) AS reporting_geography_key,
            COALESCE(
                CASE WHEN dem.location_set_key != 'LOC_SET:UNKNOWN' THEN dem.location_set_key END,
                ftn.location_set_key,
                dem.location_set_key,
                'LOC_SET:UNKNOWN'
            ) AS location_set_key,
            CONCAT('EVD:AWARD:', ai.result_revision_id) AS evidence_set_key,
            CAST('{build_ts}' AS TIMESTAMP) AS created_at
        FROM _gtmp_award_items_resolved ai
        LEFT JOIN core_fact_procurement_demand dem ON dem.package_key = ai.package_key
        LEFT JOIN _gtmp_recipient_summary rs ON ai.award_business_key = rs.award_business_key
        LEFT JOIN unique_notice_round not_sub ON ai.notify_no = not_sub.notify_no
        LEFT JOIN core_fact_tender_notice ftn ON ftn.round_business_key = not_sub.round_business_key
        """
    )
    fact_award.createOrReplaceTempView("core_fact_award")

    return {
        "dim_party": dim_party,
        "bridge_party_member": bridge_party_member,
        "bridge_award_recipient": bridge_award_recipient,
        "fact_award": fact_award,
    }
