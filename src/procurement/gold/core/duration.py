"""Tier 1 Atomic Fact: ``fact_selection_round`` (grain = round_business_key) — 7 procurement durations."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement.gold.core.dimensions_base import GoldBuildConfig


def build_duration(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Compute ``core_fact_selection_round`` across all 7 lifecycle duration metrics."""
    open_rev = f"{view_prefix}opening_revision"
    contract_rev = f"{view_prefix}contract_revision"
    contract_perf = f"{view_prefix}contract_performance_revision"
    rel_id = config.release_id
    build_ts = f"{config.observation_day.isoformat()} 00:00:00"

    # 1. Notice & Demand endpoints per round_business_key
    spark.sql(
        """
        SELECT
            n.round_business_key,
            MIN(n.package_key) AS package_key,
            COALESCE(MIN(dem.sector_key), MIN(n.sector_key)) AS sector_key,
            COALESCE(MIN(dem.method_key), MIN(n.method_key)) AS method_key,
            COALESCE(MIN(dem.package_value_band_key), MIN(n.package_value_band_key)) AS package_value_band_key,
            COALESCE(MIN(dem.investor_party_key), MIN(n.investor_party_key)) AS investor_party_key,
            COALESCE(MIN(dem.procurer_party_key), MIN(n.procurer_party_key)) AS procurer_party_key,
            COALESCE(
                MIN(CASE WHEN dem.reporting_geography_key != 'GEO_UNKNOWN' THEN dem.reporting_geography_key END),
                MIN(n.reporting_geography_key),
                MIN(dem.reporting_geography_key)
            ) AS reporting_geography_key,
            COALESCE(
                MIN(CASE WHEN dem.location_set_key != 'LOC_SET:UNKNOWN' THEN dem.location_set_key END),
                MIN(n.location_set_key),
                MIN(dem.location_set_key)
            ) AS location_set_key,
            MIN(dem.package_published_date_key) AS plan_ref_date,
            MIN(n.published_date_key) AS first_notice_date,
            MIN(n.published_at) AS first_notice_published_at,
            CASE WHEN COUNT(DISTINCT n.close_date_key) = 1 THEN MIN(n.close_date_key) ELSE CAST(NULL AS DATE) END AS close_date,
            CASE WHEN COUNT(DISTINCT n.close_at) = 1 THEN MIN(n.close_at) ELSE CAST(NULL AS TIMESTAMP) END AS close_at,
            BOOL_OR(n.cancellation_state = 'cancelled') AS is_cancelled,
            MIN(n.cancel_date_key) AS cancel_date,
            MIN(n.cancel_reason_raw) AS cancel_reason,
            MIN(n.evidence_set_key) AS evidence_set_key
        FROM core_fact_tender_notice n
        LEFT JOIN core_fact_procurement_demand dem ON dem.package_key = n.package_key
        GROUP BY n.round_business_key
        """
    ).createOrReplaceTempView("_gtmp_round_endpoints")

    # 2. Award endpoints per round_business_key
    spark.sql(
        """
        SELECT
            round_business_key,
            MIN(award_decision_date_key) AS first_award_decision_date,
            MAX(award_decision_date_key) AS complete_award_decision_date,
            MIN(award_published_date_key) AS first_award_pub_date,
            MAX(award_published_date_key) AS complete_award_pub_date,
            COUNT(*) AS award_count,
            BOOL_AND(award_state = 'effective') AS all_effective
        FROM core_fact_award
        WHERE round_business_key IS NOT NULL
        GROUP BY round_business_key
        """
    ).createOrReplaceTempView("_gtmp_round_award_endpoints")

    # 3. Bid opening endpoints per round_business_key
    spark.sql(
        f"""
        WITH notice_rounds AS (
            SELECT notify_no, MIN(round_business_key) AS round_business_key
            FROM _gtmp_cand_notice
            WHERE notify_no IS NOT NULL
            GROUP BY notify_no
        )
        SELECT
            COALESCE(n.round_business_key, CONCAT('RND:NOT:VN:', o.notify_no)) AS round_business_key,
            MIN(CAST(COALESCE(o.actual_open_date, o.scheduled_open_date) AS DATE)) AS open_date,
            MIN(COALESCE(o.actual_open_date, o.scheduled_open_date)) AS open_at
        FROM {open_rev} o
        LEFT JOIN notice_rounds n ON o.notify_no = n.notify_no
        WHERE o.notify_no IS NOT NULL
        GROUP BY COALESCE(n.round_business_key, CONCAT('RND:NOT:VN:', o.notify_no))
        """
    ).createOrReplaceTempView("_gtmp_round_opening_endpoints")

    # 4. Contract performance completion endpoints per round_business_key
    spark.sql(
        f"""
        WITH notice_rounds AS (
            SELECT notify_no, MIN(round_business_key) AS round_business_key
            FROM _gtmp_cand_notice
            WHERE notify_no IS NOT NULL
            GROUP BY notify_no
        )
        SELECT
            COALESCE(n.round_business_key, CONCAT('RND:NOT:VN:', c.notice_no)) AS round_business_key,
            MIN(cp.public_date) AS contract_complete_date,
            MIN(cp.planned_completion_date) AS planned_completion_date,
            MAX(cp.actual_completion_date) AS actual_completion_date
        FROM {contract_perf} cp
        JOIN {contract_rev} c ON cp.contract_code = c.contract_code
        LEFT JOIN notice_rounds n ON c.notice_no = n.notify_no
        WHERE c.notice_no IS NOT NULL
        GROUP BY COALESCE(n.round_business_key, CONCAT('RND:NOT:VN:', c.notice_no))
        """
    ).createOrReplaceTempView("_gtmp_round_contract_endpoints")

    # 5. fact_selection_round (note Spark DATEDIFF(end, start) argument order!)
    fact_round = spark.sql(
        f"""
        SELECT
            '{rel_id}' AS release_id,
            re.round_business_key,
            re.package_key,
            re.sector_key,
            re.method_key,
            re.package_value_band_key,
            re.investor_party_key,
            re.procurer_party_key,
            re.reporting_geography_key,
            re.location_set_key,
            re.plan_ref_date AS plan_reference_date_key,
            re.first_notice_date AS first_notice_date_key,
            re.close_date AS close_date_key,
            re.first_notice_published_at AS first_notice_at,
            re.close_at,
            ra.first_award_decision_date AS first_award_decision_date_key,
            ra.complete_award_decision_date AS complete_award_decision_date_key,
            ra.first_award_pub_date AS first_award_published_date_key,
            ra.complete_award_pub_date AS complete_award_published_date_key,
            re.cancel_date AS cancel_date_key,
            -- 1. plan_to_notice_days
            CASE
                WHEN re.plan_ref_date IS NOT NULL AND re.first_notice_date IS NOT NULL
                     AND re.first_notice_date >= re.plan_ref_date
                THEN CAST(DATEDIFF(re.first_notice_date, re.plan_ref_date) AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END AS plan_to_notice_days,
            CAST(
                re.plan_ref_date IS NOT NULL AND re.first_notice_date IS NOT NULL
                AND re.first_notice_date >= re.plan_ref_date
                AS BOOLEAN
            ) AS is_plan_to_notice_eligible,
            -- 2. notice_to_close_hours
            CASE
                WHEN re.first_notice_published_at IS NOT NULL AND re.close_at IS NOT NULL
                     AND re.close_at >= re.first_notice_published_at
                THEN CAST(ROUND((UNIX_TIMESTAMP(re.close_at) - UNIX_TIMESTAMP(re.first_notice_published_at)) / 3600.0, 2) AS DECIMAL(38,6))
                ELSE CAST(NULL AS DECIMAL(38,6))
            END AS notice_to_close_hours,
            CAST(
                re.first_notice_published_at IS NOT NULL AND re.close_at IS NOT NULL
                AND re.close_at >= re.first_notice_published_at
                AS BOOLEAN
            ) AS is_notice_to_close_eligible,
            -- 3. close_to_first_award_days
            CASE
                WHEN re.close_date IS NOT NULL AND ra.first_award_pub_date IS NOT NULL
                     AND ra.first_award_pub_date >= re.close_date
                     AND (ra.first_award_decision_date IS NULL OR ra.first_award_decision_date >= re.close_date)
                THEN CAST(DATEDIFF(ra.first_award_pub_date, re.close_date) AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END AS close_to_first_award_days,
            CAST(
                re.close_date IS NOT NULL AND ra.first_award_pub_date IS NOT NULL
                AND ra.first_award_pub_date >= re.close_date
                AND (ra.first_award_decision_date IS NULL OR ra.first_award_decision_date >= re.close_date)
                AS BOOLEAN
            ) AS is_close_to_first_award_eligible,
            -- 4. close_to_complete_award_days
            CASE
                WHEN re.close_date IS NOT NULL AND ra.complete_award_pub_date IS NOT NULL
                     AND ra.complete_award_pub_date >= re.close_date
                     AND (ra.complete_award_decision_date IS NULL OR ra.complete_award_decision_date >= re.close_date)
                THEN CAST(DATEDIFF(ra.complete_award_pub_date, re.close_date) AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END AS close_to_complete_award_days,
            CAST(
                re.close_date IS NOT NULL AND ra.complete_award_pub_date IS NOT NULL
                AND ra.complete_award_pub_date >= re.close_date
                AND (ra.complete_award_decision_date IS NULL OR ra.complete_award_decision_date >= re.close_date)
                AS BOOLEAN
            ) AS is_close_to_complete_award_eligible,
            dem.planned_start_from,
            dem.planned_start_to,
            CASE
                WHEN dem.planned_start_from IS NULL THEN 'unresolved'
                WHEN re.first_notice_date >= dem.planned_start_from AND re.first_notice_date <= dem.planned_start_to THEN 'within'
                WHEN re.first_notice_date < dem.planned_start_from THEN 'before'
                ELSE 'after'
            END AS start_timing_status,
            CASE WHEN re.is_cancelled THEN 'cancelled' ELSE 'in_progress' END AS round_state,
            CASE
                WHEN ra.award_count > 0 AND ra.all_effective THEN 'partially_observed'
                WHEN ra.award_count > 0 THEN 'unresolved'
                ELSE 'not_observed'
            END AS award_completion_state,
            CASE WHEN re.is_cancelled THEN 'cancelled' ELSE 'none' END AS cancellation_state,
            re.cancel_reason AS cancel_reason_raw,
            CASE WHEN re.is_cancelled THEN 'explicit_round_cancellation' ELSE CAST(NULL AS STRING) END AS cancel_reason_category,
            re.evidence_set_key,
            CAST('{build_ts}' AS TIMESTAMP) AS created_at,
            ro.open_date AS actual_open_date_key,
            ro.open_at AS actual_open_at,
            -- 5. close_to_open_hours
            CASE
                WHEN re.close_at IS NOT NULL AND ro.open_at IS NOT NULL AND ro.open_at >= re.close_at
                THEN CAST(ROUND((UNIX_TIMESTAMP(ro.open_at) - UNIX_TIMESTAMP(re.close_at)) / 3600.0, 2) AS DECIMAL(38,6))
                ELSE CAST(NULL AS DECIMAL(38,6))
            END AS close_to_open_hours,
            CAST(re.close_at IS NOT NULL AND ro.open_at IS NOT NULL AND ro.open_at >= re.close_at AS BOOLEAN) AS is_close_to_open_eligible,
            -- 6. open_to_award_days
            CASE
                WHEN ro.open_date IS NOT NULL AND ra.complete_award_pub_date IS NOT NULL
                     AND ra.complete_award_pub_date >= ro.open_date
                THEN CAST(DATEDIFF(ra.complete_award_pub_date, ro.open_date) AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END AS open_to_award_days,
            CAST(ro.open_date IS NOT NULL AND ra.complete_award_pub_date IS NOT NULL AND ra.complete_award_pub_date >= ro.open_date AS BOOLEAN) AS is_open_to_award_eligible,
            -- 7. planned_to_actual_delay_days (can be negative = early completion)
            CASE
                WHEN rc.planned_completion_date IS NOT NULL AND rc.actual_completion_date IS NOT NULL
                THEN CAST(DATEDIFF(rc.actual_completion_date, rc.planned_completion_date) AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END AS planned_to_actual_delay_days,
            CAST(rc.planned_completion_date IS NOT NULL AND rc.actual_completion_date IS NOT NULL AS BOOLEAN) AS is_delay_eligible,
            rc.contract_complete_date AS contract_complete_date_key
        FROM _gtmp_round_endpoints re
        LEFT JOIN _gtmp_round_award_endpoints ra ON re.round_business_key = ra.round_business_key
        LEFT JOIN _gtmp_round_opening_endpoints ro ON re.round_business_key = ro.round_business_key
        LEFT JOIN _gtmp_round_contract_endpoints rc ON re.round_business_key = rc.round_business_key
        LEFT JOIN core_fact_procurement_demand dem ON dem.package_key = re.package_key
        """
    )
    fact_round.createOrReplaceTempView("core_fact_selection_round")

    return {"fact_selection_round": fact_round}
