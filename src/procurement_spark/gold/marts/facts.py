"""Tier 2 Public Daily Facts (8 fact_*_daily tables) — pure GROUP BY over Gold Core."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement_spark.gold.core.dimensions_base import GoldBuildConfig


def _group_hash_sql(cols: tuple[str, ...]) -> str:
    """Deterministic SHA-256 grain key matching DuckDB ``sha256(to_json(list_value(...)))``."""
    casted = ", ".join(f"CAST({c} AS STRING)" for c in cols)
    return f"SHA2(TO_JSON(ARRAY({casted})), 256)"


def _vnd_band_case(amount_expr: str) -> str:
    """Assign exact DEFAULT_VALUE_BANDS key for a VND amount expression."""
    return f"""(
        CASE
            WHEN {amount_expr} IS NULL OR {amount_expr} < 0 THEN 'BAND_RAW'
            WHEN {amount_expr} < 100000000 THEN 'BAND_REF_20ce64294b791064d896a4b8f7580d8357342f3500939cb360363cc3c777428c'
            WHEN {amount_expr} < 500000000 THEN 'BAND_REF_44b00b40683ee359800012a825b1335e755bc4d323450506643d17a82304a1b5'
            WHEN {amount_expr} < 5000000000 THEN 'BAND_REF_b5c428691904824ea4a65203419948536c9a83111424473c49e3f0f0dcba62b4'
            WHEN {amount_expr} < 20000000000 THEN 'BAND_REF_e0127ec2fb7fa9d0d2ecbf2b6655eab58cc44141ebc54ae3aea268e2b43d9d71'
            WHEN {amount_expr} < 100000000000 THEN 'BAND_REF_57011a5c4d5d1a0c11490458786f58a7ee41d768953b88d67c22738f92065680'
            ELSE 'BAND_REF_c6963e30c9da1883e8461e3e59894164917d7061423bddf0cefaa0838cf9da65'
        END
    )"""


def build_public_facts(
    spark: SparkSession,
    config: GoldBuildConfig,
) -> dict[str, DataFrame]:
    """Aggregate all 8 public daily facts from ``core_fact_*`` TempViews."""
    ev_start = config.event_start.isoformat()
    ev_end = config.event_end.isoformat()

    # 1. fact_demand_daily
    g_dem = ("report_date", "geography_key", "sector_key", "value_band_key", "currency_key")
    fact_demand_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                package_published_date_key AS report_date,
                reporting_geography_key AS geography_key,
                sector_key,
                CASE
                    WHEN is_amount_eligible AND currency_key = 'VND'
                    THEN {_vnd_band_case('planned_package_amount')}
                    ELSE package_value_band_key
                END AS value_band_key,
                currency_key,
                CAST(1 AS BIGINT) AS package_count,
                CAST(CASE WHEN is_amount_eligible THEN 1 ELSE 0 END AS BIGINT) AS eligible_amount_count,
                CAST(CASE WHEN is_amount_eligible THEN COALESCE(planned_package_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS planned_amount_sum
            FROM core_fact_procurement_demand
            WHERE package_published_date_key BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR package_published_date_key IS NULL
        )
        SELECT
            {_group_hash_sql(g_dem)} AS group_key,
            report_date, geography_key, sector_key, value_band_key, currency_key,
            CAST(SUM(package_count) AS BIGINT) AS package_count,
            CAST(SUM(eligible_amount_count) AS BIGINT) AS eligible_amount_count,
            CAST(SUM(planned_amount_sum) AS DECIMAL(38,6)) AS planned_amount_sum
        FROM src
        GROUP BY report_date, geography_key, sector_key, value_band_key, currency_key
        """
    )

    # 2. fact_notice_daily
    g_not = ("report_date", "geography_key", "sector_key", "method_key", "notice_family")
    fact_notice_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                published_date_key AS report_date,
                reporting_geography_key AS geography_key,
                sector_key,
                method_key,
                notice_family,
                CAST(1 AS BIGINT) AS notice_count,
                CAST(CASE WHEN cancellation_state = 'cancelled' THEN 1 ELSE 0 END AS BIGINT) AS cancelled_notice_count
            FROM core_fact_tender_notice
            WHERE published_date_key BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR published_date_key IS NULL
        )
        SELECT
            {_group_hash_sql(g_not)} AS group_key,
            report_date, geography_key, sector_key, method_key, notice_family,
            CAST(SUM(notice_count) AS BIGINT) AS notice_count,
            CAST(SUM(cancelled_notice_count) AS BIGINT) AS cancelled_notice_count
        FROM src
        GROUP BY report_date, geography_key, sector_key, method_key, notice_family
        """
    )

    # 3. fact_award_daily
    g_awd = ("report_date", "geography_key", "sector_key", "method_key", "currency_key")
    fact_award_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                award_published_date_key AS report_date,
                reporting_geography_key AS geography_key,
                sector_key,
                method_key,
                currency_key,
                CAST(1 AS BIGINT) AS award_scope_count,
                CAST(CASE WHEN award_state IN ('effective', 'effective_completed') THEN 1 ELSE 0 END AS BIGINT) AS effective_award_scope_count,
                CAST(CASE WHEN is_amount_eligible AND award_state IN ('effective', 'effective_completed') THEN 1 ELSE 0 END AS BIGINT) AS eligible_amount_count,
                CAST(CASE WHEN is_amount_eligible AND award_state IN ('effective', 'effective_completed') THEN COALESCE(award_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS award_amount_sum
            FROM core_fact_award
            WHERE award_published_date_key BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR award_published_date_key IS NULL
        )
        SELECT
            {_group_hash_sql(g_awd)} AS group_key,
            report_date, geography_key, sector_key, method_key, currency_key,
            CAST(SUM(award_scope_count) AS BIGINT) AS award_scope_count,
            CAST(SUM(effective_award_scope_count) AS BIGINT) AS effective_award_scope_count,
            CAST(SUM(eligible_amount_count) AS BIGINT) AS eligible_amount_count,
            CAST(SUM(award_amount_sum) AS DECIMAL(38,6)) AS award_amount_sum
        FROM src
        GROUP BY report_date, geography_key, sector_key, method_key, currency_key
        """
    )

    # 4. fact_supplier_daily
    g_sup = ("report_date", "geography_key", "sector_key", "currency_key", "party_key", "recipient_allocation_state")
    fact_supplier_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                award_published_date_key AS report_date,
                reporting_geography_key AS geography_key,
                sector_key,
                currency_key,
                recipient_set_key AS party_key,
                recipient_allocation_state,
                CAST(1 AS BIGINT) AS award_scope_count,
                CAST(CASE WHEN award_state IN ('effective', 'effective_completed') THEN 1 ELSE 0 END AS BIGINT) AS effective_award_scope_count,
                CAST(CASE WHEN is_amount_eligible AND award_state IN ('effective', 'effective_completed') THEN 1 ELSE 0 END AS BIGINT) AS eligible_amount_count,
                CAST(CASE WHEN is_amount_eligible AND award_state IN ('effective', 'effective_completed') THEN COALESCE(award_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS award_amount_sum
            FROM core_fact_award
            WHERE award_published_date_key BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR award_published_date_key IS NULL
        )
        SELECT
            {_group_hash_sql(g_sup)} AS group_key,
            report_date, geography_key, sector_key, currency_key, party_key, recipient_allocation_state,
            CAST(SUM(award_scope_count) AS BIGINT) AS award_scope_count,
            CAST(SUM(effective_award_scope_count) AS BIGINT) AS effective_award_scope_count,
            CAST(SUM(eligible_amount_count) AS BIGINT) AS eligible_amount_count,
            CAST(SUM(award_amount_sum) AS DECIMAL(38,6)) AS award_amount_sum
        FROM src
        GROUP BY report_date, geography_key, sector_key, currency_key, party_key, recipient_allocation_state
        """
    )

    # 5. fact_savings_daily
    g_sav = ("report_date", "geography_key", "sector_key", "method_key", "value_band_key", "currency_key")
    fact_savings_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                s.comparison_date_key AS report_date,
                s.reporting_geography_key AS geography_key,
                s.sector_key,
                s.method_key,
                CASE
                    WHEN d.is_amount_eligible AND d.currency_key = 'VND'
                    THEN {_vnd_band_case('d.planned_package_amount')}
                    WHEN s.currency_key = 'VND' AND COALESCE(
                        CASE WHEN s.is_descriptive_eligible THEN NULLIF(s.baseline_amount, 0) END,
                        CASE WHEN s.is_descriptive_eligible THEN NULLIF(s.award_amount, 0) END
                    ) IS NOT NULL
                    THEN {_vnd_band_case('COALESCE(NULLIF(s.baseline_amount, 0), NULLIF(s.award_amount, 0))')}
                    ELSE s.package_value_band_key
                END AS value_band_key,
                s.currency_key,
                CAST(1 AS BIGINT) AS scope_count,
                CAST(CASE WHEN s.is_descriptive_eligible THEN 1 ELSE 0 END AS BIGINT) AS descriptive_sample_count,
                CAST(CASE WHEN s.is_descriptive_eligible THEN COALESCE(s.baseline_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS descriptive_baseline_sum,
                CAST(CASE WHEN s.is_descriptive_eligible THEN COALESCE(s.award_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS descriptive_award_sum,
                CAST(CASE WHEN s.is_descriptive_eligible THEN COALESCE(s.descriptive_difference_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS descriptive_difference_sum,
                CAST(CASE WHEN s.is_verified_comparable THEN 1 ELSE 0 END AS BIGINT) AS verified_sample_count,
                CAST(CASE WHEN s.is_verified_comparable THEN COALESCE(s.baseline_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS verified_baseline_sum,
                CAST(CASE WHEN s.is_verified_comparable THEN COALESCE(s.award_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS verified_award_sum,
                CAST(CASE WHEN s.is_verified_comparable THEN COALESCE(s.verified_saving_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS verified_saving_sum,
                CAST(CASE WHEN s.is_contract_eligible THEN 1 ELSE 0 END AS BIGINT) AS contract_sample_count,
                CAST(CASE WHEN s.is_contract_eligible THEN COALESCE(s.contract_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS contract_amount_sum,
                CAST(CASE WHEN s.is_contract_eligible THEN COALESCE(s.contract_saving_amount, 0) ELSE 0 END AS DECIMAL(38,6)) AS contract_saving_sum
            FROM core_fact_savings s
            LEFT JOIN core_fact_procurement_demand d ON d.package_key = s.package_key
            WHERE s.comparison_date_key BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR s.comparison_date_key IS NULL
        )
        SELECT
            {_group_hash_sql(g_sav)} AS group_key,
            report_date, geography_key, sector_key, method_key, value_band_key, currency_key,
            CAST(SUM(scope_count) AS BIGINT) AS scope_count,
            CAST(SUM(descriptive_sample_count) AS BIGINT) AS descriptive_sample_count,
            CAST(SUM(descriptive_baseline_sum) AS DECIMAL(38,6)) AS descriptive_baseline_sum,
            CAST(SUM(descriptive_award_sum) AS DECIMAL(38,6)) AS descriptive_award_sum,
            CAST(SUM(descriptive_difference_sum) AS DECIMAL(38,6)) AS descriptive_difference_sum,
            CAST(SUM(verified_sample_count) AS BIGINT) AS verified_sample_count,
            CAST(SUM(verified_baseline_sum) AS DECIMAL(38,6)) AS verified_baseline_sum,
            CAST(SUM(verified_award_sum) AS DECIMAL(38,6)) AS verified_award_sum,
            CAST(SUM(verified_saving_sum) AS DECIMAL(38,6)) AS verified_saving_sum,
            CAST(SUM(contract_sample_count) AS BIGINT) AS contract_sample_count,
            CAST(SUM(contract_amount_sum) AS DECIMAL(38,6)) AS contract_amount_sum,
            CAST(SUM(contract_saving_sum) AS DECIMAL(38,6)) AS contract_saving_sum
        FROM src
        GROUP BY report_date, geography_key, sector_key, method_key, value_band_key, currency_key
        """
    )

    # 6. fact_duration_daily (single-pass EXPLODE(ARRAY(...)) instead of 7-way UNION ALL)
    g_dur = ("report_date", "sector_key", "method_key", "metric_key")
    fact_duration_daily = spark.sql(
        f"""
        WITH exploded AS (
            SELECT
                sector_key,
                method_key,
                EXPLODE(ARRAY(
                    NAMED_STRUCT('report_date', first_notice_date_key, 'metric_key', 'plan_to_notice_days',
                        'duration_value', CAST(CASE WHEN is_plan_to_notice_eligible THEN plan_to_notice_days ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_plan_to_notice_eligible),
                    NAMED_STRUCT('report_date', close_date_key, 'metric_key', 'notice_to_close_hours',
                        'duration_value', CAST(CASE WHEN is_notice_to_close_eligible THEN notice_to_close_hours ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_notice_to_close_eligible),
                    NAMED_STRUCT('report_date', first_award_published_date_key, 'metric_key', 'close_to_first_award_days',
                        'duration_value', CAST(CASE WHEN is_close_to_first_award_eligible THEN close_to_first_award_days ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_close_to_first_award_eligible),
                    NAMED_STRUCT('report_date', complete_award_published_date_key, 'metric_key', 'close_to_complete_award_days',
                        'duration_value', CAST(CASE WHEN is_close_to_complete_award_eligible THEN close_to_complete_award_days ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_close_to_complete_award_eligible),
                    NAMED_STRUCT('report_date', actual_open_date_key, 'metric_key', 'close_to_open_hours',
                        'duration_value', CAST(CASE WHEN is_close_to_open_eligible THEN close_to_open_hours ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_close_to_open_eligible),
                    NAMED_STRUCT('report_date', complete_award_published_date_key, 'metric_key', 'open_to_award_days',
                        'duration_value', CAST(CASE WHEN is_open_to_award_eligible THEN open_to_award_days ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_open_to_award_eligible),
                    NAMED_STRUCT('report_date', contract_complete_date_key, 'metric_key', 'planned_to_actual_delay_days',
                        'duration_value', CAST(CASE WHEN is_delay_eligible THEN planned_to_actual_delay_days ELSE 0 END AS DECIMAL(38,6)),
                        'eligible', is_delay_eligible)
                )) AS m
            FROM core_fact_selection_round
        ),
        src AS (
            SELECT
                m.report_date AS report_date,
                COALESCE(sector_key, 'SEC_UNKNOWN') AS sector_key,
                COALESCE(method_key, 'METHOD_UNKNOWN') AS method_key,
                m.metric_key AS metric_key,
                CAST(1 AS BIGINT) AS interval_count,
                CAST(CASE WHEN m.eligible THEN 0 ELSE 1 END AS BIGINT) AS excluded_sample_count,
                CAST(COALESCE(m.duration_value, 0) AS DECIMAL(38,6)) AS duration_sum,
                CAST(CASE WHEN m.eligible THEN 1 ELSE 0 END AS BIGINT) AS sample_count
            FROM exploded
            WHERE m.report_date BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR m.report_date IS NULL
        )
        SELECT
            {_group_hash_sql(g_dur)} AS group_key,
            report_date, sector_key, method_key, metric_key,
            CAST(SUM(interval_count) AS BIGINT) AS interval_count,
            CAST(SUM(excluded_sample_count) AS BIGINT) AS excluded_sample_count,
            CAST(SUM(duration_sum) AS DECIMAL(38,6)) AS duration_sum,
            CAST(SUM(sample_count) AS BIGINT) AS sample_count
        FROM src
        GROUP BY report_date, sector_key, method_key, metric_key
        """
    )

    # 7. fact_status_daily (snapshot of package observation states as of observation_day)
    g_sts = ("report_date", "status_key", "sector_key")
    obs_day = config.observation_day.isoformat()
    fact_status_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                DATE '{obs_day}' AS report_date,
                CONCAT('STATUS_', SHA2(TO_JSON(ARRAY(notice_observation_state, award_observation_state)), 256)) AS status_key,
                sector_key,
                CAST(1 AS BIGINT) AS package_count
            FROM core_fact_procurement_demand
            WHERE package_published_date_key BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR package_published_date_key IS NULL
        )
        SELECT
            {_group_hash_sql(g_sts)} AS group_key,
            report_date, status_key, sector_key,
            CAST(SUM(package_count) AS BIGINT) AS package_count
        FROM src
        GROUP BY report_date, status_key, sector_key
        """
    )

    # 8. fact_bidding_competition_daily
    g_cmp = ("report_date", "geography_key", "sector_key", "method_key", "value_band_key")
    fact_bidding_competition_daily = spark.sql(
        f"""
        WITH src AS (
            SELECT
                c.report_date,
                c.geography_key,
                c.sector_key,
                c.method_key,
                CASE
                    WHEN d.is_amount_eligible AND d.currency_key = 'VND'
                    THEN {_vnd_band_case('d.planned_package_amount')}
                    WHEN COALESCE(c.total_bid_amount, 0) > 0
                    THEN {_vnd_band_case('c.total_bid_amount')}
                    ELSE c.value_band_key
                END AS value_band_key,
                CAST(COALESCE(c.opening_count, 0) AS BIGINT) AS opening_count,
                CAST(COALESCE(c.total_bidders, 0) AS BIGINT) AS total_bidders,
                CAST(COALESCE(c.single_bidder_count, 0) AS BIGINT) AS single_bidder_count,
                CAST(COALESCE(c.multi_bidder_count, 0) AS BIGINT) AS multi_bidder_count,
                CAST(COALESCE(c.tech_passed_count, 0) AS BIGINT) AS tech_passed_count,
                CAST(COALESCE(c.tech_failed_count, 0) AS BIGINT) AS tech_failed_count,
                CAST(COALESCE(c.discounted_bidder_count, 0) AS BIGINT) AS discounted_bidder_count,
                CAST(COALESCE(c.total_bid_amount, 0) AS DECIMAL(38,6)) AS total_bid_amount,
                CAST(COALESCE(c.total_discount_amount, 0) AS DECIMAL(38,6)) AS total_discount_amount,
                CAST(COALESCE(c.total_final_bid_amount, 0) AS DECIMAL(38,6)) AS total_final_bid_amount
            FROM core_fact_bidding_competition c
            LEFT JOIN core_fact_procurement_demand d ON d.package_key = c.package_key
            WHERE c.report_date BETWEEN DATE '{ev_start}' AND DATE '{ev_end}'
               OR c.report_date IS NULL
        )
        SELECT
            {_group_hash_sql(g_cmp)} AS group_key,
            report_date, geography_key, sector_key, method_key, value_band_key,
            CAST(SUM(opening_count) AS BIGINT) AS opening_count,
            CAST(SUM(total_bidders) AS BIGINT) AS total_bidders,
            CAST(SUM(single_bidder_count) AS BIGINT) AS single_bidder_count,
            CAST(SUM(multi_bidder_count) AS BIGINT) AS multi_bidder_count,
            CAST(SUM(tech_passed_count) AS BIGINT) AS tech_passed_count,
            CAST(SUM(tech_failed_count) AS BIGINT) AS tech_failed_count,
            CAST(SUM(discounted_bidder_count) AS BIGINT) AS discounted_bidder_count,
            CAST(SUM(total_bid_amount) AS DECIMAL(38,6)) AS total_bid_amount,
            CAST(SUM(total_discount_amount) AS DECIMAL(38,6)) AS total_discount_amount,
            CAST(SUM(total_final_bid_amount) AS DECIMAL(38,6)) AS total_final_bid_amount
        FROM src
        GROUP BY report_date, geography_key, sector_key, method_key, value_band_key
        """
    )

    return {
        "fact_demand_daily": fact_demand_daily,
        "fact_notice_daily": fact_notice_daily,
        "fact_award_daily": fact_award_daily,
        "fact_supplier_daily": fact_supplier_daily,
        "fact_savings_daily": fact_savings_daily,
        "fact_duration_daily": fact_duration_daily,
        "fact_status_daily": fact_status_daily,
        "fact_bidding_competition_daily": fact_bidding_competition_daily,
    }
