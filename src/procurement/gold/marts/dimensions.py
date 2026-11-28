"""Tier 2 Public Dimensions (9 dim_* tables) matching ``mart_schema.json`` 1-to-1."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def build_public_dimensions(spark: SparkSession) -> dict[str, DataFrame]:
    """Build all 9 public Gold dimension DataFrames from Gold Core TempViews."""
    dim_date = spark.sql(
        """
        SELECT
            date_key, year, quarter, month, day, day_of_week, is_weekend,
            year_quarter, year_month, iso_year, iso_week, week_start_date
        FROM core_dim_date
        """
    )

    dim_sector = spark.sql(
        """
        SELECT sector_key, taxonomy_version, sector_code, sector_name, sector_group
        FROM core_dim_sector
        """
    )

    dim_geography = spark.sql(
        """
        SELECT
            geography_key, geography_version, province_code, province_name,
            region_name, is_multi_geography, is_unknown,
            valid_from, valid_to, is_current
        FROM core_dim_geography
        """
    )

    dim_value_band = spark.sql(
        """
        SELECT band_key, currency_key, policy_version, min_amount, max_amount, band_label, band_order
        FROM core_dim_value_band
        """
    )

    dim_currency = spark.sql(
        """
        SELECT currency_key, currency_code, is_supported, is_unknown
        FROM core_dim_currency
        """
    )

    dim_procurement_method = spark.sql(
        """
        SELECT
            method_key, mapping_version, bid_form_raw, bid_mode_raw,
            is_domestic, is_internet, contract_type_raw, method_category
        FROM core_dim_procurement_method
        """
    )

    dim_duration_metric = spark.sql(
        """
        SELECT 'plan_to_notice_days' AS metric_key, 'plan_to_notice_days' AS metric_name, 'days' AS duration_unit
        UNION ALL SELECT 'notice_to_close_hours', 'notice_to_close_hours', 'hours'
        UNION ALL SELECT 'close_to_first_award_days', 'close_to_first_award_days', 'days'
        UNION ALL SELECT 'close_to_complete_award_days', 'close_to_complete_award_days', 'days'
        UNION ALL SELECT 'close_to_open_hours', 'close_to_open_hours', 'hours'
        UNION ALL SELECT 'open_to_award_days', 'open_to_award_days', 'days'
        UNION ALL SELECT 'planned_to_actual_delay_days', 'planned_to_actual_delay_days', 'days'
        """
    )

    dim_progress_status = spark.sql(
        """
        SELECT DISTINCT
            CONCAT('STATUS_', SHA2(TO_JSON(ARRAY(notice_observation_state, award_observation_state)), 256)) AS status_key,
            notice_observation_state,
            award_observation_state
        FROM core_fact_procurement_demand
        """
    )

    # Ensure every recipient_set_key in core_fact_award exists in dim_party
    dim_party = spark.sql(
        """
        WITH base_parties AS (
            SELECT party_key, party_kind, party_code, party_name, tax_code, source_namespace
            FROM core_dim_party
        ),
        missing_award_recipients AS (
            SELECT DISTINCT
                a.recipient_set_key AS party_key,
                'UNKNOWN_GROUP' AS party_kind,
                CAST(NULL AS STRING) AS party_code,
                CASE
                    WHEN a.recipient_set_key = 'REC_MULTIPLE_INDEPENDENT' THEN 'Nhiều bên nhận — chưa phân bổ'
                    WHEN a.recipient_set_key = 'REC_NO_WINNER' THEN 'Chưa có bên nhận'
                    ELSE 'Bên nhận chưa định danh'
                END AS party_name,
                CAST(NULL AS STRING) AS tax_code,
                'reporting_bucket' AS source_namespace
            FROM core_fact_award a
            LEFT ANTI JOIN base_parties p ON p.party_key = a.recipient_set_key
            WHERE a.recipient_set_key IS NOT NULL
        )
        SELECT * FROM base_parties
        UNION ALL
        SELECT * FROM missing_award_recipients
        """
    )

    return {
        "dim_date": dim_date,
        "dim_sector": dim_sector,
        "dim_geography": dim_geography,
        "dim_value_band": dim_value_band,
        "dim_currency": dim_currency,
        "dim_procurement_method": dim_procurement_method,
        "dim_duration_metric": dim_duration_metric,
        "dim_progress_status": dim_progress_status,
        "dim_party": dim_party,
    }
