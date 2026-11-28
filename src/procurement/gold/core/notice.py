"""Tier 1 Atomic Fact: ``fact_tender_notice`` (grain = notice_business_key)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement.gold.core.dimensions_base import GoldBuildConfig


def build_notice(
    spark: SparkSession,
    config: GoldBuildConfig,
) -> dict[str, DataFrame]:
    """Compute ``core_fact_tender_notice`` from selection views."""
    rel_id = config.release_id
    map_ver = config.mapping_version
    build_ts = f"{config.observation_day.isoformat()} 00:00:00"

    fact_notice = spark.sql(
        f"""
        SELECT
            '{rel_id}' AS release_id,
            n.notice_business_key,
            npm.package_key,
            CONCAT(npm.package_key, ':PACKAGE') AS scope_key,
            CONCAT('SEC_', COALESCE(NULLIF(TRIM(n.bid_field), ''), 'UNKNOWN')) AS sector_key,
            CONCAT(
                'MTH:',
                MD5(
                    CONCAT(
                        COALESCE(n.bid_form, ''), '|',
                        COALESCE(n.bid_mode, ''), '|',
                        COALESCE(CAST(n.is_domestic AS STRING), ''), '|',
                        COALESCE(CAST(n.is_internet AS STRING), ''), '|',
                        COALESCE(n.contract_type, ''), '|',
                        '{map_ver}'
                    )
                )
            ) AS method_key,
            'BAND_RAW' AS package_value_band_key,
            CASE
                WHEN NULLIF(TRIM(n.investor_code), '') IS NOT NULL
                THEN CONCAT('PARTY_INVESTOR:', TRIM(n.investor_code))
                WHEN NULLIF(TRIM(n.investor_name), '') IS NOT NULL
                THEN CONCAT('PARTY_INVESTOR_NAME:', MD5(UPPER(TRIM(n.investor_name))))
                ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:NOTICE_INVESTOR:', n.entity_id)
            END AS investor_party_key,
            CASE
                WHEN NULLIF(TRIM(n.procuring_entity_code), '') IS NOT NULL
                THEN CONCAT('PARTY_PROCURER:', TRIM(n.procuring_entity_code))
                ELSE CONCAT('PARTY_UNRESOLVED:{rel_id}:NOTICE_PROCURER:', n.entity_id)
            END AS procurer_party_key,
            n.public_date AS published_date_key,
            n.published_at,
            CAST(n.bid_close_date AS DATE) AS close_date_key,
            n.bid_close_date AS close_at,
            n.original_published_at,
            'notice_revision_timestamp' AS preparation_basis,
            CAST(n.bid_price AS DECIMAL(38,6)) AS notice_budget_amount,
            CASE
                WHEN n.currency IS NULL OR TRIM(n.currency) IN ('', 'UNKNOWN', 'None', 'CUR_UNKNOWN')
                THEN 'CUR_UNKNOWN'
                ELSE UPPER(TRIM(n.currency))
            END AS notice_currency_key,
            CAST(n.initial_price AS DECIMAL(38,6)) AS initial_reoffer_amount,
            n.reoffer_currency AS reoffer_currency_key,
            CAST(n.guarantee_amount AS DECIMAL(38,6)) AS guarantee_amount,
            'CUR_UNKNOWN' AS guarantee_currency_key,
            n.guarantee_form,
            CAST(NULL AS BIGINT) AS contract_duration_value,
            n.contract_duration_unit,
            n.contract_duration_raw,
            CASE
                WHEN n.published_at IS NOT NULL
                     AND n.bid_close_date IS NOT NULL
                     AND n.bid_close_date >= n.published_at
                THEN CAST(
                    ROUND((UNIX_TIMESTAMP(n.bid_close_date) - UNIX_TIMESTAMP(n.published_at)) / 3600.0, 2)
                    AS DECIMAL(38,6)
                )
                ELSE CAST(NULL AS DECIMAL(38,6))
            END AS preparation_hours,
            CAST(
                n.published_at IS NOT NULL
                AND n.bid_close_date IS NOT NULL
                AND n.bid_close_date >= n.published_at
                AS BOOLEAN
            ) AS is_preparation_eligible,
            FALSE AS is_guarantee_eligible,
            n.round_business_key,
            n.notice_family_clean AS notice_family,
            CASE
                WHEN n.cancelled_at IS NOT NULL
                     OR n.cancel_reason IS NOT NULL
                     OR UPPER(COALESCE(n.status_raw, '')) IN ('CANCELED', 'CANCELLED', 'REVOKED', 'HUY_THAU', 'HUY', 'ĐÃ HỦY THẦU')
                THEN 'cancelled'
                ELSE 'none'
            END AS cancellation_state,
            CAST(n.cancelled_at AS DATE) AS cancel_date_key,
            n.cancel_reason AS cancel_reason_raw,
            CASE
                WHEN n.cancel_reason IS NOT NULL THEN 'explicit_notice_cancellation'
                ELSE CAST(NULL AS STRING)
            END AS cancel_reason_category,
            CASE
                WHEN COALESCE(ngeo.prov_count, 0) = 1 THEN CONCAT('GEO_', ngeo.single_province_code)
                WHEN COALESCE(ngeo.prov_count, 0) > 1 THEN 'GEO_MULTI_UNALLOCATED'
                WHEN COALESCE(pgeo.prov_count, 0) = 1 THEN CONCAT('GEO_', pgeo.single_province_code)
                WHEN COALESCE(pgeo.prov_count, 0) > 1 THEN 'GEO_MULTI_UNALLOCATED'
                ELSE 'GEO_UNKNOWN'
            END AS reporting_geography_key,
            CONCAT('LOC_SET:', COALESCE(ngeo.loc_hash, pgeo.loc_hash, 'UNKNOWN')) AS location_set_key,
            CONCAT('EVD:NOTICE:', n.revision_id) AS evidence_set_key,
            CAST('{build_ts}' AS TIMESTAMP) AS created_at
        FROM _gtmp_cand_notice n
        JOIN _gtmp_notice_package_match npm ON npm.revision_id = n.revision_id
        LEFT JOIN _gtmp_notice_geography ngeo ON n.revision_id = ngeo.revision_id
        LEFT JOIN _gtmp_cand_package pkg ON npm.resolved_entity_id = pkg.entity_id
        LEFT JOIN _gtmp_pkg_geography pgeo ON pkg.revision_id = pgeo.revision_id
        """
    )
    fact_notice.createOrReplaceTempView("core_fact_tender_notice")

    return {"fact_tender_notice": fact_notice}
