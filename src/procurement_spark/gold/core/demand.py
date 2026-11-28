"""Tier 1 Atomic Fact: ``fact_procurement_demand`` (grain = package_key) + ``bridge_location``."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement_spark.gold.core.dimensions_base import GoldBuildConfig
from procurement_spark.gold.core.selection import reference_join_sql


def build_demand(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Compute ``core_fact_procurement_demand`` and ``core_bridge_location``."""
    pkg_loc = f"{view_prefix}package_location"
    rel_id = config.release_id
    map_ver = config.mapping_version
    build_ts = f"{config.observation_day.isoformat()} 00:00:00"
    plan_join = reference_join_sql(
        "p.plan_id", "p.plan_no", "_gtmp_cand_plan", "plan_no", "t.raw_plan_id", "pl"
    )

    fact_demand = spark.sql(
        f"""
        WITH notice_pkgs AS (
            SELECT DISTINCT resolved_entity_id AS pkg_id
            FROM _gtmp_notice_package_match
            WHERE resolved_entity_id IS NOT NULL
        ),
        result_pkgs AS (
            SELECT DISTINCT direct_package_entity_id AS pkg_id
            FROM _gtmp_result_package_match
            WHERE direct_package_entity_id IS NOT NULL
            UNION
            SELECT DISTINCT unique_code_package_entity_id AS pkg_id
            FROM _gtmp_result_package_match
            WHERE unique_code_package_entity_id IS NOT NULL
        )
        SELECT
            '{rel_id}' AS release_id,
            CONCAT('PKG:', p.entity_id) AS package_key,
            CONCAT('SEC_', COALESCE(NULLIF(TRIM(p.bid_field), ''), 'UNKNOWN')) AS sector_key,
            CONCAT(
                'MTH:',
                MD5(
                    CONCAT(
                        COALESCE(p.bid_form, ''), '|',
                        COALESCE(p.bid_mode, ''), '|',
                        COALESCE(CAST(p.is_domestic AS STRING), ''), '|',
                        COALESCE(CAST(p.is_internet AS STRING), ''), '|',
                        COALESCE(p.contract_type, ''), '|',
                        '{map_ver}'
                    )
                )
            ) AS method_key,
            'BAND_RAW' AS package_value_band_key,
            CASE
                WHEN pl.entity_id IS NOT NULL AND NULLIF(TRIM(pl.investor_name), '') IS NOT NULL
                THEN CONCAT('PARTY_INVESTOR_NAME:', MD5(UPPER(TRIM(pl.investor_name))))
                WHEN pl.entity_id IS NOT NULL
                THEN CONCAT('PARTY_UNRESOLVED:{rel_id}:PLAN_INVESTOR:', pl.entity_id)
                ELSE 'PARTY_INVESTOR:UNKNOWN'
            END AS investor_party_key,
            'PARTY_BUYER:UNKNOWN' AS procurer_party_key,
            CASE
                WHEN p.currency IS NULL OR TRIM(p.currency) IN ('', 'UNKNOWN', 'None', 'CUR_UNKNOWN')
                THEN 'CUR_UNKNOWN'
                ELSE UPPER(TRIM(p.currency))
            END AS currency_key,
            p.public_date AS package_published_date_key,
            pl.public_date AS plan_published_date_key,
            CASE
                WHEN p.bid_start_year IS NOT NULL AND p.bid_start_month IS NOT NULL
                THEN MAKE_DATE(CAST(p.bid_start_year AS INT), CAST(p.bid_start_month AS INT), 1)
                WHEN p.bid_start_year IS NOT NULL
                THEN MAKE_DATE(CAST(p.bid_start_year AS INT), 1, 1)
                ELSE CAST(NULL AS DATE)
            END AS planned_start_from,
            CASE
                WHEN p.bid_start_year IS NOT NULL AND p.bid_start_month IS NOT NULL
                THEN LAST_DAY(MAKE_DATE(CAST(p.bid_start_year AS INT), CAST(p.bid_start_month AS INT), 1))
                WHEN p.bid_start_year IS NOT NULL
                THEN MAKE_DATE(CAST(p.bid_start_year AS INT), 12, 31)
                ELSE CAST(NULL AS DATE)
            END AS planned_start_to,
            CASE
                WHEN p.bid_start_month IS NOT NULL THEN 'month'
                WHEN p.bid_start_quarter IS NOT NULL THEN 'quarter'
                WHEN p.bid_start_year IS NOT NULL THEN 'year'
                ELSE 'unresolved'
            END AS planned_start_precision,
            CASE
                WHEN geo.prov_count = 1 THEN CONCAT('GEO_', geo.single_province_code)
                WHEN geo.prov_count > 1 THEN 'GEO_MULTI_UNALLOCATED'
                ELSE 'GEO_UNKNOWN'
            END AS reporting_geography_key,
            CONCAT('LOC_SET:', COALESCE(geo.loc_hash, 'UNKNOWN')) AS location_set_key,
            CAST(p.bid_price AS DECIMAL(38,6)) AS planned_package_amount,
            CAST(
                p.bid_price IS NOT NULL
                AND p.bid_price > 0
                AND p.currency IS NOT NULL
                AND p.currency NOT IN ('', 'UNKNOWN')
                AS BOOLEAN
            ) AS is_amount_eligible,
            CASE WHEN not_sub.pkg_id IS NOT NULL THEN 'observed' ELSE 'not_observed' END AS notice_observation_state,
            CASE WHEN res_sub.pkg_id IS NOT NULL THEN 'observed' ELSE 'not_observed' END AS award_observation_state,
            p.package_resolution_state AS relationship_state,
            p.plan_no AS plan_business_id,
            pl.project_no AS project_business_id,
            CONCAT('EVD:DEMAND:', p.revision_id) AS evidence_set_key,
            CAST('{build_ts}' AS TIMESTAMP) AS created_at
        FROM _gtmp_cand_package p
        {plan_join}
        LEFT JOIN _gtmp_pkg_geography geo ON p.revision_id = geo.revision_id
        LEFT JOIN notice_pkgs not_sub ON p.entity_id = not_sub.pkg_id
        LEFT JOIN result_pkgs res_sub ON p.entity_id = res_sub.pkg_id
        """
    )
    fact_demand.createOrReplaceTempView("core_fact_procurement_demand")

    bridge_location = spark.sql(
        f"""
        SELECT
            '{rel_id}' AS release_id,
            'LOC_SET:UNKNOWN' AS location_set_key,
            'GEO_UNKNOWN' AS geography_key,
            'unknown' AS location_type,
            CAST(NULL AS DECIMAL(10,4)) AS attribution_weight
        UNION ALL
        SELECT DISTINCT
            '{rel_id}' AS release_id,
            CONCAT('LOC_SET:', geo.loc_hash) AS location_set_key,
            CONCAT('GEO_', UPPER(TRIM(ploc.province_code))) AS geography_key,
            'package_location' AS location_type,
            CAST(NULL AS DECIMAL(10,4)) AS attribution_weight
        FROM {pkg_loc} ploc
        JOIN _gtmp_pkg_geography geo ON ploc.revision_id = geo.revision_id
        WHERE ploc.province_code IS NOT NULL AND TRIM(ploc.province_code) != ''
        """
    )
    bridge_location.createOrReplaceTempView("core_bridge_location")

    return {
        "fact_procurement_demand": fact_demand,
        "bridge_location": bridge_location,
    }
