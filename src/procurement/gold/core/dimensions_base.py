"""Base static & semi-static Gold dimensions built directly from Silver Spark TempViews.

Produces:
  - ``core_dim_date`` (bounded 2000-01-01..2035-12-31, DuckDB weekday/ISO-week parity)
  - ``core_dim_currency``
  - ``core_dim_geography`` (63 Vietnam provinces + MULTI + UNKNOWN + any unmapped codes)
  - ``core_dim_sector``
  - ``core_dim_value_band``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession

from procurement.gold.core.geography import register_geography_views


@dataclass(frozen=True)
class GoldBuildConfig:
    event_start: date
    event_end: date
    observation_day: date
    release_id: str = "spark_gold_v1"
    geography_version: str = "v1"
    taxonomy_version: str = "v1"
    policy_version: str = "v1"
    mapping_version: str = "v1"


def build_base_dimensions(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Compute the 5 base dimensions and register them as ``core_dim_*`` TempViews."""
    register_geography_views(spark, geography_version=config.geography_version)

    pkg = f"{view_prefix}package_revision"
    pkg_loc = f"{view_prefix}package_location"
    plan = f"{view_prefix}plan_revision"
    not_tbl = f"{view_prefix}notice_revision"
    res_tbl = f"{view_prefix}result_revision"
    award_lot = f"{view_prefix}award_lot"

    ev_start = config.event_start.isoformat()
    ev_end = config.event_end.isoformat()
    obs_day = config.observation_day.isoformat()

    # 1. dim_date (with 2000-01-01..2035-12-31 guard + exact DuckDB weekday/ISO-week parity)
    dim_date = spark.sql(
        f"""
        WITH raw_dates AS (
            SELECT public_date AS d FROM {pkg} WHERE public_date IS NOT NULL
            UNION
            SELECT public_date FROM {not_tbl} WHERE public_date IS NOT NULL
            UNION
            SELECT public_date FROM {res_tbl} WHERE public_date IS NOT NULL
            UNION
            SELECT CAST(bid_close_date AS DATE) FROM {not_tbl} WHERE bid_close_date IS NOT NULL
            UNION
            SELECT decision_date FROM {res_tbl} WHERE decision_date IS NOT NULL
            UNION
            SELECT CAST(cancelled_at AS DATE) FROM {not_tbl} WHERE cancelled_at IS NOT NULL
            UNION
            SELECT public_date FROM {plan} WHERE public_date IS NOT NULL
            UNION
            SELECT DATE '{obs_day}'
        ),
        bounded_dates AS (
            SELECT d FROM raw_dates
            WHERE d >= DATE '2000-01-01' AND d <= DATE '2035-12-31'
        ),
        in_scope_bounds AS (
            SELECT
                DATE '{ev_start}' AS scope_start,
                GREATEST(DATE '{ev_end}', COALESCE(MAX(d), DATE '{ev_end}')) AS scope_end
            FROM bounded_dates
        ),
        dense_days AS (
            SELECT EXPLODE(SEQUENCE(scope_start, scope_end, INTERVAL 1 DAY)) AS d
            FROM in_scope_bounds
        ),
        all_target_days AS (
            SELECT d FROM dense_days
            UNION
            SELECT d FROM bounded_dates WHERE d < DATE '{ev_start}'
        )
        SELECT
            d AS date_key,
            CAST(YEAR(d) AS INT) AS year,
            CAST(QUARTER(d) AS INT) AS quarter,
            CAST(MONTH(d) AS INT) AS month,
            CAST(DAY(d) AS INT) AS day,
            CAST(DAYOFWEEK(d) - 1 AS INT) AS day_of_week,
            CAST(DAYOFWEEK(d) IN (1, 7) AS BOOLEAN) AS is_weekend,
            CONCAT(CAST(YEAR(d) AS STRING), '-Q', CAST(QUARTER(d) AS STRING)) AS year_quarter,
            DATE_FORMAT(d, 'yyyy-MM') AS year_month,
            CAST(EXTRACT(YEAROFWEEK FROM d) AS INT) AS iso_year,
            CAST(WEEKOFYEAR(d) AS INT) AS iso_week,
            CAST(DATE_TRUNC('WEEK', d) AS DATE) AS week_start_date
        FROM all_target_days
        """
    )
    dim_date.createOrReplaceTempView("core_dim_date")

    # 2. dim_currency
    dim_currency = spark.sql(
        f"""
        WITH standard_currencies AS (
            SELECT 'VND' AS currency_key, 'VND' AS currency_code, TRUE AS is_supported, FALSE AS is_unknown
            UNION ALL SELECT 'USD', 'USD', TRUE, FALSE
            UNION ALL SELECT 'EUR', 'EUR', TRUE, FALSE
            UNION ALL SELECT 'CUR_UNKNOWN', 'UNKNOWN', FALSE, TRUE
        ),
        observed_currencies AS (
            SELECT DISTINCT UPPER(TRIM(currency)) AS currency_key
            FROM (
                SELECT currency FROM {pkg}
                UNION ALL SELECT currency FROM {not_tbl}
                UNION ALL SELECT currency FROM {res_tbl}
                UNION ALL SELECT currency FROM {award_lot}
                UNION ALL SELECT reoffer_currency AS currency FROM {not_tbl}
            ) u
            WHERE currency IS NOT NULL
        )
        SELECT currency_key, currency_code, is_supported, is_unknown
        FROM standard_currencies
        UNION ALL
        SELECT o.currency_key, o.currency_key, FALSE, FALSE
        FROM observed_currencies o
        WHERE o.currency_key NOT IN ('', 'NONE', 'UNKNOWN', 'CUR_UNKNOWN', 'VND', 'USD', 'EUR')
        """
    )
    dim_currency.createOrReplaceTempView("core_dim_currency")

    # 3. dim_geography (65 canonical rows + any unmapped province_code observed in package_location)
    geo_ver = config.geography_version
    dim_geography = spark.sql(
        f"""
        SELECT
            cg.geography_key,
            '{geo_ver}' AS geography_version,
            cg.province_code,
            cg.province_name,
            cg.region_name,
            cg.is_multi_geography,
            cg.is_unknown,
            CAST(NULL AS DATE) AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            TRUE AS is_current
        FROM _gtmp_canonical_geography cg
        UNION ALL
        SELECT
            CONCAT('GEO_', UPPER(TRIM(pl.province_code))) AS geography_key,
            '{geo_ver}' AS geography_version,
            UPPER(TRIM(pl.province_code)) AS province_code,
            CASE
                WHEN COUNT(DISTINCT NULLIF(TRIM(pl.province_name), '')) = 1
                THEN MIN(NULLIF(TRIM(pl.province_name), ''))
                ELSE NULL
            END AS province_name,
            CAST(NULL AS STRING) AS region_name,
            FALSE AS is_multi_geography,
            FALSE AS is_unknown,
            CAST(NULL AS DATE) AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            TRUE AS is_current
        FROM {pkg_loc} pl
        LEFT JOIN _gtmp_province_map pm ON UPPER(TRIM(pl.province_code)) = pm.raw_code
        WHERE pl.province_code IS NOT NULL AND TRIM(pl.province_code) != ''
          AND pm.raw_code IS NULL
        GROUP BY UPPER(TRIM(pl.province_code))
        """
    )
    dim_geography.createOrReplaceTempView("core_dim_geography")

    # 4. dim_sector
    tax_ver = config.taxonomy_version
    dim_sector = spark.sql(
        f"""
        WITH observed AS (
            SELECT NULLIF(TRIM(bid_field), '') AS bf FROM {pkg}
            UNION
            SELECT NULLIF(TRIM(bid_field), '') AS bf FROM {not_tbl}
            UNION
            SELECT 'UNKNOWN' AS bf
        ),
        deduped AS (
            SELECT DISTINCT COALESCE(bf, 'UNKNOWN') AS code_raw FROM observed
        )
        SELECT
            CONCAT('SEC_', code_raw) AS sector_key,
            '{tax_ver}' AS taxonomy_version,
            code_raw AS sector_code,
            CASE UPPER(code_raw)
                WHEN 'HH' THEN 'Hàng hóa'
                WHEN 'XL' THEN 'Xây lắp'
                WHEN 'TV' THEN 'Tư vấn'
                WHEN 'PTV' THEN 'Phi tư vấn'
                WHEN 'HON_HOP' THEN 'Hỗn hợp'
                WHEN 'UNKNOWN' THEN 'Chưa xác định'
                ELSE code_raw
            END AS sector_name,
            CASE UPPER(code_raw)
                WHEN 'HH' THEN 'Mua sắm hàng hóa'
                WHEN 'XL' THEN 'Đầu tư xây dựng'
                WHEN 'TV' THEN 'Dịch vụ tư vấn'
                WHEN 'PTV' THEN 'Dịch vụ phi tư vấn'
                WHEN 'HON_HOP' THEN 'Đa lĩnh vực'
                WHEN 'UNKNOWN' THEN CAST(NULL AS STRING)
                ELSE 'Chưa phân nhóm'
            END AS sector_group
        FROM deduped
        """
    )
    dim_sector.createOrReplaceTempView("core_dim_sector")

    # 5. dim_value_band (exact DEFAULT_VALUE_BANDS from policies + BAND_RAW)
    pol_ver = config.policy_version
    dim_value_band = spark.sql(
        f"""
        SELECT 'BAND_REF_20ce64294b791064d896a4b8f7580d8357342f3500939cb360363cc3c777428c' AS band_key,
               'VND' AS currency_key, '{pol_ver}' AS policy_version,
               CAST(0 AS DECIMAL(38,6)) AS min_amount, CAST(100000000 AS DECIMAL(38,6)) AS max_amount,
               'Dưới 100 triệu VND' AS band_label, 1 AS band_order
        UNION ALL SELECT 'BAND_REF_44b00b40683ee359800012a825b1335e755bc4d323450506643d17a82304a1b5',
               'VND', '{pol_ver}', CAST(100000000 AS DECIMAL(38,6)), CAST(500000000 AS DECIMAL(38,6)),
               'Từ 100 triệu đến dưới 500 triệu VND', 2
        UNION ALL SELECT 'BAND_REF_b5c428691904824ea4a65203419948536c9a83111424473c49e3f0f0dcba62b4',
               'VND', '{pol_ver}', CAST(500000000 AS DECIMAL(38,6)), CAST(5000000000 AS DECIMAL(38,6)),
               'Từ 500 triệu đến dưới 5 tỷ VND', 3
        UNION ALL SELECT 'BAND_REF_e0127ec2fb7fa9d0d2ecbf2b6655eab58cc44141ebc54ae3aea268e2b43d9d71',
               'VND', '{pol_ver}', CAST(5000000000 AS DECIMAL(38,6)), CAST(20000000000 AS DECIMAL(38,6)),
               'Từ 5 tỷ đến dưới 20 tỷ VND', 4
        UNION ALL SELECT 'BAND_REF_57011a5c4d5d1a0c11490458786f58a7ee41d768953b88d67c22738f92065680',
               'VND', '{pol_ver}', CAST(20000000000 AS DECIMAL(38,6)), CAST(100000000000 AS DECIMAL(38,6)),
               'Từ 20 tỷ đến dưới 100 tỷ VND', 5
        UNION ALL SELECT 'BAND_REF_c6963e30c9da1883e8461e3e59894164917d7061423bddf0cefaa0838cf9da65',
               'VND', '{pol_ver}', CAST(100000000000 AS DECIMAL(38,6)), CAST(NULL AS DECIMAL(38,6)),
               'Từ 100 tỷ VND trở lên', 6
        UNION ALL SELECT 'BAND_RAW', 'VND', '{pol_ver}',
               CAST(0 AS DECIMAL(38,6)), CAST(NULL AS DECIMAL(38,6)), 'Chưa phân nhóm quy mô', 99
        """
    )
    dim_value_band.createOrReplaceTempView("core_dim_value_band")

    return {
        "dim_date": dim_date,
        "dim_currency": dim_currency,
        "dim_geography": dim_geography,
        "dim_sector": dim_sector,
        "dim_value_band": dim_value_band,
    }
