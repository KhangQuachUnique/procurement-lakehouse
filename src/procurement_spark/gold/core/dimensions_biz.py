"""Business entity dimensions built from selection views: dim_package, dim_scope, dim_procurement_method."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from procurement_spark.gold.core.dimensions_base import GoldBuildConfig
from procurement_spark.gold.core.selection import reference_join_sql


def build_business_dimensions(
    spark: SparkSession,
    config: GoldBuildConfig,
    *,
    view_prefix: str = "silver_",
) -> dict[str, DataFrame]:
    """Compute ``core_dim_package``, ``core_dim_scope``, and ``core_dim_procurement_method``."""
    award_lot = f"{view_prefix}award_lot"
    rel_id = config.release_id
    map_ver = config.mapping_version

    # 1. dim_package (resolved packages + context-scoped placeholders for unmatched results/notices)
    plan_join = reference_join_sql(
        "p.plan_id", "p.plan_no", "_gtmp_cand_plan", "plan_no", "t.raw_plan_id", "pl"
    )
    dim_package = spark.sql(
        f"""
        WITH resolved_pkgs AS (
            SELECT
                '{rel_id}' AS release_id,
                CONCAT('PKG:', p.entity_id) AS package_key,
                p.package_no,
                p.title AS package_name,
                p.plan_id,
                p.plan_no,
                pl.project_id,
                pl.project_no,
                p.raw_package_id AS source_id,
                p.revision_id AS source_revision_id,
                CAST(p.numeric_ver AS STRING) AS source_version,
                p.package_resolution_state AS resolution_state
            FROM _gtmp_cand_package p
            {plan_join}
        ),
        unmatched_refs AS (
            SELECT * FROM _gtmp_result_package_match
            UNION ALL
            SELECT * FROM _gtmp_notice_package_match
        ),
        placeholders AS (
            SELECT DISTINCT
                '{rel_id}' AS release_id,
                rpm.package_key,
                rpm.res_package_no AS package_no,
                CONCAT('Gói thầu chưa giải quyết cho kết quả ', rpm.result_entity_id) AS package_name,
                CAST(NULL AS STRING) AS plan_id,
                CAST(NULL AS STRING) AS plan_no,
                CAST(NULL AS STRING) AS project_id,
                CAST(NULL AS STRING) AS project_no,
                rpm.bid_id AS source_id,
                rpm.revision_id AS source_revision_id,
                CAST(NULL AS STRING) AS source_version,
                rpm.package_resolution_state AS resolution_state
            FROM unmatched_refs rpm
            LEFT ANTI JOIN resolved_pkgs rp ON rp.package_key = rpm.package_key
        )
        SELECT * FROM resolved_pkgs
        UNION ALL
        SELECT * FROM placeholders
        """
    )
    dim_package.createOrReplaceTempView("core_dim_package")

    # 2. dim_scope (:PACKAGE, :LOT:..., and :PACKAGE:ROUND:...)
    dim_scope = spark.sql(
        f"""
        WITH unique_notice_round AS (
            SELECT notify_no, MIN(round_business_key) AS round_business_key
            FROM _gtmp_cand_notice
            WHERE notify_no IS NOT NULL
            GROUP BY notify_no
            HAVING COUNT(*) = 1
        ),
        package_scopes AS (
            SELECT
                '{rel_id}' AS release_id,
                CONCAT(package_key, ':PACKAGE') AS scope_key,
                package_key,
                'PACKAGE' AS scope_type,
                CAST(NULL AS STRING) AS lot_no,
                'Toàn gói thầu' AS lot_name,
                CAST(NULL AS STRING) AS source_lot_id,
                'resolved_package_scope' AS resolution_state
            FROM core_dim_package
        ),
        lot_scopes AS (
            SELECT DISTINCT
                '{rel_id}' AS release_id,
                CONCAT(
                    rpm.package_key, ':LOT:', al.lot_no,
                    ':REVISION:', al.award_lot_id,
                    ':ROUND:', COALESCE(n.round_business_key, CONCAT('RESULT:', r.entity_id))
                ) AS scope_key,
                rpm.package_key,
                'LOT' AS scope_type,
                al.lot_no,
                al.lot_name,
                CAST(NULL AS STRING) AS source_lot_id,
                'unresolved_revision_scoped_lot' AS resolution_state
            FROM {award_lot} al
            JOIN _gtmp_cand_result r ON al.revision_id = r.revision_id
            JOIN _gtmp_result_package_match rpm ON r.revision_id = rpm.revision_id
            LEFT JOIN unique_notice_round n ON n.notify_no = r.notify_no
            WHERE al.lot_no IS NOT NULL AND TRIM(al.lot_no) != ''
        ),
        package_round_scopes AS (
            SELECT DISTINCT
                '{rel_id}' AS release_id,
                CONCAT(
                    rpm.package_key, ':PACKAGE:ROUND:',
                    COALESCE(n.round_business_key, CONCAT('RESULT:', r.entity_id))
                ) AS scope_key,
                rpm.package_key,
                'PACKAGE' AS scope_type,
                CAST(NULL AS STRING) AS lot_no,
                'Toàn gói thầu' AS lot_name,
                CAST(NULL AS STRING) AS source_lot_id,
                CASE
                    WHEN n.round_business_key IS NULL THEN 'unresolved_result_round'
                    ELSE 'resolved_package_round'
                END AS resolution_state
            FROM _gtmp_cand_result r
            JOIN _gtmp_result_package_match rpm ON rpm.revision_id = r.revision_id
            LEFT JOIN unique_notice_round n ON n.notify_no = r.notify_no
            LEFT ANTI JOIN {award_lot} al ON al.revision_id = r.revision_id
        )
        SELECT * FROM package_scopes
        UNION ALL
        SELECT * FROM lot_scopes
        UNION ALL
        SELECT * FROM package_round_scopes
        """
    )
    dim_scope.createOrReplaceTempView("core_dim_scope")

    # 3. dim_procurement_method (METHOD_UNKNOWN + MTH:<md5> matching DuckDB exact MD5)
    dim_procurement_method = spark.sql(
        f"""
        WITH all_methods AS (
            SELECT bid_form, bid_mode, is_domestic, is_internet, contract_type FROM _gtmp_cand_package
            UNION
            SELECT bid_form, bid_mode, is_domestic, is_internet, contract_type FROM _gtmp_cand_notice
        ),
        computed AS (
            SELECT DISTINCT
                CONCAT(
                    'MTH:',
                    MD5(
                        CONCAT(
                            COALESCE(m.bid_form, ''), '|',
                            COALESCE(m.bid_mode, ''), '|',
                            COALESCE(CAST(m.is_domestic AS STRING), ''), '|',
                            COALESCE(CAST(m.is_internet AS STRING), ''), '|',
                            COALESCE(m.contract_type, ''), '|',
                            '{map_ver}'
                        )
                    )
                ) AS method_key,
                '{map_ver}' AS mapping_version,
                m.bid_form AS bid_form_raw,
                m.bid_mode AS bid_mode_raw,
                m.is_domestic,
                m.is_internet,
                m.contract_type AS contract_type_raw,
                CASE UPPER(COALESCE(NULLIF(TRIM(m.bid_form), ''), 'UNKNOWN'))
                    WHEN 'DTRR' THEN 'Đấu thầu rộng rãi'
                    WHEN 'DTR' THEN 'Đấu thầu rộng rãi'
                    WHEN 'CHCT' THEN 'Chào hàng cạnh tranh'
                    WHEN 'CHCTRG' THEN 'Chào hàng cạnh tranh rút gọn'
                    WHEN 'CDT' THEN 'Chỉ định thầu'
                    WHEN 'CDTRG' THEN 'Chỉ định thầu rút gọn'
                    WHEN 'CGTTRG' THEN 'Chào giá trực tuyến rút gọn'
                    WHEN 'CGTT' THEN 'Chào giá trực tuyến'
                    WHEN 'MSONL' THEN 'Mua sắm trực tuyến'
                    WHEN 'DTHC' THEN 'Đấu thầu hạn chế'
                    WHEN 'TT' THEN 'Tự thực hiện'
                    WHEN 'TTH' THEN 'Tự thực hiện'
                    WHEN 'LCNT_DB' THEN 'Lựa chọn nhà thầu trong trường hợp đặc biệt'
                    WHEN 'DPTT' THEN 'Đàm phán trực tiếp'
                    WHEN 'MSTT' THEN 'Mua sắm trực tiếp'
                    WHEN 'UNKNOWN' THEN 'Chưa xác định'
                    ELSE COALESCE(NULLIF(TRIM(m.bid_form), ''), 'Chưa xác định')
                END AS method_category
            FROM all_methods m
        )
        SELECT
            'METHOD_UNKNOWN' AS method_key,
            '{map_ver}' AS mapping_version,
            CAST(NULL AS STRING) AS bid_form_raw,
            CAST(NULL AS STRING) AS bid_mode_raw,
            CAST(NULL AS BOOLEAN) AS is_domestic,
            CAST(NULL AS BOOLEAN) AS is_internet,
            CAST(NULL AS STRING) AS contract_type_raw,
            'Chưa xác định' AS method_category
        UNION ALL
        SELECT * FROM computed
        """
    )
    dim_procurement_method.createOrReplaceTempView("core_dim_procurement_method")

    return {
        "dim_package": dim_package,
        "dim_scope": dim_scope,
        "dim_procurement_method": dim_procurement_method,
    }
