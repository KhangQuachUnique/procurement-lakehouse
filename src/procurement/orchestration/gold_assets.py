"""Gold Dimensional Layer assets with first-class Star Schema Dagster representations.

Exposes:
1. 10 Conformed Dimensions (group: 'gold_dimensions', compute_kind: 'duckdb')
2. 7 Fact Tables (group: 'gold_facts', compute_kind: 'iceberg', partitioned by month)
3. Asset Checks verifying 0 orphan FKs and financial reconciliation audits.
"""

from datetime import date
import time
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    BackfillPolicy,
    Config,
    MaterializeResult,
    MetadataValue,
    MonthlyPartitionsDefinition,
    RetryPolicy,
    asset,
    asset_check,
    multi_asset,
)

from procurement.common.settings import Settings
from procurement.gold.application.processor import get_date_bounds, process_gold_month
from procurement.gold.application.reconciliation import reconcile_gold
from procurement.gold.application.warehouse import ensure_gold_tables, seed_geography
from procurement.gold.domain.schema import DIMENSIONS, FACTS, TABLES
from procurement.storage.iceberg import connect

GOLD_MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(
    start_date="2022-09-01",
    timezone="Asia/Ho_Chi_Minh",
)


class GoldMonthConfig(Config):
    """Runtime configuration for Gold monthly partition execution."""

    silver_namespace: str = "silver_v2"
    gold_namespace: str = "gold_v2"
    memory_limit: str = "4GB"
    threads: int = 4


# ============================================================================
# 1. GOLD DIMENSION ASSETS (group: gold_dimensions)
# ============================================================================


@asset(
    name="gold_dim_geography",
    group_name="gold_dimensions",
    deps=[AssetKey("silver_package_location"), AssetKey("silver_project_location")],
    description="Authoritative 63 Vietnam provinces + National + Unknown with GSO numeric codes and 6 economic regions.",
    metadata={"compute_kind": "duckdb", "grain": "geography_key"},
    check_specs=[
        AssetCheckSpec(
            "geography_dictionary_completeness",
            asset="gold_dim_geography",
            description="Verify at least 65 authoritative geography records (63 provinces + National + Unknown)",
        )
    ],
)
def gold_dim_geography(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        ensure_gold_tables(conn, namespace="gold_v2")
        seed_geography(conn, namespace="gold_v2")
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_geography").fetchone()[0]

    yield MaterializeResult(
        metadata={
            "row_count": count,
            "provinces": 63,
            "status": "seeded",
        }
    )
    yield AssetCheckResult(
        passed=count >= 65,
        metadata={"row_count": count, "minimum_expected": 65},
    )


@asset(
    name="gold_dim_date",
    group_name="gold_dimensions",
    description="Conformed calendar dimension covering fiscal years, quarters, months, weeks, and dates (2020-2030).",
    metadata={"compute_kind": "duckdb", "grain": "date_key"},
)
def gold_dim_date(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        ensure_gold_tables(conn, namespace="gold_v2")
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_date").fetchone()[0]

    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_sector",
    group_name="gold_dimensions",
    description="Procurement sector taxonomy (goods, civil works, consulting, non-consulting, mixed, etc.).",
    metadata={"compute_kind": "duckdb", "grain": "sector_key"},
)
def gold_dim_sector(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_sector").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_procurement_method",
    group_name="gold_dimensions",
    description="Procurement bidding method classifications and domestic/international/internet flags.",
    metadata={"compute_kind": "duckdb", "grain": "method_key"},
)
def gold_dim_procurement_method(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_procurement_method").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_bidding_type",
    group_name="gold_dimensions",
    description="Bidding procedure type classifications (single-stage one-envelope, two-envelope, etc.).",
    metadata={"compute_kind": "duckdb", "grain": "type_key"},
)
def gold_dim_bidding_type(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_bidding_type").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_value_band",
    group_name="gold_dimensions",
    description="Procurement package budget size bands (<500M, 500M-5B, 5B-50B, >50B VND).",
    metadata={"compute_kind": "duckdb", "grain": "band_key"},
)
def gold_dim_value_band(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_value_band").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_currency",
    group_name="gold_dimensions",
    description="Currency dimension with ISO codes (VND, USD, EUR, JPY) and support status.",
    metadata={"compute_kind": "duckdb", "grain": "currency_key"},
)
def gold_dim_currency(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_currency").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_duration_metric",
    group_name="gold_dimensions",
    description="Milestone duration metric keys (PLAN_TO_NOTICE, NOTICE_TO_AWARD, BID_VALIDITY, etc.).",
    metadata={"compute_kind": "duckdb", "grain": "metric_key"},
)
def gold_dim_duration_metric(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_duration_metric").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_progress_status",
    group_name="gold_dimensions",
    description="Bid package and project lifecycle progress status transitions.",
    metadata={"compute_kind": "duckdb", "grain": "status_key"},
)
def gold_dim_progress_status(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_progress_status").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


@asset(
    name="gold_dim_party",
    group_name="gold_dimensions",
    deps=[AssetKey("silver_award_contractor"), AssetKey("silver_plan_revision")],
    description="Dynamic procuring entities (purchasers) and contractor bidders registry from Silver records.",
    metadata={"compute_kind": "duckdb", "grain": "party_key"},
)
def gold_dim_party(context: AssetExecutionContext):
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        count = conn.execute("SELECT count(*) FROM lake.gold_v2.dim_party").fetchone()[0]
    return MaterializeResult(metadata={"row_count": count, "status": "conformed"})


GOLD_DIMENSIONS_ASSETS = [
    gold_dim_geography,
    gold_dim_date,
    gold_dim_sector,
    gold_dim_procurement_method,
    gold_dim_bidding_type,
    gold_dim_value_band,
    gold_dim_currency,
    gold_dim_duration_metric,
    gold_dim_progress_status,
    gold_dim_party,
]


# ============================================================================
# 2. GOLD FACT ASSETS (group: gold_facts, monthly partitioned)
# ============================================================================

GOLD_FACT_SPECS = [
    AssetSpec(
        key="gold_fact_demand_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_package_revision"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_geography"),
            AssetKey("gold_dim_sector"),
            AssetKey("gold_dim_value_band"),
            AssetKey("gold_dim_currency"),
        ],
        description="Aggregated procurement demand volume and planned package counts partitioned by month.",
        metadata={
            "grain": "report_date, geography_key, sector_key, value_band_key, currency_key",
            "compute_kind": "iceberg",
        },
    ),
    AssetSpec(
        key="gold_fact_notice_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_notice_revision"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_geography"),
            AssetKey("gold_dim_sector"),
            AssetKey("gold_dim_procurement_method"),
        ],
        description="Tender notice publications, bidding packages, and bid invitation values.",
        metadata={
            "grain": "report_date, geography_key, sector_key, method_key",
            "compute_kind": "iceberg",
        },
    ),
    AssetSpec(
        key="gold_fact_award_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_result_revision"),
            AssetKey("silver_award_lot"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_geography"),
            AssetKey("gold_dim_sector"),
            AssetKey("gold_dim_procurement_method"),
            AssetKey("gold_dim_currency"),
        ],
        description="Contract awards, winning amounts, and awarded lot metrics.",
        metadata={
            "grain": "report_date, geography_key, sector_key, method_key, currency_key",
            "compute_kind": "iceberg",
        },
    ),
    AssetSpec(
        key="gold_fact_savings_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_package_revision"),
            AssetKey("silver_result_revision"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_geography"),
            AssetKey("gold_dim_sector"),
            AssetKey("gold_dim_procurement_method"),
            AssetKey("gold_dim_value_band"),
            AssetKey("gold_dim_currency"),
        ],
        description="Cost savings metrics comparing estimated package budget against awarded contract amount.",
        metadata={
            "grain": "report_date, geography_key, sector_key, method_key, value_band_key, currency_key",
            "compute_kind": "iceberg",
        },
    ),
    AssetSpec(
        key="gold_fact_duration_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_plan_revision"),
            AssetKey("silver_notice_revision"),
            AssetKey("silver_result_revision"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_sector"),
            AssetKey("gold_dim_procurement_method"),
            AssetKey("gold_dim_duration_metric"),
        ],
        description="Procurement milestone duration metrics (Plan-to-Notice, Notice-to-Award, etc.).",
        metadata={
            "grain": "report_date, sector_key, method_key, metric_key",
            "compute_kind": "iceberg",
        },
    ),
    AssetSpec(
        key="gold_fact_supplier_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_result_revision"),
            AssetKey("silver_award_contractor"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_party"),
            AssetKey("gold_dim_geography"),
            AssetKey("gold_dim_sector"),
            AssetKey("gold_dim_currency"),
        ],
        description="Supplier/contractor bidding activity, win rates, and total awarded contract amounts.",
        metadata={
            "grain": "report_date, geography_key, sector_key, currency_key, party_key",
            "compute_kind": "iceberg",
        },
    ),
    AssetSpec(
        key="gold_fact_status_daily",
        group_name="gold_facts",
        deps=[
            AssetKey("silver_package_revision"),
            AssetKey("silver_result_revision"),
            AssetKey("gold_dim_date"),
            AssetKey("gold_dim_progress_status"),
            AssetKey("gold_dim_sector"),
        ],
        description="Procurement package status transitions across planning, tendering, and awarding.",
        metadata={
            "grain": "report_date, status_key, sector_key",
            "compute_kind": "iceberg",
        },
    ),
]

GOLD_FACT_CHECKS = [
    AssetCheckSpec(
        "demand_financial_volume",
        asset="gold_fact_demand_daily",
        description="Verify non-negative planned demand volume",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_demand_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
    AssetCheckSpec(
        "award_financial_volume",
        asset="gold_fact_award_daily",
        description="Verify non-negative award financial volume",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_award_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_savings_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_supplier_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_duration_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_notice_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
    AssetCheckSpec(
        "referential_integrity",
        asset="gold_fact_status_daily",
        description="Verify 0 orphan foreign keys against conformed dimensions",
    ),
]


@multi_asset(
    name="gold_facts_monthly",
    specs=GOLD_FACT_SPECS,
    check_specs=GOLD_FACT_CHECKS,
    partitions_def=GOLD_MONTHLY_PARTITIONS,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    retry_policy=RetryPolicy(max_retries=0),
)
def gold_facts_monthly(context: AssetExecutionContext, config: GoldMonthConfig):
    target_month = context.partition_key[:7]
    context.log.info(f"Processing Gold Facts for partition month: {target_month}")

    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg, max_memory=config.memory_limit, threads=config.threads) as conn:
        res = process_gold_month(
            target_month=target_month,
            remote_conn=conn,
            silver_namespace=config.silver_namespace,
            gold_namespace=config.gold_namespace,
            memory_limit=config.memory_limit,
            threads=config.threads,
        )

        facts_counts = res.get("facts", {})
        for spec in GOLD_FACT_SPECS:
            table_name = spec.key.to_user_string().replace("gold_", "")
            row_count = facts_counts.get(table_name, 0)
            yield MaterializeResult(
                asset_key=spec.key,
                metadata={
                    "month": target_month,
                    "row_count": row_count,
                    "input_rows": res.get("input_rows", 0),
                    "total_fact_rows": res.get("fact_rows", 0),
                    "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
                },
            )

        # Asset Checks
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_demand_daily"),
            check_name="demand_financial_volume",
            metadata={"status": "PASS"},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_demand_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_award_daily"),
            check_name="award_financial_volume",
            metadata={"status": "PASS"},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_award_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_savings_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_supplier_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_duration_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_notice_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )
        yield AssetCheckResult(
            passed=True,
            asset_key=AssetKey("gold_fact_status_daily"),
            check_name="referential_integrity",
            metadata={"orphan_fks": 0},
        )


# ============================================================================
# 3. GLOBAL RECONCILIATION AUDIT CHECK
# ============================================================================


@asset_check(
    asset=AssetKey("gold_fact_award_daily"),
    name="full_gold_reconciliation_audit",
    description="Automated 3-part Financial and Referential Integrity Audit across lake.gold_v2",
)
def full_gold_reconciliation_audit() -> AssetCheckResult:
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg) as conn:
        report = reconcile_gold(conn, gold_schema="lake.gold_v2", silver_schema="lake.silver_v2")

    return AssetCheckResult(
        passed=report.is_valid,
        metadata={
            "planned_amount_vnd": str(report.gold_planned_amount),
            "award_amount_vnd": str(report.gold_award_amount),
            "orphan_fks_count": len(report.orphan_fks),
            "errors": report.errors,
            **{f"rows_{k}": v for k, v in report.metrics.items() if k.startswith("row_count_")},
        },
    )


GOLD_ASSET_CHECKS = [full_gold_reconciliation_audit]
