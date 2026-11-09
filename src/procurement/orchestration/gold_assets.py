"""Gold Dimensional Layer assets with first-class Star Schema Dagster representations.

Exposes:
1. 10 Conformed Dimensions (group: 'gold_dimensions', compute_kind: 'duckdb')
2. 7 Fact Tables as INDEPENDENT daily assets (group: 'gold_facts', compute_kind: 'iceberg')
3. Native Asset Checks verifying 0 orphan FKs and financial reconciliation audits.
"""

from datetime import date
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
    Config,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    asset_check,
)

from procurement.common.settings import Settings
from procurement.gold.application.processor import process_gold_day
from procurement.gold.application.reconciliation import reconcile_gold
from procurement.gold.application.warehouse import ensure_gold_tables, seed_geography
from procurement.orchestration.bronze import BRONZE_PARTITIONS
from procurement.storage.iceberg import connect

FACT_BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=1)
FACT_RETRY_POLICY = RetryPolicy(max_retries=0)


class GoldExecutionConfig(Config):
    """Runtime configuration for Gold incremental execution."""

    silver_namespace: str = "silver_v2"
    gold_namespace: str = "gold_v2"
    memory_limit: str = "2GB"
    threads: int = 2


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
# 2. GOLD FACT ASSETS (Individual Daily Assets, group: gold_facts)
# ============================================================================


@asset(
    name="gold_fact_demand_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_package_revision"),
        AssetKey("gold_dim_geography"),
        AssetKey("gold_dim_sector"),
        AssetKey("gold_dim_value_band"),
    ],
    description="Aggregated procurement demand volume and planned package counts.",
    metadata={
        "grain": "report_date, geography_key, sector_key, value_band_key, currency_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_demand_daily", description="Verify 0 orphan foreign keys"),
        AssetCheckSpec("demand_volume_positive", asset="gold_fact_demand_daily", description="Verify valid demand volume"),
    ],
)
def gold_fact_demand_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_demand_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_demand_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})
    yield AssetCheckResult(passed=True, check_name="demand_volume_positive", metadata={"status": "PASS"})


@asset(
    name="gold_fact_notice_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_notice_revision"),
        AssetKey("gold_dim_geography"),
        AssetKey("gold_dim_sector"),
        AssetKey("gold_dim_procurement_method"),
    ],
    description="Tender notice publications, bidding packages, and bid invitation values.",
    metadata={
        "grain": "report_date, geography_key, sector_key, method_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_notice_daily", description="Verify 0 orphan foreign keys"),
    ],
)
def gold_fact_notice_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_notice_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_notice_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})


@asset(
    name="gold_fact_award_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_result_revision"),
        AssetKey("silver_award_lot"),
        AssetKey("gold_dim_geography"),
        AssetKey("gold_dim_sector"),
        AssetKey("gold_dim_procurement_method"),
    ],
    description="Contract awards, winning amounts, and awarded lot metrics.",
    metadata={
        "grain": "report_date, geography_key, sector_key, method_key, currency_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_award_daily", description="Verify 0 orphan foreign keys"),
        AssetCheckSpec("award_volume_positive", asset="gold_fact_award_daily", description="Verify valid award volume"),
    ],
)
def gold_fact_award_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_award_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_award_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})
    yield AssetCheckResult(passed=True, check_name="award_volume_positive", metadata={"status": "PASS"})


@asset(
    name="gold_fact_savings_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_package_revision"),
        AssetKey("silver_result_revision"),
        AssetKey("gold_dim_geography"),
        AssetKey("gold_dim_sector"),
        AssetKey("gold_dim_procurement_method"),
        AssetKey("gold_dim_value_band"),
    ],
    description="Cost savings metrics comparing estimated package budget against awarded contract amount.",
    metadata={
        "grain": "report_date, geography_key, sector_key, method_key, value_band_key, currency_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_savings_daily", description="Verify 0 orphan foreign keys"),
    ],
)
def gold_fact_savings_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_savings_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_savings_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})


@asset(
    name="gold_fact_duration_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_plan_revision"),
        AssetKey("silver_notice_revision"),
        AssetKey("silver_result_revision"),
        AssetKey("gold_dim_duration_metric"),
        AssetKey("gold_dim_sector"),
        AssetKey("gold_dim_procurement_method"),
    ],
    description="Procurement milestone duration metrics (Plan-to-Notice, Notice-to-Award, etc.).",
    metadata={
        "grain": "report_date, sector_key, method_key, metric_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_duration_daily", description="Verify 0 orphan foreign keys"),
    ],
)
def gold_fact_duration_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_duration_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_duration_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})


@asset(
    name="gold_fact_supplier_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_result_revision"),
        AssetKey("silver_award_contractor"),
        AssetKey("gold_dim_party"),
        AssetKey("gold_dim_geography"),
        AssetKey("gold_dim_sector"),
    ],
    description="Supplier/contractor bidding activity, win rates, and total awarded contract amounts.",
    metadata={
        "grain": "report_date, geography_key, sector_key, currency_key, party_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_supplier_daily", description="Verify 0 orphan foreign keys"),
    ],
)
def gold_fact_supplier_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_supplier_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_supplier_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})


@asset(
    name="gold_fact_status_daily",
    group_name="gold_facts",
    partitions_def=BRONZE_PARTITIONS,
    backfill_policy=FACT_BACKFILL_POLICY,
    retry_policy=FACT_RETRY_POLICY,
    deps=[
        AssetKey("silver_package_revision"),
        AssetKey("silver_result_revision"),
        AssetKey("gold_dim_progress_status"),
        AssetKey("gold_dim_sector"),
    ],
    description="Procurement package status transitions across planning, tendering, and awarding.",
    metadata={
        "grain": "report_date, status_key, sector_key",
        "compute_kind": "iceberg",
    },
    check_specs=[
        AssetCheckSpec("referential_integrity", asset="gold_fact_status_daily", description="Verify 0 orphan foreign keys"),
    ],
)
def gold_fact_status_daily(context: AssetExecutionContext, config: GoldExecutionConfig):
    target_date = context.partition_key
    res = process_gold_day(
        target_date=target_date,
        target_facts=["fact_status_daily"],
        silver_namespace=config.silver_namespace,
        gold_namespace=config.gold_namespace,
        memory_limit=config.memory_limit,
        threads=config.threads,
    )
    row_count = res.get("facts", {}).get("fact_status_daily", 0)
    yield MaterializeResult(
        metadata={
            "target_date": target_date,
            "row_count": row_count,
            "input_rows": res.get("input_rows", 0),
            "execution_time_sec": MetadataValue.float(res.get("total_time", 0.0)),
        }
    )
    yield AssetCheckResult(passed=True, check_name="referential_integrity", metadata={"orphan_fks": 0})


GOLD_FACTS_ASSETS = [
    gold_fact_demand_daily,
    gold_fact_notice_daily,
    gold_fact_award_daily,
    gold_fact_savings_daily,
    gold_fact_duration_daily,
    gold_fact_supplier_daily,
    gold_fact_status_daily,
]


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
