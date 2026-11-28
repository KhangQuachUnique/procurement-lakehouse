"""Single registry of Spark Silver resource contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from procurement.silver.quality import (
    SHARED_GOVERNANCE_DDLS,
    SHARED_QUARANTINE_TABLE,
    QualityRule,
)
from procurement.silver.resources.bid_opening import (
    BID_OPENING_ICEBERG_DDLS,
    BID_OPENING_QUALITY_RULES,
    BID_OPENING_TRANSFORM_VERSION,
    transform_bid_opening,
)
from procurement.silver.resources.contract_performance import (
    CONTRACT_PERF_ICEBERG_DDLS,
    CONTRACT_PERF_QUALITY_RULES,
    CONTRACT_PERF_TRANSFORM_VERSION,
    transform_contract_performance,
)
from procurement.silver.resources.contractor_result import (
    CONTRACTOR_RESULT_ICEBERG_DDLS,
    CONTRACTOR_RESULT_QUALITY_RULES,
    CONTRACTOR_RESULT_TRANSFORM_VERSION,
    transform_contractor_result,
)
from procurement.silver.resources.khlcnt import (
    KHLCNT_ICEBERG_DDLS,
    KHLCNT_QUALITY_RULES,
    KHLCNT_TRANSFORM_VERSION,
    transform_khlcnt,
)
from procurement.silver.resources.notice import (
    NOTICE_ICEBERG_DDLS,
    NOTICE_QUALITY_RULES,
    NOTICE_TRANSFORM_VERSION,
    transform_notice,
)
from procurement.silver.resources.project import (
    PROJECT_ICEBERG_DDLS,
    PROJECT_QUALITY_RULES,
    PROJECT_TRANSFORM_VERSION,
    transform_project,
)


@dataclass(frozen=True)
class SparkResourceSpec:
    name: str
    bronze_resource: str
    bronze_tables: tuple[str, ...]
    transform_version: str
    primary_tables: tuple[tuple[str, str], ...]  # (table_name, pk_column)
    quarantine_table: str
    iceberg_ddls: dict[str, str]
    quality_rules: tuple[QualityRule, ...]
    transform_fn: Callable[..., Any]


SPARK_RESOURCES: dict[str, SparkResourceSpec] = {
    "project": SparkResourceSpec(
        name="project",
        bronze_resource="project",
        bronze_tables=("project_detail",),
        transform_version=PROJECT_TRANSFORM_VERSION,
        primary_tables=(("project_revision", "revision_id"),),
        quarantine_table=SHARED_QUARANTINE_TABLE,
        iceberg_ddls={**PROJECT_ICEBERG_DDLS, **SHARED_GOVERNANCE_DDLS},
        quality_rules=PROJECT_QUALITY_RULES,
        transform_fn=transform_project,
    ),
    "khlcnt": SparkResourceSpec(
        name="khlcnt",
        bronze_resource="khlcnt",
        bronze_tables=("khlcnt_plan_detail", "khlcnt_bid_package_detail"),
        transform_version=KHLCNT_TRANSFORM_VERSION,
        primary_tables=(
            ("plan_revision", "revision_id"),
            ("package_revision", "revision_id"),
        ),
        quarantine_table=SHARED_QUARANTINE_TABLE,
        iceberg_ddls={**KHLCNT_ICEBERG_DDLS, **SHARED_GOVERNANCE_DDLS},
        quality_rules=KHLCNT_QUALITY_RULES,
        transform_fn=transform_khlcnt,
    ),
    "notice": SparkResourceSpec(
        name="notice",
        bronze_resource="notify_contractor",
        bronze_tables=(
            "notify_contractor_standard_detail",
            "notify_contractor_vk_adb_detail",
            "notify_contractor_reoffer_detail",
        ),
        transform_version=NOTICE_TRANSFORM_VERSION,
        primary_tables=(("notice_revision", "revision_id"),),
        quarantine_table=SHARED_QUARANTINE_TABLE,
        iceberg_ddls={**NOTICE_ICEBERG_DDLS, **SHARED_GOVERNANCE_DDLS},
        quality_rules=NOTICE_QUALITY_RULES,
        transform_fn=transform_notice,
    ),
    "contract_performance": SparkResourceSpec(
        name="contract_performance",
        bronze_resource="contract_performance",
        bronze_tables=("contract_detail", "contract_performance_detail"),
        transform_version=CONTRACT_PERF_TRANSFORM_VERSION,
        primary_tables=(
            ("contract_revision", "revision_id"),
            ("contract_performance_revision", "revision_id"),
        ),
        quarantine_table=SHARED_QUARANTINE_TABLE,
        iceberg_ddls={**CONTRACT_PERF_ICEBERG_DDLS, **SHARED_GOVERNANCE_DDLS},
        quality_rules=CONTRACT_PERF_QUALITY_RULES,
        transform_fn=transform_contract_performance,
    ),
    "contractor_result": SparkResourceSpec(
        name="contractor_result",
        bronze_resource="contractor_result",
        bronze_tables=("contractor_result_detail",),
        transform_version=CONTRACTOR_RESULT_TRANSFORM_VERSION,
        primary_tables=(("result_revision", "revision_id"),),
        quarantine_table=SHARED_QUARANTINE_TABLE,
        iceberg_ddls={**CONTRACTOR_RESULT_ICEBERG_DDLS, **SHARED_GOVERNANCE_DDLS},
        quality_rules=CONTRACTOR_RESULT_QUALITY_RULES,
        transform_fn=transform_contractor_result,
    ),
    "bid_opening": SparkResourceSpec(
        name="bid_opening",
        bronze_resource="bid_opening",
        bronze_tables=("bid_opening_detail",),
        transform_version=BID_OPENING_TRANSFORM_VERSION,
        primary_tables=(("opening_revision", "revision_id"),),
        quarantine_table=SHARED_QUARANTINE_TABLE,
        iceberg_ddls={**BID_OPENING_ICEBERG_DDLS, **SHARED_GOVERNANCE_DDLS},
        quality_rules=BID_OPENING_QUALITY_RULES,
        transform_fn=transform_bid_opening,
    ),
}


def get_spark_resource(name: str) -> SparkResourceSpec:
    if name not in SPARK_RESOURCES:
        raise KeyError(f"Unknown Spark Silver resource: {name!r}. Available: {sorted(SPARK_RESOURCES)}")
    return SPARK_RESOURCES[name]
