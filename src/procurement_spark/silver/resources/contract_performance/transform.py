"""Pure PySpark transformation for resource: contract_performance."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from procurement_spark.silver.common import (
    clean_text,
    dedup_latest,
    parse_boolean,
    parse_decimal_38_6,
    parse_vn_date,
    sha256_key,
)
from procurement_spark.silver.quality import (
    SHARED_DQ_TABLE,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement_spark.silver.resources.contract_performance.schema import (
    CONTRACT_PAYLOAD_SCHEMA,
    PERFORMANCE_PAYLOAD_SCHEMA,
)

CONTRACT_PERF_TRANSFORM_VERSION = "spark_contract_perf_v1"
_JSON_OPTS = {"primitivesAsString": "true"}


@dataclass(frozen=True)
class ContractPerfTransformOutput:
    tables: dict[str, DataFrame]
    per_day_metrics: DataFrame


def _transform_contract(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    parsed = df.withColumn("_p", F.from_json(F.col("payload"), CONTRACT_PAYLOAD_SCHEMA, _JSON_OPTS))
    root = F.col("_p.contract")

    contract_code = clean_text(root.getField("contractCode"))
    raw_price = clean_text(root.getField("priceAfter"))
    parsed_price = parse_decimal_38_6(raw_price)
    currency = clean_text(root.getField("priceAfterUnit"))
    raw_signed = clean_text(root.getField("contractSignDate"))
    parsed_signed = parse_vn_date(raw_signed)
    raw_eff = clean_text(root.getField("contractEffectiveDate"))
    parsed_eff = parse_vn_date(raw_eff)
    raw_pub = clean_text(root.getField("publicDate"))
    parsed_pub = parse_vn_date(raw_pub)

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(root.isNull(), F.lit("missing_root"))
        .when(contract_code.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "contract", "contract_code", contract_code)
    src_ver = clean_text(F.col("source_version"))
    revision_id = sha256_key(entity_id, src_ver, F.col("content_hash"), CONTRACT_PERF_TRANSFORM_VERSION)

    staged = parsed.select(
        F.col("source_date"),
        F.col("source_id").cast("string").alias("_bronze_source_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("content_hash").cast("string").alias("content_hash"),
        F.col("payload").alias("_raw_payload"),
        quarantine_reason.alias("_quarantine_reason"),
        entity_id.alias("entity_id"),
        revision_id.alias("revision_id"),
        clean_text(root.getField("id")).alias("source_contract_id"),
        src_ver.alias("source_version"),
        contract_code.alias("contract_code"),
        clean_text(root.getField("contractNo")).alias("contract_no"),
        clean_text(root.getField("contractVersion")).alias("contract_version"),
        clean_text(root.getField("contractName")).alias("contract_name"),
        clean_text(root.getField("bidName")).alias("package_name"),
        clean_text(root.getField("investorCode")).alias("investor_code"),
        clean_text(root.getField("investorName")).alias("investor_name"),
        clean_text(root.getField("contractorCode")).alias("contractor_code"),
        clean_text(root.getField("contractorName")).alias("contractor_name"),
        parsed_price.alias("price_after_tax"),
        raw_price.alias("_raw_price"),
        currency.alias("currency"),
        clean_text(root.getField("notifyId")).alias("notice_source_id"),
        clean_text(root.getField("notifyNo")).alias("notice_no"),
        clean_text(root.getField("notifyVersion")).alias("notice_version"),
        clean_text(root.getField("resultId")).alias("result_source_id"),
        clean_text(root.getField("resultVersion")).alias("result_version"),
        parsed_signed.alias("signed_date"),
        raw_signed.alias("_raw_signed"),
        parsed_eff.alias("effective_date"),
        raw_eff.alias("_raw_eff"),
        parsed_pub.alias("public_date"),
        raw_pub.alias("published_at_raw"),
    )

    quarantine_df = staged.filter(F.col("_quarantine_reason").isNotNull()).select(
        sha256_key(
            F.lit("contract_performance"),
            F.lit("contract_detail"),
            F.col("source_date"),
            F.col("_bronze_source_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason"),
        ).alias("quarantine_id"),
        F.lit("contract_performance").alias("resource"),
        F.lit("contract_detail").alias("bronze_table"),
        F.col("source_date"),
        F.col("_bronze_source_id").alias("source_id"),
        F.col("run_id"),
        F.col("content_hash"),
        F.col("_quarantine_reason").alias("reason"),
        F.col("_raw_payload").alias("raw_payload"),
    )

    valid_deduped = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNull()),
        partition_cols=["revision_id"],
        order_cols=[F.col("ingested_at").desc_nulls_last(), F.col("source_date").desc()],
    )

    issue_specs = [
        FieldIssueSpec(
            condition=F.col("_raw_price").isNotNull() & F.col("price_after_tax").isNull(),
            field_name="price_after_tax",
            issue_code="invalid_money",
            raw_value=F.col("_raw_price"),
        ),
        FieldIssueSpec(
            condition=F.col("price_after_tax") < F.lit(0),
            field_name="price_after_tax",
            issue_code="negative_money",
            raw_value=F.col("_raw_price"),
        ),
        FieldIssueSpec(
            condition=F.col("price_after_tax").isNotNull() & F.col("currency").isNull(),
            field_name="currency",
            issue_code="unknown_currency",
            raw_value=F.col("_raw_price"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_signed").isNotNull() & F.col("signed_date").isNull(),
            field_name="signed_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_signed"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_eff").isNotNull() & F.col("effective_date").isNull(),
            field_name="effective_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_eff"),
        ),
    ]

    annotated, dq_issues_df = extract_dq_issues(
        valid_deduped,
        resource="contract_performance",
        table_name="contract_revision",
        specs=issue_specs,
    )

    rev_df = annotated.select(
        "revision_id",
        "entity_id",
        "source_contract_id",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "contract_code",
        "contract_no",
        "contract_version",
        "contract_name",
        "package_name",
        "investor_code",
        "investor_name",
        "contractor_code",
        "contractor_name",
        "price_after_tax",
        "currency",
        "notice_source_id",
        "notice_no",
        "notice_version",
        "result_source_id",
        "result_version",
        "signed_date",
        "effective_date",
        "public_date",
        "published_at_raw",
        "dq_status",
    )

    raw_counts = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    return rev_df, dq_issues_df, quarantine_df, raw_counts


def _transform_performance(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    parsed = df.withColumn("_p", F.from_json(F.col("payload"), PERFORMANCE_PAYLOAD_SCHEMA, _JSON_OPTS))
    root = F.col("_p.performance")

    eval_id = clean_text(root.getField("id"))
    res_status = clean_text(F.col("_p.resolution.status"))
    prog_status = clean_text(root.getField("contractStatus"))
    raw_pub = clean_text(root.getField("publicDate"))
    parsed_pub = parse_vn_date(raw_pub)

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(root.isNull(), F.lit("missing_root"))
        .when(eval_id.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "contract_performance", "uuid", eval_id)
    src_ver = clean_text(F.col("source_version"))
    revision_id = sha256_key(entity_id, src_ver, F.col("content_hash"), CONTRACT_PERF_TRANSFORM_VERSION)

    staged = parsed.select(
        F.col("source_date"),
        F.col("source_id").cast("string").alias("_bronze_source_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("content_hash").cast("string").alias("content_hash"),
        F.col("payload").alias("_raw_payload"),
        quarantine_reason.alias("_quarantine_reason"),
        entity_id.alias("entity_id"),
        revision_id.alias("revision_id"),
        eval_id.alias("source_evaluation_id"),
        src_ver.alias("source_version"),
        clean_text(root.getField("idNo")).alias("evaluation_no"),
        clean_text(root.getField("version")).alias("evaluation_version"),
        clean_text(root.getField("contractCode")).alias("contract_code"),
        clean_text(root.getField("contractVersion")).alias("contract_version"),
        clean_text(root.getField("investorCode")).alias("investor_code"),
        clean_text(root.getField("investorName")).alias("investor_name"),
        clean_text(root.getField("contractorCode")).alias("contractor_code"),
        clean_text(root.getField("contractorName")).alias("contractor_name"),
        prog_status.alias("progress_status_code"),
        clean_text(root.getField("contractQualityPer")).alias("quality_status_code"),
        parse_boolean(root.getField("isContractComplete")).alias("is_completed"),
        parsed_pub.alias("public_date"),
        parse_vn_date(root.getField("completePlanDate")).alias("planned_completion_date"),
        parse_vn_date(root.getField("completeRealityDate")).alias("actual_completion_date"),
        parse_vn_date(root.getField("completeAdjustedDate")).alias("adjusted_completion_date"),
        raw_pub.alias("published_at_raw"),
        res_status.alias("_resolution_status"),
    )

    quarantine_df = staged.filter(F.col("_quarantine_reason").isNotNull()).select(
        sha256_key(
            F.lit("contract_performance"),
            F.lit("contract_performance_detail"),
            F.col("source_date"),
            F.col("_bronze_source_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason"),
        ).alias("quarantine_id"),
        F.lit("contract_performance").alias("resource"),
        F.lit("contract_performance_detail").alias("bronze_table"),
        F.col("source_date"),
        F.col("_bronze_source_id").alias("source_id"),
        F.col("run_id"),
        F.col("content_hash"),
        F.col("_quarantine_reason").alias("reason"),
        F.col("_raw_payload").alias("raw_payload"),
    )

    valid_deduped = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNull()),
        partition_cols=["revision_id"],
        order_cols=[F.col("ingested_at").desc_nulls_last(), F.col("source_date").desc()],
    )

    issue_specs = [
        FieldIssueSpec(
            condition=(
                F.col("progress_status_code").isNotNull()
                & ~F.col("progress_status_code").isin("01", "02", "03")
            ),
            field_name="progress_status_code",
            issue_code="unknown_progress_status",
            raw_value=F.col("progress_status_code"),
        ),
        FieldIssueSpec(
            condition=F.col("_resolution_status").isNull() | (F.col("_resolution_status") != "resolved"),
            field_name="contract_version",
            issue_code="missing_resolution",
            raw_value=F.coalesce(F.col("_resolution_status"), F.lit("null")),
        ),
    ]

    annotated, dq_issues_df = extract_dq_issues(
        valid_deduped,
        resource="contract_performance",
        table_name="contract_performance_revision",
        specs=issue_specs,
    )

    rev_df = annotated.select(
        "revision_id",
        "entity_id",
        "source_evaluation_id",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "evaluation_no",
        "evaluation_version",
        "contract_code",
        "contract_version",
        "investor_code",
        "investor_name",
        "contractor_code",
        "contractor_name",
        "progress_status_code",
        "quality_status_code",
        "is_completed",
        "public_date",
        "planned_completion_date",
        "actual_completion_date",
        "adjusted_completion_date",
        "published_at_raw",
        "dq_status",
    )

    raw_counts = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    return rev_df, dq_issues_df, quarantine_df, raw_counts


def transform_contract_performance(
    bronze_by_table: dict[str, DataFrame],
) -> ContractPerfTransformOutput:
    c_rev, c_issues, c_quar, c_raw = _transform_contract(bronze_by_table["contract_detail"])
    p_rev, p_issues, p_quar, p_raw = _transform_performance(
        bronze_by_table["contract_performance_detail"]
    )

    dq_issues_df = c_issues.unionByName(p_issues)
    quarantine_df = dedup_latest(
        c_quar.unionByName(p_quar),
        partition_cols=["quarantine_id"],
        order_cols=[F.col("source_date").desc()],
    )

    raw_day = (
        c_raw.unionByName(p_raw)
        .groupBy("source_date")
        .agg(
            F.sum("raw_input_rows").alias("raw_input_rows"),
            F.sum("valid_rows").alias("valid_rows"),
            F.sum("quarantined_rows").alias("quarantined_rows"),
        )
    )
    warn_day = (
        c_rev.select("source_date", "dq_status")
        .unionByName(p_rev.select("source_date", "dq_status"))
        .groupBy("source_date")
        .agg(F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows"))
    )
    issue_day = dq_issues_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("issue_code") == "invalid_money", 1).otherwise(0)).alias("invalid_money_rows"),
    )
    per_day_metrics = (
        raw_day.join(warn_day, "source_date", "left")
        .join(issue_day, "source_date", "left")
        .na.fill(0)
    )

    return ContractPerfTransformOutput(
        tables={
            "contract_revision": c_rev,
            "contract_performance_revision": p_rev,
            SHARED_DQ_TABLE: dq_issues_df,
            SHARED_QUARANTINE_TABLE: quarantine_df,
        },
        per_day_metrics=per_day_metrics,
    )
