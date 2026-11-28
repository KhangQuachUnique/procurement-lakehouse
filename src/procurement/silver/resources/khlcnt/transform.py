"""Pure PySpark transformation for resource: khlcnt (Plan + Package + Location + Lot).

Read once per Bronze table with primitivesAsString=true, emit:
  - plan_revision (clean business table with lightweight dq_status)
  - package_revision (clean business table with lightweight dq_status)
  - package_location (exploded from bidLocation)
  - package_lot (exploded from bidpBidLotList)
  - silver_dq_issues (shared field-level DQ issues table)
  - silver_quarantine (shared structural quarantine table)
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from procurement.silver.common import (
    clean_pipe_code,
    clean_text,
    dedup_latest,
    parse_boolean,
    parse_decimal_38_6,
    parse_nonnegative_int,
    parse_vn_date,
    sha256_key,
)
from procurement.silver.quality import (
    SHARED_DQ_TABLE,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement.silver.resources.khlcnt.schema import (
    PACKAGE_PAYLOAD_SCHEMA,
    PLAN_PAYLOAD_SCHEMA,
)

KHLCNT_TRANSFORM_VERSION = "spark_khlcnt_v2"
_JSON_OPTS = {"primitivesAsString": "true"}


@dataclass(frozen=True)
class KhlcntTransformOutput:
    tables: dict[str, DataFrame]
    per_day_metrics: DataFrame


def _transform_plan(
    plan_bronze_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Transform khlcnt_plan_detail -> (plan_revision, dq_issues, quarantine, raw_counts)."""
    parsed = plan_bronze_df.withColumn("_p", F.from_json(F.col("payload"), PLAN_PAYLOAD_SCHEMA, _JSON_OPTS))
    root = F.col("_p.bidPoBidpPlanProjectDetailView")

    raw_id = clean_text(root.getField("id"))
    raw_invest = clean_text(root.getField("investTotal"))
    parsed_invest = parse_decimal_38_6(raw_invest)
    currency = clean_text(root.getField("investTotalUnit"))
    raw_pub_date = clean_text(root.getField("publicDate"))
    parsed_pub_date = parse_vn_date(raw_pub_date)
    raw_dec_date = clean_text(root.getField("decisionDate"))
    parsed_dec_date = parse_vn_date(raw_dec_date)
    raw_bid_pack = clean_text(root.getField("bidPack"))
    parsed_bid_pack = parse_nonnegative_int(raw_bid_pack)

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(root.isNull(), F.lit("missing_root"))
        .when(raw_id.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "plan", "uuid", raw_id)
    src_ver = clean_text(F.col("source_version"))
    revision_id = sha256_key(entity_id, src_ver, F.col("content_hash"), KHLCNT_TRANSFORM_VERSION)

    staged = parsed.select(
        F.col("source_date"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("content_hash").cast("string").alias("content_hash"),
        F.col("payload").alias("_raw_payload"),
        quarantine_reason.alias("_quarantine_reason"),
        entity_id.alias("entity_id"),
        revision_id.alias("revision_id"),
        raw_id.alias("source_id"),
        src_ver.alias("source_version"),
        clean_text(root.getField("planNo")).alias("plan_no"),
        clean_text(root.getField("name")).alias("title"),
        clean_text(root.getField("investorName")).alias("investor_name"),
        clean_text(root.getField("pid")).alias("project_id"),
        clean_text(root.getField("pno")).alias("project_no"),
        clean_text(root.getField("planType")).alias("plan_type"),
        parsed_invest.alias("total_investment"),
        raw_invest.alias("_raw_invest"),
        currency.alias("currency"),
        parsed_dec_date.alias("decision_date"),
        raw_dec_date.alias("_raw_dec_date"),
        clean_text(root.getField("decisionNo")).alias("decision_no"),
        clean_text(root.getField("decisionAgency")).alias("decision_agency"),
        parsed_bid_pack.alias("bid_pack"),
        raw_bid_pack.alias("_raw_bid_pack"),
        clean_text(root.getField("location")).alias("location"),
        parsed_pub_date.alias("public_date"),
        raw_pub_date.alias("_raw_pub_date"),
        clean_text(root.getField("status")).alias("status_raw"),
    )

    quarantine_df = staged.filter(F.col("_quarantine_reason").isNotNull()).select(
        sha256_key(
            F.lit("khlcnt"),
            F.lit("khlcnt_plan_detail"),
            F.col("source_date"),
            F.col("source_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason"),
        ).alias("quarantine_id"),
        F.lit("khlcnt").alias("resource"),
        F.lit("khlcnt_plan_detail").alias("bronze_table"),
        F.col("source_date"),
        F.col("source_id"),
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

    plan_issue_specs = [
        FieldIssueSpec(
            condition=F.col("_raw_invest").isNotNull() & F.col("total_investment").isNull(),
            field_name="total_investment",
            issue_code="invalid_money",
            raw_value=F.col("_raw_invest"),
        ),
        FieldIssueSpec(
            condition=F.col("total_investment") < F.lit(0),
            field_name="total_investment",
            issue_code="negative_money",
            raw_value=F.col("_raw_invest"),
        ),
        FieldIssueSpec(
            condition=F.col("total_investment").isNotNull() & F.col("currency").isNull(),
            field_name="currency",
            issue_code="unknown_currency",
            raw_value=F.col("_raw_invest"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_pub_date").isNotNull() & F.col("public_date").isNull(),
            field_name="public_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_pub_date"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_dec_date").isNotNull() & F.col("decision_date").isNull(),
            field_name="decision_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_dec_date"),
        ),
        FieldIssueSpec(
            condition=(
                F.col("decision_date").isNotNull()
                & (
                    (F.col("decision_date") < F.lit("2000-01-01").cast("date"))
                    | (F.col("decision_date") > F.lit("2035-12-31").cast("date"))
                )
            ),
            field_name="decision_date",
            issue_code="suspect_decision_date",
            raw_value=F.col("_raw_dec_date"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_bid_pack").isNotNull() & F.col("bid_pack").isNull(),
            field_name="bid_pack",
            issue_code="invalid_count",
            raw_value=F.col("_raw_bid_pack"),
        ),
        FieldIssueSpec(
            condition=F.col("plan_no").isNull(),
            field_name="plan_no",
            issue_code="missing_business_number",
            raw_value=F.col("source_id"),
        ),
    ]

    annotated_plan, plan_issues_df = extract_dq_issues(
        valid_deduped,
        resource="khlcnt",
        table_name="plan_revision",
        specs=plan_issue_specs,
    )

    plan_rev_df = annotated_plan.select(
        "revision_id",
        "entity_id",
        "source_id",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "plan_no",
        "title",
        "investor_name",
        "project_id",
        "project_no",
        "plan_type",
        "total_investment",
        "currency",
        "decision_date",
        "decision_no",
        "decision_agency",
        "bid_pack",
        "location",
        "public_date",
        "status_raw",
        "dq_status",
    )

    raw_counts = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    return plan_rev_df, plan_issues_df, quarantine_df, raw_counts


def _transform_package(
    pkg_bronze_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """Transform khlcnt_bid_package_detail -> (pkg_rev, pkg_loc, pkg_lot, dq_issues, quarantine, raw_counts)."""
    parsed = pkg_bronze_df.withColumn("_p", F.from_json(F.col("payload"), PACKAGE_PAYLOAD_SCHEMA, _JSON_OPTS))

    raw_id = clean_text(F.col("_p.id"))
    raw_price = clean_text(F.col("_p.bidPrice"))
    parsed_price = parse_decimal_38_6(raw_price)
    currency = clean_text(F.col("_p.bidPriceUnit"))

    parsed_pub_date = F.coalesce(
        parse_vn_date(F.col("_p.createdDate")),
        parse_vn_date(F.col("_p.originalCreateDate")),
        parse_vn_date(F.col("_p.exchangeDate")),
    )

    raw_cperiod = clean_text(F.col("_p.cperiod"))
    parsed_cperiod = parse_nonnegative_int(raw_cperiod)
    raw_year = parse_nonnegative_int(F.col("_p.bidStartYear"))
    parsed_year = F.when(raw_year == 0, F.lit(None).cast("int")).otherwise(raw_year)
    raw_quarter = clean_text(F.col("_p.bidStartQuarter"))
    parsed_quarter = F.when(raw_quarter == "0", F.lit(None).cast("string")).otherwise(raw_quarter)
    raw_month_str = clean_text(F.col("_p.bidStartMonth"))
    raw_month = parse_nonnegative_int(raw_month_str)
    parsed_month = F.when(
        raw_month.isNotNull() & (raw_month >= 1) & (raw_month <= 12), raw_month
    ).otherwise(F.lit(None).cast("int"))

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(raw_id.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "package", "uuid", raw_id)
    revision_id = sha256_key(entity_id, F.lit(None).cast("string"), F.col("content_hash"), KHLCNT_TRANSFORM_VERSION)

    staged = parsed.select(
        F.col("source_date"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("content_hash").cast("string").alias("content_hash"),
        F.col("payload").alias("_raw_payload"),
        quarantine_reason.alias("_quarantine_reason"),
        entity_id.alias("entity_id"),
        revision_id.alias("revision_id"),
        raw_id.alias("source_id"),
        clean_text(F.col("_p.bidNo")).alias("package_no"),
        clean_text(F.col("_p.bidName")).alias("title"),
        clean_text(F.col("_p.planNo")).alias("plan_no"),
        clean_text(F.col("_p.planId")).alias("plan_id"),
        parsed_price.alias("bid_price"),
        raw_price.alias("_raw_price"),
        currency.alias("currency"),
        clean_text(F.col("_p.processApply")).alias("process_apply"),
        clean_text(F.col("_p.bidField")).alias("bid_field"),
        clean_text(F.col("_p.bidForm")).alias("bid_form"),
        clean_text(F.col("_p.bidMode")).alias("bid_mode"),
        clean_text(F.col("_p.capitalDetail")).alias("capital_detail"),
        clean_text(F.col("_p.ctype")).alias("contract_type"),
        parsed_cperiod.alias("execution_period"),
        raw_cperiod.alias("_raw_cperiod"),
        clean_text(F.col("_p.cperiodUnit")).alias("execution_period_unit"),
        parse_boolean(F.col("_p.isDomestic")).alias("is_domestic"),
        parse_boolean(F.col("_p.isInternet")).alias("is_internet"),
        clean_text(F.col("_p.bidStartUnit")).alias("bid_start_unit"),
        parsed_year.alias("bid_start_year"),
        parsed_quarter.alias("bid_start_quarter"),
        parsed_month.alias("bid_start_month"),
        raw_month.alias("_raw_month_int"),
        raw_month_str.alias("_raw_month_str"),
        parsed_pub_date.alias("public_date"),
        F.col("_p.bidLocation").alias("_bid_locations"),
        F.col("_p.bidpBidLotList").alias("_bid_lots"),
    )

    quarantine_df = staged.filter(F.col("_quarantine_reason").isNotNull()).select(
        sha256_key(
            F.lit("khlcnt"),
            F.lit("khlcnt_bid_package_detail"),
            F.col("source_date"),
            F.col("source_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason"),
        ).alias("quarantine_id"),
        F.lit("khlcnt").alias("resource"),
        F.lit("khlcnt_bid_package_detail").alias("bronze_table"),
        F.col("source_date"),
        F.col("source_id"),
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

    pkg_issue_specs = [
        FieldIssueSpec(
            condition=F.col("_raw_price").isNotNull() & F.col("bid_price").isNull(),
            field_name="bid_price",
            issue_code="invalid_money",
            raw_value=F.col("_raw_price"),
        ),
        FieldIssueSpec(
            condition=F.col("bid_price") < F.lit(0),
            field_name="bid_price",
            issue_code="negative_money",
            raw_value=F.col("_raw_price"),
        ),
        FieldIssueSpec(
            condition=F.col("bid_price").isNotNull() & F.col("currency").isNull(),
            field_name="currency",
            issue_code="unknown_currency",
            raw_value=F.col("_raw_price"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_cperiod").isNotNull() & F.col("execution_period").isNull(),
            field_name="execution_period",
            issue_code="invalid_count",
            raw_value=F.col("_raw_cperiod"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_month_int").isNotNull() & (F.col("_raw_month_int") > 12),
            field_name="bid_start_month",
            issue_code="invalid_month",
            raw_value=F.col("_raw_month_str"),
        ),
        FieldIssueSpec(
            condition=F.col("package_no").isNull(),
            field_name="package_no",
            issue_code="missing_business_number",
            raw_value=F.col("source_id"),
        ),
    ]

    annotated_pkg, pkg_issues_df = extract_dq_issues(
        valid_deduped,
        resource="khlcnt",
        table_name="package_revision",
        specs=pkg_issue_specs,
    )

    pkg_rev_df = annotated_pkg.select(
        "revision_id",
        "entity_id",
        "source_id",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "package_no",
        "title",
        "plan_no",
        "plan_id",
        "bid_price",
        "currency",
        "process_apply",
        "bid_field",
        "bid_form",
        "bid_mode",
        "capital_detail",
        "contract_type",
        "execution_period",
        "execution_period_unit",
        "is_domestic",
        "is_internet",
        "bid_start_unit",
        "bid_start_year",
        "bid_start_quarter",
        "bid_start_month",
        "public_date",
        "dq_status",
    )

    pkg_loc_df = (
        valid_deduped.filter(
            F.col("_bid_locations").isNotNull() & (F.size(F.col("_bid_locations")) > 0)
        )
        .select(
            "revision_id",
            "source_date",
            F.posexplode(F.col("_bid_locations")).alias("location_ordinal", "_loc"),
        )
        .select(
            sha256_key(F.col("revision_id"), F.lit("location"), F.col("location_ordinal")).alias(
                "location_id"
            ),
            F.col("revision_id"),
            F.col("source_date"),
            F.col("location_ordinal").cast("int").alias("location_ordinal"),
            clean_pipe_code(F.col("_loc.provCode")).alias("province_code"),
            clean_text(F.col("_loc.provName")).alias("province_name"),
            clean_pipe_code(F.col("_loc.districtCode")).alias("district_code"),
            clean_text(F.col("_loc.districtName")).alias("district_name"),
            clean_pipe_code(F.col("_loc.wardCode")).alias("ward_code"),
            clean_text(F.col("_loc.wardName")).alias("ward_name"),
        )
    )

    pkg_lot_df = (
        valid_deduped.filter(F.col("_bid_lots").isNotNull() & (F.size(F.col("_bid_lots")) > 0))
        .select(
            "revision_id",
            "source_date",
            F.col("currency").alias("_pkg_currency"),
            F.posexplode(F.col("_bid_lots")).alias("lot_ordinal", "_lot"),
        )
        .withColumn("_source_lot_id", clean_text(F.col("_lot.id")))
        .select(
            sha256_key(
                F.col("revision_id"), F.lit("lot"), F.col("lot_ordinal"), F.col("_source_lot_id")
            ).alias("lot_id"),
            F.col("revision_id"),
            F.col("source_date"),
            F.col("_source_lot_id").alias("source_lot_id"),
            F.col("lot_ordinal").cast("int").alias("lot_ordinal"),
            clean_text(F.col("_lot.lotNo")).alias("lot_no"),
            clean_text(F.col("_lot.lotName")).alias("lot_name"),
            parse_decimal_38_6(F.col("_lot.lotPrice")).alias("lot_price"),
            F.coalesce(clean_text(F.col("_lot.lotPriceUnit")), F.col("_pkg_currency")).alias(
                "currency"
            ),
            clean_text(F.col("_lot.medicineCode")).alias("medicine_code"),
            parse_decimal_38_6(F.col("_lot.quantity")).alias("quantity"),
            clean_text(F.col("_lot.uom")).alias("uom"),
            parse_decimal_38_6(F.col("_lot.pricePlan")).alias("planned_unit_price"),
        )
    )

    raw_counts = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    return pkg_rev_df, pkg_loc_df, pkg_lot_df, pkg_issues_df, quarantine_df, raw_counts


def transform_khlcnt(bronze_by_table: dict[str, DataFrame]) -> KhlcntTransformOutput:
    """Pure Spark transformation for KHLCNT (plan + package + location + lot + shared DQ/quarantine)."""
    plan_df = bronze_by_table["khlcnt_plan_detail"]
    pkg_df = bronze_by_table["khlcnt_bid_package_detail"]

    plan_rev, plan_issues, plan_quar, plan_raw = _transform_plan(plan_df)
    pkg_rev, pkg_loc, pkg_lot, pkg_issues, pkg_quar, pkg_raw = _transform_package(pkg_df)

    dq_issues_df = plan_issues.unionByName(pkg_issues)
    quarantine_df = dedup_latest(
        plan_quar.unionByName(pkg_quar),
        partition_cols=["quarantine_id"],
        order_cols=[F.col("source_date").desc()],
    )

    raw_day = (
        plan_raw.unionByName(pkg_raw)
        .groupBy("source_date")
        .agg(
            F.sum("raw_input_rows").alias("raw_input_rows"),
            F.sum("valid_rows").alias("valid_rows"),
            F.sum("quarantined_rows").alias("quarantined_rows"),
        )
    )
    warn_day = (
        plan_rev.select("source_date", "dq_status")
        .unionByName(pkg_rev.select("source_date", "dq_status"))
        .groupBy("source_date")
        .agg(F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows"))
    )
    issue_day = dq_issues_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("issue_code") == "invalid_money", 1).otherwise(0)).alias("invalid_money_rows"),
        F.sum(F.when(F.col("issue_code") == "unknown_currency", 1).otherwise(0)).alias("unknown_currency_rows"),
    )
    combined_metrics = (
        raw_day.join(warn_day, "source_date", "left")
        .join(issue_day, "source_date", "left")
        .na.fill(0)
    )

    return KhlcntTransformOutput(
        tables={
            "plan_revision": plan_rev,
            "package_revision": pkg_rev,
            "package_location": pkg_loc,
            "package_lot": pkg_lot,
            SHARED_DQ_TABLE: dq_issues_df,
            SHARED_QUARANTINE_TABLE: quarantine_df,
        },
        per_day_metrics=combined_metrics,
    )
