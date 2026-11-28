"""Pure PySpark transformation for resource: notice (standard, vk_adb, reoffer).

Read once per Bronze family table with primitivesAsString=true, produce:
  - notice_revision
  - notice_lot
  - notice_location
  - notice_reoffer_submission
  - silver_dq_issues (shared)
  - silver_quarantine (shared)
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
    parse_vn_date,
    parse_vn_timestamp,
    sha256_key,
)
from procurement.silver.quality import (
    SHARED_DQ_TABLE,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement.silver.resources.notice.schema import NOTICE_PAYLOAD_SCHEMA

NOTICE_TRANSFORM_VERSION = "spark_notice_v1"
_JSON_OPTS = {"primitivesAsString": "true"}

_FAMILY_BRONZE_TABLES = (
    ("standard", "notify_contractor_standard_detail"),
    ("vk_adb", "notify_contractor_vk_adb_detail"),
    ("reoffer", "notify_contractor_reoffer_detail"),
)


@dataclass(frozen=True)
class NoticeTransformOutput:
    tables: dict[str, DataFrame]
    per_day_metrics: DataFrame


def _transform_family(
    df: DataFrame,
    *,
    family: str,
    bronze_table: str,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    parsed = df.withColumn("_p", F.from_json(F.col("payload"), NOTICE_PAYLOAD_SCHEMA, _JSON_OPTS))

    if family == "standard":
        root = F.coalesce(
            F.col("_p.bidoNotifyContractorM"),
            F.col("_p.bidNoContractorResponse.bidNotification"),
        )
        embedded_pkg = F.col("_p.bidpPlanDetail")
    elif family == "vk_adb":
        root = F.coalesce(
            F.col("_p.bidoNotifyContractorP"),
            F.col("_p.bidNoContractorResponse.bidNotification"),
        )
        embedded_pkg = F.col("_p.bidpPlanDetail")
    else:  # reoffer: top-level payload IS the root
        root = F.col("_p")
        embedded_pkg = F.col("_p.bidDetail")

    identity_field = "reofferNo" if family == "reoffer" else "notifyNo"
    version_field = "reofferVersion" if family == "reoffer" else "notifyVersion"

    business_id = clean_text(root.getField(identity_field))
    resolved_ver = F.coalesce(
        clean_text(root.getField(version_field)),
        clean_text(F.col("source_version")),
    )

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(root.isNull(), F.lit("missing_root"))
        .when(business_id.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    # Monetary fields
    raw_bid_price = clean_text(root.getField("bidPrice"))
    bid_price = parse_decimal_38_6(raw_bid_price)
    raw_est_price = clean_text(root.getField("bidEstimatePrice"))
    est_price = parse_decimal_38_6(raw_est_price)
    guar_src = "bidGuaranteeValue" if family == "vk_adb" else "guaranteeValue"
    raw_guar = clean_text(root.getField(guar_src))
    guar_amt = parse_decimal_38_6(raw_guar)
    guar_form_primary = "bidGuaranteeForm" if family == "vk_adb" else "guaranteeForm"
    guar_form_fallback = "guaranteeForm" if family == "vk_adb" else "bidGuaranteeForm"
    guarantee_form = F.coalesce(
        clean_text(root.getField(guar_form_primary)),
        clean_text(root.getField(guar_form_fallback)),
    )
    raw_init = clean_text(root.getField("priceInit"))
    init_price = parse_decimal_38_6(raw_init)
    raw_step = clean_text(root.getField("priceStep"))
    step_price = parse_decimal_38_6(raw_step)
    fee_amt = parse_decimal_38_6(root.getField("feeValue"))

    is_domestic = parse_boolean(root.getField("isDomestic"))
    base_currency = clean_text(root.getField("bidPriceUnit"))
    raw_est_curr = clean_text(root.getField("bidEstimatePriceUnit"))
    raw_guar_curr = clean_text(root.getField("bidGuaranteeUnit"))
    reoffer_curr = clean_text(root.getField("reofferPriceUnit"))

    # Domestic standard notices inherit currency (or VND only if is_domestic is NOT False)
    if family == "standard":
        inferred_curr = F.when(
            base_currency.isNotNull(), base_currency
        ).when(
            is_domestic.isNull() | (is_domestic == F.lit(True)), F.lit("VND")
        )
        est_curr = F.when(
            raw_est_curr.isNotNull(), raw_est_curr
        ).when(est_price.isNotNull(), inferred_curr)
        guar_curr = F.when(
            raw_guar_curr.isNotNull(), raw_guar_curr
        ).when(guar_amt.isNotNull(), inferred_curr)
    else:
        est_curr = raw_est_curr
        guar_curr = raw_guar_curr

    package_id = clean_text(root.getField("bidId"))
    emb_pkg_matches = (
        embedded_pkg.isNotNull()
        & package_id.isNotNull()
        & (clean_text(embedded_pkg.getField("id")) == package_id)
    )
    plan_id = F.when(emb_pkg_matches, clean_text(embedded_pkg.getField("planId")))

    is_multi_lot = F.coalesce(
        parse_boolean(root.getField("isMultiLot")),
        parse_boolean(embedded_pkg.getField("isMultiLot")),
        parse_boolean(F.col("_p.bidoBidStatus.isMultiLot")),
    )
    bid_field = F.coalesce(
        clean_text(root.getField("investField")),
        clean_text(embedded_pkg.getField("bidField")),
    )
    ct_field = "cType" if family == "vk_adb" else "contractType"
    dur_field = "cPeriod" if family == "vk_adb" else "contractPeriod"
    dur_unit_field = "cPeriodUnit" if family == "vk_adb" else "contractPeriodUnit"

    raw_pub = clean_text(root.getField("publicDate"))
    pub_date = parse_vn_date(raw_pub)
    pub_ts = parse_vn_timestamp(raw_pub)
    orig_pub_ts = parse_vn_timestamp(root.getField("originalPublicDate"))
    open_ts = parse_vn_timestamp(root.getField("bidOpenDate"))
    close_ts = parse_vn_timestamp(root.getField("bidCloseDate"))
    raw_plan_dec = clean_text(root.getField("planDecisionDate"))
    plan_dec_date = parse_vn_date(raw_plan_dec)

    entity_id = sha256_key("muasamcong", "notice", "notify_no", business_id)
    revision_id = sha256_key(entity_id, resolved_ver, F.col("content_hash"), NOTICE_TRANSFORM_VERSION)

    # Lot list mirror resolution for standard
    if family == "standard":
        lots_arr = F.coalesce(
            F.col("_p.bidNoContractorResponse.bidNotification.lotDTOList"),
            root.getField("lotDTOList"),
        )
    else:
        lots_arr = root.getField("lotDTOList")

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
        F.lit(family).alias("source_family"),
        clean_text(root.getField("id")).alias("source_document_id"),
        resolved_ver.alias("source_version"),
        clean_text(root.getField("notifyNo")).alias("notify_no"),
        clean_text(root.getField("notifyVersion")).alias("notify_version"),
        clean_text(root.getField("reofferNo")).alias("reoffer_no"),
        clean_text(root.getField("reofferVersion")).alias("reoffer_version"),
        package_id.alias("package_id"),
        clean_text(root.getField("bidNo")).alias("package_no"),
        plan_id.alias("plan_id"),
        clean_text(root.getField("planNo")).alias("plan_no"),
        clean_text(root.getField("planName")).alias("plan_name"),
        clean_text(root.getField("planType")).alias("plan_type_raw"),
        clean_text(root.getField("projectName")).alias("project_name"),
        clean_text(root.getField("bidName")).alias("title"),
        clean_text(root.getField("procuringEntityCode")).alias("procuring_entity_code"),
        clean_text(root.getField("procuringEntityName")).alias("procuring_entity_name"),
        clean_text(root.getField("investorCode")).alias("investor_code"),
        clean_text(root.getField("investorName")).alias("investor_name"),
        clean_text(root.getField("processApply")).alias("process_apply_raw"),
        bid_field.alias("bid_field"),
        clean_text(root.getField("bidForm")).alias("bid_form"),
        clean_text(root.getField("bidMode")).alias("bid_mode"),
        clean_text(root.getField("notifyType")).alias("notification_type"),
        clean_text(root.getField("status")).alias("status_raw"),
        bid_price.alias("bid_price"),
        raw_bid_price.alias("_raw_bid_price"),
        base_currency.alias("currency"),
        est_price.alias("estimate_price"),
        raw_est_price.alias("_raw_est_price"),
        est_curr.alias("estimate_currency"),
        guar_amt.alias("guarantee_amount"),
        raw_guar.alias("_raw_guar"),
        guar_curr.alias("guarantee_currency"),
        guarantee_form.alias("guarantee_form"),
        init_price.alias("initial_price"),
        step_price.alias("price_step"),
        reoffer_curr.alias("reoffer_currency"),
        fee_amt.alias("fee_amount"),
        clean_text(root.getField(ct_field)).alias("contract_type"),
        F.coalesce(clean_text(root.getField(dur_field)), clean_text(embedded_pkg.getField("cperiod"))).alias(
            "contract_duration_raw"
        ),
        F.coalesce(
            clean_text(root.getField(dur_unit_field)), clean_text(embedded_pkg.getField("cperiodUnit"))
        ).alias("contract_duration_unit"),
        parse_boolean(root.getField("isInternet")).alias("is_internet"),
        is_domestic.alias("is_domestic"),
        is_multi_lot.alias("is_multi_lot"),
        parse_boolean(root.getField("isPrequalification")).alias("is_prequalification"),
        pub_date.alias("public_date"),
        raw_pub.alias("_raw_pub"),
        pub_ts.alias("published_at"),
        orig_pub_ts.alias("original_published_at"),
        open_ts.alias("bid_open_date"),
        close_ts.alias("bid_close_date"),
        plan_dec_date.alias("plan_decision_date"),
        raw_plan_dec.alias("_raw_plan_dec"),
        parse_vn_timestamp(root.getField("cancelDate")).alias("cancelled_at"),
        parse_vn_date(root.getField("cancelDecisionDate")).alias("cancel_decision_date"),
        clean_text(root.getField("cancelReason")).alias("cancel_reason"),
        lots_arr.alias("_lots"),
        F.col("_p.bidpBidLocationList").alias("_loc_list_1"),
        F.col("_p.lsBidpBidLocationDTO").alias("_loc_list_2"),
        F.col("_p.bidDetail.bidLocation").alias("_loc_list_3"),
        F.col("_p.contractorsResult").alias("_reoffer_1"),
        F.col("_p.contractorsResultAll").alias("_reoffer_2"),
    )

    quarantine_df = staged.filter(F.col("_quarantine_reason").isNotNull()).select(
        sha256_key(
            F.lit("notice"),
            F.lit(bronze_table),
            F.col("source_date"),
            F.col("_bronze_source_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason"),
        ).alias("quarantine_id"),
        F.lit("notice").alias("resource"),
        F.lit(bronze_table).alias("bronze_table"),
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
            condition=F.col("_raw_bid_price").isNotNull() & F.col("bid_price").isNull(),
            field_name="bid_price",
            issue_code="invalid_money",
            raw_value=F.col("_raw_bid_price"),
        ),
        FieldIssueSpec(
            condition=F.col("bid_price") < F.lit(0),
            field_name="bid_price",
            issue_code="negative_money",
            raw_value=F.col("_raw_bid_price"),
        ),
        FieldIssueSpec(
            condition=F.col("bid_price").isNotNull() & F.col("currency").isNull(),
            field_name="currency",
            issue_code="unknown_currency",
            raw_value=F.col("_raw_bid_price"),
        ),
        FieldIssueSpec(
            condition=F.col("estimate_price").isNotNull() & F.col("estimate_currency").isNull(),
            field_name="estimate_currency",
            issue_code="unknown_currency",
            raw_value=F.col("_raw_est_price"),
        ),
        FieldIssueSpec(
            condition=F.col("guarantee_amount").isNotNull() & F.col("guarantee_currency").isNull(),
            field_name="guarantee_currency",
            issue_code="unknown_currency",
            raw_value=F.col("_raw_guar"),
        ),
        FieldIssueSpec(
            condition=(
                F.col("bid_close_date").isNotNull()
                & F.col("bid_open_date").isNotNull()
                & (F.col("bid_close_date") > F.col("bid_open_date"))
            ),
            field_name="bid_close_date",
            issue_code="invalid_date_order",
            raw_value=F.col("bid_close_date").cast("string"),
        ),
        FieldIssueSpec(
            condition=(
                F.col("plan_decision_date").isNotNull()
                & (
                    (F.col("plan_decision_date") < F.lit("2000-01-01").cast("date"))
                    | (F.col("plan_decision_date") > F.lit("2035-12-31").cast("date"))
                )
            ),
            field_name="plan_decision_date",
            issue_code="suspect_decision_date",
            raw_value=F.col("_raw_plan_dec"),
        ),
    ]

    annotated, dq_issues_df = extract_dq_issues(
        valid_deduped,
        resource="notice",
        table_name="notice_revision",
        specs=issue_specs,
    )

    rev_df = annotated.select(
        "revision_id",
        "entity_id",
        "source_family",
        "source_document_id",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "notify_no",
        "notify_version",
        "reoffer_no",
        "reoffer_version",
        "package_id",
        "package_no",
        "plan_id",
        "plan_no",
        "plan_name",
        "plan_type_raw",
        "project_name",
        "title",
        "procuring_entity_code",
        "procuring_entity_name",
        "investor_code",
        "investor_name",
        "process_apply_raw",
        "bid_field",
        "bid_form",
        "bid_mode",
        "notification_type",
        "status_raw",
        "bid_price",
        "currency",
        "estimate_price",
        "estimate_currency",
        "guarantee_amount",
        "guarantee_currency",
        "guarantee_form",
        "initial_price",
        "price_step",
        "reoffer_currency",
        "fee_amount",
        "contract_type",
        "contract_duration_raw",
        "contract_duration_unit",
        "is_internet",
        "is_domestic",
        "is_multi_lot",
        "is_prequalification",
        "public_date",
        "published_at",
        "original_published_at",
        "bid_open_date",
        "bid_close_date",
        "plan_decision_date",
        "cancelled_at",
        "cancel_decision_date",
        "cancel_reason",
        "dq_status",
    )

    # Child 1: notice_lot
    lot_df = (
        valid_deduped.filter(F.col("_lots").isNotNull() & (F.size(F.col("_lots")) > 0))
        .select("revision_id", "source_date", F.posexplode(F.col("_lots")).alias("ordinal", "_l"))
        .select(
            sha256_key(F.col("revision_id"), F.lit("lot"), F.col("ordinal"), clean_text(F.col("_l.id"))).alias(
                "lot_id"
            ),
            F.col("revision_id"),
            F.col("source_date"),
            F.col("ordinal").cast("int").alias("ordinal"),
            clean_text(F.col("_l.id")).alias("source_lot_id"),
            clean_text(F.col("_l.lotNo")).alias("lot_no"),
            clean_text(F.col("_l.lotName")).alias("lot_name"),
            parse_decimal_38_6(F.col("_l.lotPrice")).alias("lot_price"),
            parse_decimal_38_6(F.col("_l.lotEstimatePrice")).alias("estimate_price"),
            clean_text(F.col("_l.lotPriceUnit")).alias("currency"),
            parse_decimal_38_6(F.col("_l.lotGuaranteeValue")).alias("guarantee_amount"),
            clean_text(F.col("_l.cperiod")).alias("contract_duration_raw"),
            clean_text(F.col("_l.cperiodUnit")).alias("contract_duration_unit"),
            clean_text(F.col("_l.notifyId")).alias("notify_id"),
            clean_text(F.col("_l.notifyNo")).alias("notify_no"),
        )
    )

    # Child 2: notice_location (union across the 3 possible location arrays)
    loc_dfs = []
    for col_name, src_label in (
        ("_loc_list_1", "bidpBidLocationList"),
        ("_loc_list_2", "lsBidpBidLocationDTO"),
        ("_loc_list_3", "bidDetail.bidLocation"),
    ):
        loc_dfs.append(
            valid_deduped.filter(F.col(col_name).isNotNull() & (F.size(F.col(col_name)) > 0))
            .select(
                "revision_id",
                "source_date",
                F.posexplode(F.col(col_name)).alias("ordinal", "_loc"),
            )
            .select(
                sha256_key(
                    F.col("revision_id"), F.lit(src_label), F.col("ordinal"), clean_text(F.col("_loc.id"))
                ).alias("location_id"),
                F.col("revision_id"),
                F.col("source_date"),
                F.lit(src_label).alias("source_list"),
                F.col("ordinal").cast("int").alias("ordinal"),
                clean_pipe_code(F.col("_loc.provCode")).alias("province_code"),
                clean_text(F.col("_loc.provName")).alias("province_name"),
                clean_pipe_code(F.col("_loc.districtCode")).alias("district_code"),
                clean_text(F.col("_loc.districtName")).alias("district_name"),
                clean_pipe_code(F.col("_loc.wardCode")).alias("ward_code"),
                clean_text(F.col("_loc.wardName")).alias("ward_name"),
            )
        )
    loc_df = loc_dfs[0].unionByName(loc_dfs[1]).unionByName(loc_dfs[2])

    # Child 3: notice_reoffer_submission (only for reoffer family)
    sub_dfs = []
    for col_name, src_label in (
        ("_reoffer_1", "contractorsResult"),
        ("_reoffer_2", "contractorsResultAll"),
    ):
        sub_source = valid_deduped if family == "reoffer" else valid_deduped.limit(0)
        sub_dfs.append(
            sub_source.filter(F.col(col_name).isNotNull() & (F.size(F.col(col_name)) > 0))
            .select(
                "revision_id",
                "source_date",
                F.posexplode(F.col(col_name)).alias("ordinal", "_s"),
            )
            .select(
                sha256_key(
                    F.col("revision_id"), F.lit(src_label), F.col("ordinal"), clean_text(F.col("_s.id"))
                ).alias("submission_row_id"),
                F.col("revision_id"),
                F.col("source_date"),
                F.lit(src_label).alias("source_list"),
                F.col("ordinal").cast("int").alias("ordinal"),
                clean_text(F.col("_s.reofferSubmissionId")).alias("submission_id"),
                clean_text(F.col("_s.reofferId")).alias("reoffer_id"),
                clean_text(F.col("_s.lotNo")).alias("lot_no"),
                clean_text(F.col("_s.contractorCode")).alias("contractor_code"),
                clean_text(F.col("_s.contractorName")).alias("contractor_name"),
                clean_text(F.col("_s.ventureCode")).alias("venture_code"),
                clean_text(F.col("_s.ventureName")).alias("venture_name"),
                clean_text(F.col("_s.times")).alias("times_raw"),
                parse_decimal_38_6(F.col("_s.reofferPrice")).alias("price"),
                parse_decimal_38_6(F.col("_s.reofferPriceFinal")).alias("final_price"),
                clean_text(F.col("_s.reofferPriceUnit")).alias("currency"),
                parse_vn_timestamp(F.col("_s.reofferDate")).alias("offered_at"),
                parse_boolean(F.col("_s.isNewest")).alias("is_newest"),
            )
        )
    sub_df = sub_dfs[0].unionByName(sub_dfs[1])

    raw_counts = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    return rev_df, lot_df, loc_df, sub_df, dq_issues_df, quarantine_df, raw_counts


def transform_notice(bronze_by_table: dict[str, DataFrame]) -> NoticeTransformOutput:
    """Pure Spark transformation across all 3 notice families."""
    revs, lots, locs, subs, issues, quars, raws = [], [], [], [], [], [], []
    for family, tbl in _FAMILY_BRONZE_TABLES:
        r, lt, lc, sb, iss, qr, rw = _transform_family(
            bronze_by_table[tbl], family=family, bronze_table=tbl
        )
        revs.append(r)
        lots.append(lt)
        locs.append(lc)
        subs.append(sb)
        issues.append(iss)
        quars.append(qr)
        raws.append(rw)

    rev_df = dedup_latest(
        revs[0].unionByName(revs[1]).unionByName(revs[2]),
        partition_cols=["revision_id"],
        order_cols=[F.col("ingested_at").desc_nulls_last(), F.col("source_date").desc()],
    )
    lot_df = lots[0].unionByName(lots[1]).unionByName(lots[2])
    loc_df = locs[0].unionByName(locs[1]).unionByName(locs[2])
    sub_df = subs[0].unionByName(subs[1]).unionByName(subs[2])
    dq_issues_df = issues[0].unionByName(issues[1]).unionByName(issues[2])
    quarantine_df = dedup_latest(
        quars[0].unionByName(quars[1]).unionByName(quars[2]),
        partition_cols=["quarantine_id"],
        order_cols=[F.col("source_date").desc()],
    )

    raw_day = (
        raws[0].unionByName(raws[1]).unionByName(raws[2])
        .groupBy("source_date")
        .agg(
            F.sum("raw_input_rows").alias("raw_input_rows"),
            F.sum("valid_rows").alias("valid_rows"),
            F.sum("quarantined_rows").alias("quarantined_rows"),
        )
    )
    warn_day = rev_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows")
    )
    issue_day = dq_issues_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("issue_code") == "invalid_money", 1).otherwise(0)).alias("invalid_money_rows"),
        F.sum(F.when(F.col("issue_code") == "unknown_currency", 1).otherwise(0)).alias("unknown_currency_rows"),
    )
    per_day_metrics = (
        raw_day.join(warn_day, "source_date", "left")
        .join(issue_day, "source_date", "left")
        .na.fill(0)
    )

    return NoticeTransformOutput(
        tables={
            "notice_revision": rev_df,
            "notice_lot": lot_df,
            "notice_location": loc_df,
            "notice_reoffer_submission": sub_df,
            SHARED_DQ_TABLE: dq_issues_df,
            SHARED_QUARANTINE_TABLE: quarantine_df,
        },
        per_day_metrics=per_day_metrics,
    )
