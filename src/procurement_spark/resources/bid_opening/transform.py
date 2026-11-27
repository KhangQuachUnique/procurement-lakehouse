"""Pure PySpark transformation for resource: bid_opening.

Reads Bronze table `bid_opening_detail` with primitivesAsString=true,
handles all 5 opening shapes (ldt_single_envelope, ldt_two_envelope,
khac_single_envelope, khac_two_envelope, adb_wb_single_envelope),
and produces:
  - opening_revision
  - opening_phase
  - opening_submission
  - opening_lot_submission
  - silver_dq_issues (shared)
  - silver_quarantine (shared)
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from procurement_spark.common import (
    clean_text,
    dedup_latest,
    parse_boolean,
    parse_decimal_38_6,
    parse_nonnegative_int,
    parse_vn_date,
    parse_vn_timestamp,
    sha256_key,
)
from procurement_spark.quality import (
    SHARED_DQ_TABLE,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement_spark.resources.bid_opening.schema import BID_OPENING_PAYLOAD_SCHEMA

BID_OPENING_TRANSFORM_VERSION = "spark_bid_opening_v1"
_JSON_OPTS = {"primitivesAsString": "true"}


@dataclass(frozen=True)
class BidOpeningTransformOutput:
    tables: dict[str, DataFrame]
    per_day_metrics: DataFrame


def _parse_bounded_decimal(col: Column, precision: int, scale: int) -> Column:
    """Safely cast decimal string to DECIMAL(precision, scale) returning NULL on overflow."""
    d38 = parse_decimal_38_6(col)
    limit = 10 ** (precision - scale)
    target = DecimalType(precision, scale)
    return F.when(d38.isNotNull() & (F.abs(d38) < F.lit(limit)), d38.cast(target)).otherwise(
        F.lit(None).cast(target)
    )


def _has_valid_part(part_col: str) -> Column:
    part = F.col(part_col)
    return (
        part.isNotNull()
        & clean_text(part.getField("error")).isNull()
        & (F.coalesce(F.lower(clean_text(part.getField("success"))), F.lit("true")) != F.lit("false"))
        & part.getField("bidSubmissionByContractorViewResponse").isNotNull()
        & part.getField("bidSubmissionByContractorViewResponse").getField("bidSubmissionDTOList").isNotNull()
    )


def _classify_shape() -> Column:
    has_adb = F.col("_p.opening_adb_wb").isNotNull()
    has_out = F.col("_p.opening_out").isNotNull()
    has_out_tech = F.col("_p.opening_out_technical").isNotNull()
    has_base = F.col("_p.notify").isNotNull() & F.col("_p.roundmng").isNotNull()
    has_ldt_single = F.col("_p.bid_open").isNotNull() & F.col("_p.lot_open_detail").isNotNull()
    has_ldt_tech = F.col("_p.bid_open_technical").isNotNull() & F.col("_p.lot_open_detail_technical").isNotNull()

    return (
        F.when(has_adb & ~has_out & ~has_out_tech & ~has_base, F.lit("adb_wb_single_envelope"))
        .when(has_out & ~has_adb & ~has_out_tech & ~has_base, F.lit("khac_single_envelope"))
        .when(has_out_tech & ~has_adb & ~has_out & ~has_base, F.lit("khac_two_envelope"))
        .when(has_base & has_ldt_single & ~has_ldt_tech, F.lit("ldt_single_envelope"))
        .when(has_base & has_ldt_tech & ~has_ldt_single, F.lit("ldt_two_envelope"))
        .otherwise(F.lit(None).cast("string"))
    )


def _resolve_root() -> Column:
    return F.coalesce(
        F.col("_p.notify.bidNoContractorResponse.bidNotification"),
        F.col("_p.opening_out.bidoNotifyContractorP"),
        F.col("_p.opening_out_technical.bidoNotifyContractorP"),
        F.col("_p.opening_adb_wb.bidNoContractorResponse.bidNotification"),
    )


def _resolve_round() -> Column:
    return F.coalesce(
        F.col("_p.roundmng.bidoBidroundMngViewDTO"),
        F.col("_p.opening_out.bidoBidroundOutMngViewDTO"),
        F.col("_p.opening_out_technical.bidoBidroundOutMngViewDTO"),
        F.col("_p.opening_adb_wb.bidoBidroundMngViewDTO"),
    )


def _infer_collection_status(shape: Column, root: Column, round_info: Column) -> Column:
    is_cancelled = (
        F.col("_p.cancellation_notice").isNotNull()
        | root.getField("cancelDate").isNotNull()
        | root.getField("cancelDecisionNo").isNotNull()
        | (root.getField("status") == F.lit("03"))
    )
    raw_fin_dt = clean_text(round_info.getField("successBidOpenDateTc"))

    ldt_single_ok = F.col("_p.lot_open_detail").isNotNull() & _has_valid_part("_p.bid_open")
    khac_single_ok = _has_valid_part("_p.opening_out")
    adb_single_ok = _has_valid_part("_p.opening_adb_wb")

    ldt_two_status = (
        F.when(~(F.col("_p.lot_open_detail_technical").isNotNull() & _has_valid_part("_p.bid_open_technical")), F.lit(None))
        .when(
            raw_fin_dt.isNull() & F.col("_p.bid_open_financial").isNull() & F.col("_p.lot_open_detail_financial").isNull(),
            F.lit("awaiting_financial"),
        )
        .when(
            raw_fin_dt.isNotNull() & F.col("_p.lot_open_detail_financial").isNotNull() & _has_valid_part("_p.bid_open_financial"),
            F.lit("complete"),
        )
        .otherwise(F.lit(None))
    )
    khac_two_status = (
        F.when(~_has_valid_part("_p.opening_out_technical"), F.lit(None))
        .when(raw_fin_dt.isNull() & F.col("_p.opening_out_financial").isNull(), F.lit("awaiting_financial"))
        .when(raw_fin_dt.isNotNull() & _has_valid_part("_p.opening_out_financial"), F.lit("complete"))
        .otherwise(F.lit(None))
    )

    return (
        F.when(shape.isNull() | is_cancelled | round_info.isNull(), F.lit(None).cast("string"))
        .when(shape == "ldt_single_envelope", F.when(ldt_single_ok, F.lit("complete")))
        .when(shape == "khac_single_envelope", F.when(khac_single_ok, F.lit("complete")))
        .when(shape == "adb_wb_single_envelope", F.when(adb_single_ok, F.lit("complete")))
        .when(shape == "ldt_two_envelope", ldt_two_status)
        .when(shape == "khac_two_envelope", khac_two_status)
        .otherwise(F.lit(None).cast("string"))
    )


def _sub_shape(part: Column) -> Column:
    return (
        F.when(part.isNull(), F.lit("not_fetched"))
        .when(part.getField("bidSubmissionByContractorViewResponse").isNull(), F.lit("null"))
        .when(
            part.getField("bidSubmissionByContractorViewResponse").getField("bidSubmissionDTOList").isNull(),
            F.lit("null"),
        )
        .otherwise(F.lit("list"))
    )


def _build_phase_entries(shape: Column) -> Column:
    """Build a compact 1-or-2 element array of phase structs per row (O(1) Catalyst plan)."""

    def _entry(phase: str, path: str, part: Column, opened_at: Column, cnt: Column, ldt_lots: Column) -> Column:
        return F.struct(
            F.lit(phase).alias("phase"),
            F.lit(path).alias("source_path"),
            opened_at.alias("opened_at"),
            cnt.alias("reported_contractor_count"),
            _sub_shape(part).alias("submission_shape"),
            part.getField("bidSubmissionByContractorViewResponse")
            .getField("bidSubmissionDTOList")
            .alias("submissions"),
            ldt_lots.alias("ldt_lots"),
        )

    no_lots = F.lit(None).cast(BID_OPENING_PAYLOAD_SCHEMA["lot_open_detail"].dataType)
    no_cnt = F.lit(None).cast("int")

    return (
        F.when(
            shape == "ldt_single_envelope",
            F.array(
                _entry(
                    "single",
                    "bid_open.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.bid_open"),
                    F.col("actual_open_date"),
                    F.col("total_bidders"),
                    F.col("_p.lot_open_detail"),
                )
            ),
        )
        .when(
            shape == "ldt_two_envelope",
            F.array(
                _entry(
                    "technical",
                    "bid_open_technical.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.bid_open_technical"),
                    F.col("technical_open_date"),
                    F.col("total_bidders"),
                    F.col("_p.lot_open_detail_technical"),
                ),
                _entry(
                    "financial",
                    "bid_open_financial.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.bid_open_financial"),
                    F.col("financial_open_date"),
                    no_cnt,
                    F.col("_p.lot_open_detail_financial"),
                ),
            ),
        )
        .when(
            shape == "khac_single_envelope",
            F.array(
                _entry(
                    "single",
                    "opening_out.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.opening_out"),
                    F.col("actual_open_date"),
                    F.col("total_bidders"),
                    no_lots,
                )
            ),
        )
        .when(
            shape == "khac_two_envelope",
            F.array(
                _entry(
                    "technical",
                    "opening_out_technical.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.opening_out_technical"),
                    F.col("technical_open_date"),
                    F.col("total_bidders"),
                    no_lots,
                ),
                _entry(
                    "financial",
                    "opening_out_financial.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.opening_out_financial"),
                    F.col("financial_open_date"),
                    no_cnt,
                    no_lots,
                ),
            ),
        )
        .when(
            shape == "adb_wb_single_envelope",
            F.array(
                _entry(
                    "single",
                    "opening_adb_wb.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
                    F.col("_p.opening_adb_wb"),
                    F.col("actual_open_date"),
                    F.col("total_bidders"),
                    no_lots,
                )
            ),
        )
    )


def transform_bid_opening(bronze_by_table: dict[str, DataFrame]) -> BidOpeningTransformOutput:
    """Pure Spark transformation for bid_opening_detail."""
    df = bronze_by_table["bid_opening_detail"]
    parsed = (
        df.withColumn("_p", F.from_json(F.col("payload"), BID_OPENING_PAYLOAD_SCHEMA, _JSON_OPTS))
        .withColumn("_root", _resolve_root())
        .withColumn("_round", _resolve_round())
        .withColumn("_shape", _classify_shape())
    )

    root = F.col("_root")
    round_info = F.col("_round")
    shape = F.col("_shape")
    notify_no = clean_text(root.getField("notifyNo"))
    notify_ver = clean_text(root.getField("notifyVersion"))
    resolved_ver = F.coalesce(notify_ver, clean_text(F.col("source_version")))
    collection_status = _infer_collection_status(shape, root, round_info)

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(root.isNull(), F.lit("missing_root"))
        .when(notify_no.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "opening", "notify_no", notify_no)
    revision_id = sha256_key(entity_id, resolved_ver, F.col("content_hash"), BID_OPENING_TRANSFORM_VERSION)

    cancel_resp = F.col("_p.cancellation_notice.bidCancelingResponse")
    actual_open = parse_vn_timestamp(round_info.getField("successBidOpenDate"))
    is_two_env = shape.isin("ldt_two_envelope", "khac_two_envelope")
    tech_open = F.when(is_two_env, actual_open)
    fin_open = F.when(is_two_env, parse_vn_timestamp(round_info.getField("successBidOpenDateTc")))

    raw_pub = clean_text(root.getField("publicDate"))
    pub_date = parse_vn_date(raw_pub)

    staged = parsed.select(
        F.col("source_date"),
        F.col("source_id").cast("string").alias("_bronze_source_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("content_hash").cast("string").alias("content_hash"),
        F.col("payload").alias("_raw_payload"),
        F.col("_p"),
        quarantine_reason.alias("_quarantine_reason"),
        entity_id.alias("entity_id"),
        revision_id.alias("revision_id"),
        notify_no.alias("notify_no"),
        notify_ver.alias("notify_version"),
        resolved_ver.alias("source_version"),
        notify_no.alias("business_number"),
        clean_text(root.getField("bidName")).alias("title"),
        clean_text(root.getField("processApply")).alias("process_apply"),
        clean_text(root.getField("bidForm")).alias("bid_form"),
        clean_text(root.getField("bidMode")).alias("bid_mode"),
        clean_text(root.getField("bidNo")).alias("package_no"),
        clean_text(root.getField("bidId")).alias("package_id"),
        shape.alias("opening_shape"),
        parse_boolean(round_info.getField("isMultiLot")).alias("is_multi_lot"),
        parse_vn_timestamp(root.getField("bidOpenDate")).alias("scheduled_open_date"),
        tech_open.alias("technical_open_date"),
        fin_open.alias("financial_open_date"),
        actual_open.alias("actual_open_date"),
        parse_vn_timestamp(F.coalesce(cancel_resp.getField("cancelDate"), root.getField("cancelDate"))).alias(
            "cancel_date"
        ),
        parse_vn_date(
            F.coalesce(cancel_resp.getField("cancelDecisionDate"), root.getField("cancelDecisionDate"))
        ).alias("cancel_decision_date"),
        clean_text(F.coalesce(cancel_resp.getField("cancelDecisionNo"), root.getField("cancelDecisionNo"))).alias(
            "cancel_decision_no"
        ),
        clean_text(F.coalesce(cancel_resp.getField("cancelReason"), root.getField("cancelReason"))).alias(
            "cancel_reason"
        ),
        collection_status.alias("collection_status"),
        parse_nonnegative_int(round_info.getField("countContractor")).alias("total_bidders"),
        pub_date.alias("public_date"),
        raw_pub.alias("_raw_pub"),
        clean_text(root.getField("status")).alias("status_raw"),
    )

    quarantine_df = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNotNull()).select(
            sha256_key(
                F.lit("bid_opening"),
                F.lit("bid_opening_detail"),
                F.col("source_date"),
                F.col("_bronze_source_id"),
                F.col("content_hash"),
                F.col("_quarantine_reason"),
            ).alias("quarantine_id"),
            F.lit("bid_opening").alias("resource"),
            F.lit("bid_opening_detail").alias("bronze_table"),
            F.col("source_date"),
            F.col("_bronze_source_id").alias("source_id"),
            F.col("run_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason").alias("reason"),
            F.col("_raw_payload").alias("raw_payload"),
        ),
        partition_cols=["quarantine_id"],
        order_cols=[F.col("source_date").desc()],
    )

    valid_deduped = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNull()),
        partition_cols=["revision_id"],
        order_cols=[F.col("ingested_at").desc_nulls_last(), F.col("source_date").desc()],
    )

    issue_specs = [
        FieldIssueSpec(
            condition=F.col("opening_shape").isNull(),
            field_name="opening_shape",
            issue_code="unknown_opening_shape",
            raw_value=F.col("notify_no"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_pub").isNotNull() & F.col("public_date").isNull(),
            field_name="public_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_pub"),
        ),
    ]

    annotated, dq_issues_df = extract_dq_issues(
        valid_deduped,
        resource="bid_opening",
        table_name="opening_revision",
        specs=issue_specs,
    )

    opening_revision_df = annotated.select(
        "revision_id",
        "entity_id",
        "notify_no",
        "notify_version",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "business_number",
        "title",
        "process_apply",
        "bid_form",
        "bid_mode",
        "package_no",
        "package_id",
        "opening_shape",
        "is_multi_lot",
        "scheduled_open_date",
        "technical_open_date",
        "financial_open_date",
        "actual_open_date",
        "cancel_date",
        "cancel_decision_date",
        "cancel_decision_no",
        "cancel_reason",
        "collection_status",
        "total_bidders",
        "public_date",
        "status_raw",
        "dq_status",
    )

    # Single compact phase explosion for all 5 shapes (O(1) Catalyst plan)
    phases_exploded = (
        valid_deduped.filter(F.col("opening_shape").isNotNull())
        .select(
            "revision_id",
            "source_date",
            "opening_shape",
            F.explode(_build_phase_entries(F.col("opening_shape"))).alias("_ph"),
        )
    )

    opening_phase_df = phases_exploded.select(
        sha256_key(F.col("revision_id"), F.lit("opening_phase"), F.col("_ph.phase")).alias("phase_id"),
        F.col("revision_id"),
        F.col("source_date"),
        F.col("_ph.phase").alias("phase"),
        F.col("_ph.source_path").alias("source_path"),
        F.col("_ph.opened_at").alias("opened_at"),
        F.col("_ph.reported_contractor_count").alias("reported_contractor_count"),
        F.col("_ph.submission_shape").alias("submission_shape"),
    )

    # Single submission explosion across all shapes
    subs_exploded = (
        phases_exploded.filter(
            F.col("_ph.submissions").isNotNull() & (F.size(F.col("_ph.submissions")) > 0)
        )
        .select(
            "revision_id",
            "source_date",
            "opening_shape",
            F.col("_ph.phase").alias("phase"),
            F.posexplode(F.col("_ph.submissions")).alias("ordinal", "_s"),
        )
        .filter(F.col("_s").isNotNull() & clean_text(F.col("_s.contractorCode")).isNotNull())
        .withColumn(
            "submission_id",
            sha256_key(
                F.col("revision_id"),
                F.lit("opening_submission"),
                F.col("phase"),
                F.col("ordinal"),
                clean_text(F.col("_s.id")),
            ),
        )
    )

    opening_submission_df = subs_exploded.select(
        F.col("submission_id"),
        F.col("revision_id"),
        F.col("source_date"),
        F.col("ordinal").cast("int").alias("ordinal"),
        clean_text(F.col("_s.id")).alias("source_submission_id"),
        F.col("phase"),
        clean_text(F.col("_s.lotNo")).alias("lot_no"),
        clean_text(F.col("_s.contractorCode")).alias("contractor_code"),
        clean_text(F.col("_s.contractorName")).alias("contractor_name"),
        clean_text(F.col("_s.ventureCode")).alias("venture_code"),
        clean_text(F.col("_s.ventureName")).alias("venture_name"),
        parse_decimal_38_6(F.col("_s.bidPrice")).alias("bid_price"),
        clean_text(F.col("_s.bidPriceUnit")).alias("currency"),
        _parse_bounded_decimal(F.col("_s.saleNumber"), 10, 4).alias("discount_rate"),
        parse_decimal_38_6(F.col("_s.bidFinalPrice")).alias("final_bid_price"),
        clean_text(F.col("_s.techResult")).alias("tech_result"),
        _parse_bounded_decimal(F.col("_s.techScore"), 18, 6).alias("tech_score"),
        parse_vn_timestamp(F.col("_s.createdDateBidOpen")).alias("submission_time"),
    )

    # Path A: LDT top-level lot_open_detail*
    ldt_sub_lookup = (
        subs_exploded.select(
            F.col("revision_id").alias("_sub_rev_id"),
            F.col("phase").alias("_sub_phase"),
            clean_text(F.col("_s.contractorCode")).alias("_sub_cc"),
            "submission_id",
        )
        .groupBy("_sub_rev_id", "_sub_phase", "_sub_cc")
        .agg(
            F.count("*").alias("_cnt"),
            F.first("submission_id").alias("_uniq_sub_id"),
        )
        .filter(F.col("_cnt") == 1)
        .select("_sub_rev_id", "_sub_phase", "_sub_cc", "_uniq_sub_id")
    )

    ldt_lots_df = (
        phases_exploded.filter(F.col("_ph.ldt_lots").isNotNull() & (F.size(F.col("_ph.ldt_lots")) > 0))
        .select(
            "revision_id",
            "source_date",
            F.col("_ph.phase").alias("phase"),
            F.posexplode(F.col("_ph.ldt_lots")).alias("lot_idx", "_l"),
        )
        .filter(F.col("_l").isNotNull())
        .withColumn("_cc", clean_text(F.col("_l.contractorCode")))
        .join(
            ldt_sub_lookup,
            (F.col("revision_id") == F.col("_sub_rev_id"))
            & (F.col("phase") == F.col("_sub_phase"))
            & (F.col("_cc") == F.col("_sub_cc")),
            "left",
        )
        .select(
            sha256_key(
                F.col("revision_id"),
                F.lit("opening_lot_submission"),
                F.col("phase"),
                F.lit("ldt"),
                F.col("lot_idx"),
            ).alias("lot_submission_id"),
            F.col("revision_id"),
            F.col("_uniq_sub_id").alias("submission_id"),
            F.col("source_date"),
            F.col("phase"),
            clean_text(F.col("_l.id")).alias("source_lot_id"),
            clean_text(F.col("_l.bidNo")).alias("bid_no"),
            clean_text(F.col("_l.lotNo")).alias("lot_no"),
            clean_text(F.col("_l.lotName")).alias("lot_name"),
            F.col("_cc").alias("contractor_code"),
            clean_text(F.col("_l.contractorName")).alias("contractor_name"),
            parse_decimal_38_6(F.col("_l.lotPrice")).alias("lot_price"),
            parse_decimal_38_6(F.col("_l.lotFinalPrice")).alias("lot_final_price"),
            _parse_bounded_decimal(F.col("_l.discountPercent"), 10, 4).alias("discount_rate"),
            clean_text(F.col("_l.lotPriceUnit")).alias("currency"),
            parse_boolean(F.col("_l.passedYn")).alias("passed_yn"),
            _parse_bounded_decimal(F.col("_l.techScore"), 18, 6).alias("tech_score"),
            parse_decimal_38_6(F.col("_l.evalPrice")).alias("evaluation_price"),
            parse_nonnegative_int(F.col("_l.ranking")).alias("evaluation_rank"),
        )
    )

    # Path B: KHAC & ADB/WB nested lots inside submission.lotNoValueDTOList
    nested_parents = (
        subs_exploded.filter(
            ~F.col("opening_shape").isin("ldt_single_envelope", "ldt_two_envelope")
            & F.col("_s.lotNoValueDTOList").isNotNull()
            & (F.size(F.col("_s.lotNoValueDTOList")) > 0)
        )
        .select(
            "revision_id",
            "source_date",
            "phase",
            "submission_id",
            F.col("ordinal").alias("sub_idx"),
            clean_text(F.col("_s.contractorCode")).alias("_sub_cc"),
            clean_text(F.col("_s.contractorName")).alias("_sub_cname"),
            F.posexplode(F.col("_s.lotNoValueDTOList")).alias("p_idx", "_pl"),
        )
        .filter(F.col("_pl").isNotNull())
    )

    # Normalize bidOpenView vs direct parent lot into one array of _LOT_ITEM_SCHEMA structs
    parent_as_item = F.struct(
        F.col("_pl.id"),
        F.col("_pl.bidOpenId"),
        F.col("_pl.bidNo"),
        F.col("_pl.lotNo"),
        F.col("_pl.lotName"),
        F.col("_pl.contractorCode"),
        F.col("_pl.contractorName"),
        F.col("_pl.lotPrice"),
        F.col("_pl.lotFinalPrice"),
        F.col("_pl.discountPercent"),
        F.col("_pl.saleNumber"),
        F.col("_pl.lotPriceUnit"),
        F.col("_pl.passedYn"),
        F.col("_pl.techScore"),
        F.col("_pl.evalPrice"),
        F.col("_pl.ranking"),
    )
    effective_views = F.when(
        F.col("_pl.bidOpenView").isNotNull() & (F.size(F.col("_pl.bidOpenView")) > 0),
        F.col("_pl.bidOpenView"),
    ).otherwise(F.array(parent_as_item))

    nested_lots_df = (
        nested_parents.select(
            "revision_id",
            "source_date",
            "phase",
            "submission_id",
            "sub_idx",
            "p_idx",
            "_sub_cc",
            "_sub_cname",
            clean_text(F.col("_pl.bidNo")).alias("_p_bid_no"),
            clean_text(F.col("_pl.contractorCode")).alias("_p_cc"),
            clean_text(F.col("_pl.lotPriceUnit")).alias("_p_unit"),
            F.posexplode(effective_views).alias("v_idx", "_lot"),
        )
        .filter(F.col("_lot").isNotNull())
        .select(
            sha256_key(
                F.col("revision_id"),
                F.lit("opening_lot_submission"),
                F.col("phase"),
                F.col("sub_idx"),
                F.col("p_idx"),
                F.col("v_idx"),
            ).alias("lot_submission_id"),
            F.col("revision_id"),
            F.col("submission_id"),
            F.col("source_date"),
            F.col("phase"),
            clean_text(F.col("_lot.id")).alias("source_lot_id"),
            F.coalesce(clean_text(F.col("_lot.bidNo")), F.col("_p_bid_no")).alias("bid_no"),
            clean_text(F.col("_lot.lotNo")).alias("lot_no"),
            clean_text(F.col("_lot.lotName")).alias("lot_name"),
            F.coalesce(clean_text(F.col("_lot.contractorCode")), F.col("_p_cc"), F.col("_sub_cc")).alias(
                "contractor_code"
            ),
            F.coalesce(clean_text(F.col("_lot.contractorName")), F.col("_sub_cname")).alias("contractor_name"),
            parse_decimal_38_6(F.col("_lot.lotPrice")).alias("lot_price"),
            parse_decimal_38_6(F.col("_lot.lotFinalPrice")).alias("lot_final_price"),
            _parse_bounded_decimal(F.coalesce(F.col("_lot.discountPercent"), F.col("_lot.saleNumber")), 10, 4).alias(
                "discount_rate"
            ),
            F.coalesce(clean_text(F.col("_lot.lotPriceUnit")), F.col("_p_unit")).alias("currency"),
            parse_boolean(F.col("_lot.passedYn")).alias("passed_yn"),
            _parse_bounded_decimal(F.col("_lot.techScore"), 18, 6).alias("tech_score"),
            parse_decimal_38_6(F.col("_lot.evalPrice")).alias("evaluation_price"),
            parse_nonnegative_int(F.col("_lot.ranking")).alias("evaluation_rank"),
        )
    )

    opening_lot_submission_df = ldt_lots_df.unionByName(nested_lots_df)

    raw_day = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    warn_day = opening_revision_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows")
    )
    per_day_metrics = raw_day.join(warn_day, "source_date", "left").na.fill(0)

    return BidOpeningTransformOutput(
        tables={
            "opening_revision": opening_revision_df,
            "opening_phase": opening_phase_df,
            "opening_submission": opening_submission_df,
            "opening_lot_submission": opening_lot_submission_df,
            SHARED_DQ_TABLE: dq_issues_df,
            SHARED_QUARANTINE_TABLE: quarantine_df,
        },
        per_day_metrics=per_day_metrics,
    )
