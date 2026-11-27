"""Pure PySpark transformation for resource: contractor_result.

Reads Bronze table `contractor_result_detail` with primitivesAsString=true,
produces:
  - result_revision
  - award_lot
  - award_contractor (confirmed winners only: bidResult == '1')
  - silver_dq_issues (shared)
  - silver_quarantine (shared)
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from procurement_spark.common import (
    clean_text,
    dedup_latest,
    parse_boolean,
    parse_decimal_38_6,
    parse_vn_date,
    sha256_key,
)
from procurement_spark.quality import (
    SHARED_DQ_TABLE,
    SHARED_QUARANTINE_TABLE,
    FieldIssueSpec,
    extract_dq_issues,
)
from procurement_spark.resources.contractor_result.schema import RESULT_PAYLOAD_SCHEMA

CONTRACTOR_RESULT_TRANSFORM_VERSION = "spark_contractor_result_v1"
_JSON_OPTS = {"primitivesAsString": "true"}

_ROOT_FIELD_NAMES = (
    "id",
    "resultVersion",
    "notifyNo",
    "notifyVersion",
    "bidId",
    "bidNo",
    "planNo",
    "bidName",
    "procuringEntityCode",
    "procuringEntityName",
    "investorCode",
    "investorName",
    "decisionNo",
    "decisionDate",
    "decisionAgency",
    "bidPriceUnit",
    "bidEstimatePriceUnit",
    "publicDate",
    "status",
    "lotResultDTO",
)


@dataclass(frozen=True)
class ContractorResultTransformOutput:
    tables: dict[str, DataFrame]
    per_day_metrics: DataFrame


def _rfield(name: str):
    """Read field from bideContractorInputResultDTO, falling back to top-level payload."""
    return F.coalesce(
        F.col(f"_p.bideContractorInputResultDTO.{name}"),
        F.when(F.col("_p.bideContractorInputResultDTO").isNull(), F.col(f"_p.{name}")),
    )


def _clean_currency(col):
    """Return cleaned currency string; reject numeric/boolean-like literals."""
    txt = clean_text(col)
    is_bad = txt.isNotNull() & (
        F.lower(txt).isin("true", "false") | txt.rlike(r"^[+-]?\d+(\.\d+)?$")
    )
    return F.when(is_bad, F.lit(None).cast("string")).otherwise(txt)


def transform_contractor_result(
    bronze_by_table: dict[str, DataFrame],
) -> ContractorResultTransformOutput:
    """Pure Spark transformation for contractor_result_detail."""
    df = bronze_by_table["contractor_result_detail"]
    parsed = df.withColumn("_p", F.from_json(F.col("payload"), RESULT_PAYLOAD_SCHEMA, _JSON_OPTS))

    has_root = F.col("_p.bideContractorInputResultDTO").isNotNull() | F.col("_p.id").isNotNull()
    result_id = clean_text(_rfield("id"))
    result_ver = clean_text(_rfield("resultVersion"))
    resolved_ver = F.coalesce(result_ver, clean_text(F.col("source_version")))

    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(~has_root, F.lit("missing_root"))
        .when(result_id.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "result", "uuid", result_id)
    revision_id = sha256_key(entity_id, resolved_ver, F.col("content_hash"), CONTRACTOR_RESULT_TRANSFORM_VERSION)

    # Root currency conflict check
    root_u1 = _clean_currency(_rfield("bidPriceUnit"))
    root_u2 = _clean_currency(_rfield("bidEstimatePriceUnit"))
    root_curr_conflict = root_u1.isNotNull() & root_u2.isNotNull() & (root_u1 != root_u2)
    root_currency = F.when(root_curr_conflict, F.lit(None).cast("string")).otherwise(F.coalesce(root_u1, root_u2))

    notify_no = clean_text(_rfield("notifyNo"))
    package_no = clean_text(_rfield("bidNo"))
    raw_pub = clean_text(_rfield("publicDate"))
    pub_date = parse_vn_date(raw_pub)
    raw_dec = clean_text(_rfield("decisionDate"))
    dec_date = parse_vn_date(raw_dec)

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
        result_id.alias("result_id"),
        result_ver.alias("result_version"),
        resolved_ver.alias("source_version"),
        notify_no.alias("notify_no"),
        clean_text(_rfield("notifyVersion")).alias("notify_version"),
        F.coalesce(notify_no, package_no).alias("business_number"),
        clean_text(_rfield("bidId")).alias("bid_id"),
        package_no.alias("package_no"),
        clean_text(_rfield("planNo")).alias("plan_no"),
        clean_text(_rfield("bidName")).alias("title"),
        clean_text(_rfield("procuringEntityCode")).alias("procuring_entity_code"),
        clean_text(_rfield("procuringEntityName")).alias("procuring_entity_name"),
        clean_text(_rfield("investorCode")).alias("investor_code"),
        clean_text(_rfield("investorName")).alias("investor_name"),
        clean_text(_rfield("decisionNo")).alias("decision_no"),
        dec_date.alias("decision_date"),
        raw_dec.alias("_raw_dec"),
        clean_text(_rfield("decisionAgency")).alias("decision_agency"),
        pub_date.alias("public_date"),
        raw_pub.alias("_raw_pub"),
        clean_text(_rfield("status")).alias("status_raw"),
        root_currency.alias("_root_currency"),
        root_curr_conflict.alias("_root_curr_conflict"),
        _rfield("lotResultDTO").alias("_lots"),
    )

    quarantine_df = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNotNull()).select(
            sha256_key(
                F.lit("contractor_result"),
                F.lit("contractor_result_detail"),
                F.col("source_date"),
                F.col("_bronze_source_id"),
                F.col("content_hash"),
                F.col("_quarantine_reason"),
            ).alias("quarantine_id"),
            F.lit("contractor_result").alias("resource"),
            F.lit("contractor_result_detail").alias("bronze_table"),
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

    # ------------------------------------------------------------------
    # Explode lots and resolve lot currency & lot budget price
    # ------------------------------------------------------------------
    lots_raw = (
        valid_deduped.filter(F.col("_lots").isNotNull() & (F.size(F.col("_lots")) > 0))
        .select(
            "revision_id",
            "source_date",
            "_root_currency",
            F.posexplode(F.col("_lots")).alias("ordinal", "_l"),
        )
        .filter(F.col("_l").isNotNull())
    )

    lu1 = _clean_currency(F.col("_l.lotPriceUnit"))
    lu2 = _clean_currency(F.col("_l.currency"))
    lu3 = _clean_currency(F.col("_l.bidEstimatePriceUnit"))
    distinct_lu = F.array_distinct(F.filter(F.array(lu1, lu2, lu3), lambda x: x.isNotNull()))
    explicit_lot_curr = F.when(F.size(distinct_lu) == 1, F.element_at(distinct_lu, 1))
    lot_curr_conflict = (F.size(distinct_lu) > 1) | (
        explicit_lot_curr.isNotNull()
        & F.col("_root_currency").isNotNull()
        & (explicit_lot_curr != F.col("_root_currency"))
    )
    resolved_lot_curr = F.when(lot_curr_conflict, F.lit(None).cast("string")).otherwise(
        F.coalesce(explicit_lot_curr, F.col("_root_currency"))
    )

    raw_lot_price = clean_text(F.col("_l.lotPrice"))
    parsed_lot_price = parse_decimal_38_6(raw_lot_price)
    lot_price_clean = F.when(
        parsed_lot_price.isNull()
        | (parsed_lot_price <= F.lit(0))
        | ((parsed_lot_price <= F.lit(10)) & (resolved_lot_curr.isNull() | (resolved_lot_curr == F.lit("VND")))),
        F.lit(None).cast("decimal(38,6)"),
    ).otherwise(parsed_lot_price)

    source_lot_id = clean_text(F.col("_l.id"))
    lot_no = F.coalesce(
        clean_text(F.col("_l.lotNo")),
        clean_text(F.col("_l.bidNo")),
        (F.col("ordinal") + F.lit(1)).cast("string"),
    )
    award_lot_id = sha256_key(
        F.col("revision_id"),
        F.lit("award_lot"),
        F.col("ordinal"),
        F.coalesce(source_lot_id, lot_no),
    )

    lots_staged = lots_raw.select(
        award_lot_id.alias("award_lot_id"),
        F.col("revision_id"),
        F.col("source_date"),
        F.col("ordinal").cast("int").alias("ordinal"),
        source_lot_id.alias("source_lot_id"),
        lot_no.alias("lot_no"),
        F.coalesce(clean_text(F.col("_l.lotName")), clean_text(F.col("_l.bidName"))).alias("lot_name"),
        lot_price_clean.alias("lot_price"),
        resolved_lot_curr.alias("currency"),
        lot_curr_conflict.alias("_lot_curr_conflict"),
        clean_text(F.col("_l.winningCode")).alias("winning_code"),
        clean_text(F.col("_l.status")).alias("status"),
        F.col("_l.contractorList").alias("_contractors"),
    )

    # ------------------------------------------------------------------
    # Explode contractors per lot
    # ------------------------------------------------------------------
    contractors_raw = (
        lots_staged.filter(F.col("_contractors").isNotNull() & (F.size(F.col("_contractors")) > 0))
        .select(
            "award_lot_id",
            "revision_id",
            "source_date",
            F.col("ordinal").alias("lot_ordinal"),
            F.col("currency").alias("lot_currency"),
            F.posexplode(F.col("_contractors")).alias("c_ordinal", "_c"),
        )
        .filter(F.col("_c").isNotNull())
    )

    c_id = clean_text(F.col("_c.id"))
    contractor_code = F.coalesce(
        clean_text(F.col("_c.orgCode")),
        clean_text(F.col("_c.taxCode")),
        F.lit("UNKNOWN"),
    )
    contractor_name = F.coalesce(
        clean_text(F.col("_c.orgFullname")),
        clean_text(F.col("_c.newContractorName")),
        clean_text(F.col("_c.contractorName")),
        F.lit("UNKNOWN"),
    )
    venture_code = clean_text(F.col("_c.ventureCode"))
    venture_name = clean_text(F.col("_c.ventureName"))
    is_consortium = (
        venture_code.isNotNull()
        | venture_name.isNotNull()
        | F.coalesce(parse_boolean(F.col("_c.isConsortium")), F.lit(False))
        | F.coalesce(parse_boolean(F.col("_c.is_consortium")), F.lit(False))
    )

    raw_outcome = clean_text(F.col("_c.bidResult"))
    is_missing_outcome = raw_outcome.isNull()
    is_unsupported_outcome = raw_outcome.isNotNull() & ~raw_outcome.isin("0", "1", "2", "3")
    is_winner = raw_outcome == F.lit("1")

    raw_win_price = clean_text(F.col("_c.bidWiningPrice"))
    parsed_win_price = parse_decimal_38_6(raw_win_price)
    awarded_price = F.when(parsed_win_price < F.lit(0), F.lit(None).cast("decimal(38,6)")).otherwise(
        parsed_win_price
    )

    contractors_staged = contractors_raw.select(
        sha256_key(
            F.col("revision_id"),
            F.lit("award_contractor"),
            F.col("lot_ordinal"),
            F.col("c_ordinal"),
            F.coalesce(c_id, contractor_code),
        ).alias("award_contractor_id"),
        F.col("award_lot_id"),
        F.col("revision_id"),
        F.col("source_date"),
        F.col("c_ordinal").cast("int").alias("ordinal"),
        contractor_code.alias("contractor_code"),
        contractor_name.alias("contractor_name"),
        clean_text(F.col("_c.taxCode")).alias("tax_code"),
        F.coalesce(clean_text(F.col("_c.taxNation")), F.lit("VN")).alias("country"),
        is_consortium.alias("is_consortium"),
        venture_code.alias("venture_code"),
        venture_name.alias("venture_name"),
        clean_text(F.col("_c.role")).alias("role"),
        awarded_price.alias("awarded_price"),
        F.col("lot_currency").alias("currency"),
        is_winner.alias("_is_winner"),
        is_missing_outcome.alias("_is_missing_outcome"),
        is_unsupported_outcome.alias("_is_unsupported_outcome"),
        (
            is_winner
            & (
                (awarded_price == F.lit(0))
                | (
                    (awarded_price <= F.lit(10))
                    & (F.col("lot_currency").isNull() | (F.col("lot_currency") == F.lit("VND")))
                )
            )
        ).alias("_unverified_zero_price"),
    )

    # Child table: award_contractor (confirmed winners only)
    award_contractor_df = contractors_staged.filter(F.col("_is_winner")).select(
        "award_contractor_id",
        "award_lot_id",
        "revision_id",
        "source_date",
        "ordinal",
        "contractor_code",
        "contractor_name",
        "tax_code",
        "country",
        "is_consortium",
        "venture_code",
        "venture_name",
        "role",
        "awarded_price",
        F.lit(1).cast("int").alias("bid_result"),
        F.when(F.col("awarded_price").isNotNull(), F.lit("bidWiningPrice")).alias("price_source"),
        "currency",
    )

    # ------------------------------------------------------------------
    # Aggregate contractor stats per lot -> lot winning_price
    # ------------------------------------------------------------------
    lot_agg = contractors_staged.groupBy("award_lot_id").agg(
        F.sum(F.when(F.col("_is_winner"), 1).otherwise(0)).alias("_winner_cnt"),
        F.sum(F.when(F.col("_is_winner") & F.col("is_consortium"), 1).otherwise(0)).alias("_consortium_winner_cnt"),
        F.max(F.when(F.col("_is_winner") & F.col("awarded_price").isNull(), 1).otherwise(0)).alias(
            "_incomplete_winner_price"
        ),
        F.max(F.when(F.col("_unverified_zero_price"), 1).otherwise(0)).alias("_has_unverified_zero"),
        F.max(F.when(F.col("_is_missing_outcome") | F.col("_is_unsupported_outcome"), 1).otherwise(0)).alias(
            "_has_unresolved_outcome"
        ),
        F.sum(F.when(F.col("_is_winner"), F.col("awarded_price"))).cast("decimal(38,6)").alias("_sum_winner_price"),
    )

    lots_enriched = lots_staged.join(lot_agg, "award_lot_id", "left").withColumn(
        "_winner_cnt", F.coalesce(F.col("_winner_cnt"), F.lit(0))
    ).withColumn(
        "_consortium_ambiguity", F.coalesce(F.col("_consortium_winner_cnt"), F.lit(0)) > F.lit(1)
    ).withColumn(
        "_lot_unresolved",
        (F.coalesce(F.col("_has_unresolved_outcome"), F.lit(0)) > 0)
        | (F.coalesce(F.col("_incomplete_winner_price"), F.lit(0)) > 0)
        | (F.coalesce(F.col("_has_unverified_zero"), F.lit(0)) > 0)
        | F.col("_consortium_ambiguity")
        | F.col("_lot_curr_conflict")
        | ((F.col("_winner_cnt") > 0) & F.col("currency").isNull()),
    ).withColumn(
        "winning_price",
        F.when((F.col("_winner_cnt") > 0) & ~F.col("_lot_unresolved"), F.col("_sum_winner_price")).cast(
            "decimal(38,6)"
        ),
    )

    award_lot_df = lots_enriched.select(
        "award_lot_id",
        "revision_id",
        "source_date",
        "ordinal",
        "source_lot_id",
        "lot_no",
        "lot_name",
        "lot_price",
        "winning_price",
        "currency",
        "winning_code",
        "status",
    )

    # ------------------------------------------------------------------
    # Aggregate lot stats per revision -> total_winning_price & currency
    # ------------------------------------------------------------------
    rev_lot_agg = lots_enriched.groupBy("revision_id").agg(
        F.sum(F.when(F.col("_winner_cnt") > 0, 1).otherwise(0)).alias("_lots_with_winners"),
        F.max(F.when(F.col("_lot_unresolved"), 1).otherwise(0)).alias("_any_lot_unresolved"),
        F.max(F.when(F.col("_consortium_ambiguity"), 1).otherwise(0)).alias("_any_consortium_ambiguity"),
        F.max(F.when(F.col("_has_unresolved_outcome") > 0, 1).otherwise(0)).alias("_any_unresolved_outcome"),
        F.max(F.when(F.col("_incomplete_winner_price") > 0, 1).otherwise(0)).alias("_any_incomplete_price"),
        F.countDistinct(F.when(F.col("_winner_cnt") > 0, F.col("currency"))).alias("_distinct_winner_curr"),
        F.first(F.when(F.col("_winner_cnt") > 0, F.col("currency")), ignorenulls=True).alias("_first_winner_curr"),
        F.sum(F.when(F.col("_winner_cnt") > 0, F.col("winning_price"))).cast("decimal(38,6)").alias("_sum_lot_win"),
    )

    rev_joined = valid_deduped.join(rev_lot_agg, "revision_id", "left")

    can_derive_total = (
        (F.coalesce(F.col("_lots_with_winners"), F.lit(0)) > 0)
        & (F.coalesce(F.col("_any_lot_unresolved"), F.lit(0)) == 0)
        & ~F.col("_root_curr_conflict")
        & (F.coalesce(F.col("_distinct_winner_curr"), F.lit(0)) == 1)
    )
    total_winning_price = F.when(can_derive_total, F.col("_sum_lot_win")).cast("decimal(38,6)")
    final_currency = F.when(
        F.col("_root_currency").isNotNull(), F.col("_root_currency")
    ).when(can_derive_total, F.col("_first_winner_curr"))

    rev_pre_dq = rev_joined.withColumn("total_winning_price", total_winning_price).withColumn(
        "currency", final_currency
    )

    issue_specs = [
        FieldIssueSpec(
            condition=F.col("_root_curr_conflict"),
            field_name="currency",
            issue_code="conflicting_currency",
            raw_value=F.lit("bidPriceUnit!=bidEstimatePriceUnit"),
        ),
        FieldIssueSpec(
            condition=F.col("currency").isNull()
            & ((F.col("total_winning_price").isNotNull()) | (F.coalesce(F.col("_lots_with_winners"), F.lit(0)) > 0)),
            field_name="currency",
            issue_code="unknown_currency",
            raw_value=F.col("total_winning_price").cast("string"),
        ),
        FieldIssueSpec(
            condition=F.coalesce(F.col("_any_consortium_ambiguity"), F.lit(0)) > 0,
            field_name="total_winning_price",
            issue_code="unverified_consortium_allocation",
            raw_value=F.col("result_id"),
        ),
        FieldIssueSpec(
            condition=F.coalesce(F.col("_any_unresolved_outcome"), F.lit(0)) > 0,
            field_name="lotResultDTO",
            issue_code="unresolved_outcome",
            raw_value=F.col("result_id"),
        ),
        FieldIssueSpec(
            condition=F.coalesce(F.col("_any_incomplete_price"), F.lit(0)) > 0,
            field_name="total_winning_price",
            issue_code="incomplete_winner_price",
            raw_value=F.col("result_id"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_pub").isNotNull() & F.col("public_date").isNull(),
            field_name="public_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_pub"),
        ),
        FieldIssueSpec(
            condition=F.col("_raw_dec").isNotNull() & F.col("decision_date").isNull(),
            field_name="decision_date",
            issue_code="invalid_date",
            raw_value=F.col("_raw_dec"),
        ),
        FieldIssueSpec(
            condition=(
                F.col("decision_date").isNotNull()
                & F.col("public_date").isNotNull()
                & (F.col("decision_date") > F.col("public_date"))
            ),
            field_name="decision_date",
            issue_code="invalid_date_order",
            raw_value=F.col("_raw_dec"),
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
            raw_value=F.col("_raw_dec"),
        ),
    ]

    annotated, dq_issues_df = extract_dq_issues(
        rev_pre_dq,
        resource="contractor_result",
        table_name="result_revision",
        specs=issue_specs,
    )

    result_revision_df = annotated.select(
        "revision_id",
        "entity_id",
        "result_id",
        "result_version",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "notify_no",
        "notify_version",
        "business_number",
        "bid_id",
        "package_no",
        "plan_no",
        "title",
        "procuring_entity_code",
        "procuring_entity_name",
        "investor_code",
        "investor_name",
        "decision_no",
        "decision_date",
        "decision_agency",
        "total_winning_price",
        "currency",
        "public_date",
        "status_raw",
        "dq_status",
    )

    raw_day = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    warn_day = result_revision_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows")
    )
    issue_day = dq_issues_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("issue_code") == "unverified_consortium_allocation", 1).otherwise(0)).alias(
            "consortium_ambiguity_rows"
        ),
        F.sum(F.when(F.col("issue_code") == "suspect_decision_date", 1).otherwise(0)).alias(
            "suspect_decision_date_rows"
        ),
    )
    per_day_metrics = (
        raw_day.join(warn_day, "source_date", "left")
        .join(issue_day, "source_date", "left")
        .na.fill(0)
    )

    return ContractorResultTransformOutput(
        tables={
            "result_revision": result_revision_df,
            "award_lot": award_lot_df,
            "award_contractor": award_contractor_df,
            SHARED_DQ_TABLE: dq_issues_df,
            SHARED_QUARANTINE_TABLE: quarantine_df,
        },
        per_day_metrics=per_day_metrics,
    )
