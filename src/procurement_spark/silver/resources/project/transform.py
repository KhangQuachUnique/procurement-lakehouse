"""Pure PySpark transformation for resource: project (Bronze DF -> Silver DFs).

Zero I/O, zero Python UDFs: read Bronze once, parse JSON with primitivesAsString=true, produce:
  - project_revision (clean business table with lightweight dq_status)
  - project_location (exploded plocation JSON array + root-level fallback)
  - silver_dq_issues (shared field-level DQ issues table)
  - silver_quarantine (shared structural quarantine table)
  - per_day_metrics (grouped by source_date for accurate daily receipts)
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from procurement_spark.silver.common import (
    clean_pipe_code,
    clean_text,
    dedup_latest,
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
from procurement_spark.silver.resources.project.schema import (
    PLOCATION_ITEM_SCHEMA,
    PROJECT_PAYLOAD_SCHEMA,
)

PROJECT_TRANSFORM_VERSION = "spark_project_v2"
_JSON_OPTS = {"primitivesAsString": "true"}


@dataclass(frozen=True)
class ProjectTransformOutput:
    tables: dict[str, DataFrame]
    per_day_metrics: DataFrame


def _coalesce_dto(field: str):
    return F.coalesce(F.col(f"_p.projectDTO.{field}"), F.col(f"_p.{field}"))


def _coalesce_loc(field: str):
    dto_val = clean_text(F.col(f"_p.projectDTO.{field}"))
    top_val = clean_text(F.col(f"_p.{field}"))
    return F.coalesce(dto_val, top_val)


def transform_project(bronze_input: DataFrame | dict[str, DataFrame]) -> ProjectTransformOutput:
    """Pure Spark transformation from Bronze project_detail DataFrame to Silver output DataFrames."""
    bronze_df = bronze_input["project_detail"] if isinstance(bronze_input, dict) else bronze_input

    # 1. Parse raw JSON once with primitivesAsString=true (zero float precision loss)
    parsed = bronze_df.withColumn("_p", F.from_json(F.col("payload"), PROJECT_PAYLOAD_SCHEMA, _JSON_OPTS))

    # 2. Extract canonical fields & provenance
    raw_id = clean_text(_coalesce_dto("id"))
    raw_no = clean_text(_coalesce_dto("no"))
    raw_invest = clean_text(_coalesce_dto("investTotal"))
    parsed_invest = parse_decimal_38_6(raw_invest)
    currency = F.coalesce(
        clean_text(_coalesce_dto("investTotalUnit")),
        clean_text(_coalesce_dto("investUnit")),
    )
    raw_pub_date = clean_text(_coalesce_dto("publicDate"))
    parsed_pub_date = parse_vn_date(raw_pub_date)
    raw_dec_date = clean_text(_coalesce_dto("decisionDate"))
    parsed_dec_date = parse_vn_date(raw_dec_date)
    resolved_version = F.coalesce(
        clean_text(F.col("source_version")),
        clean_text(F.col("_p.projectDTO.version")),
        clean_text(F.col("_p.pversion")),
        clean_text(F.col("_p.version")),
    )

    has_root = F.col("_p").isNotNull() & (
        F.col("_p.projectDTO").isNotNull() | F.col("_p.id").isNotNull()
    )
    quarantine_reason = (
        F.when(F.col("_p").isNull(), F.lit("invalid_json"))
        .when(~has_root, F.lit("missing_root"))
        .when(raw_id.isNull(), F.lit("missing_identity"))
        .otherwise(F.lit(None).cast("string"))
    )

    entity_id = sha256_key("muasamcong", "project", "uuid", raw_id)
    revision_id = sha256_key(entity_id, resolved_version, F.col("content_hash"), PROJECT_TRANSFORM_VERSION)

    staged = parsed.select(
        F.col("source_date"),
        F.col("source_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("content_hash").cast("string").alias("content_hash"),
        F.col("payload").alias("_raw_payload"),
        quarantine_reason.alias("_quarantine_reason"),
        entity_id.alias("entity_id"),
        revision_id.alias("revision_id"),
        resolved_version.alias("source_version"),
        raw_no.alias("project_no"),
        clean_text(_coalesce_dto("name")).alias("title"),
        clean_text(_coalesce_dto("investorCode")).alias("investor_code"),
        clean_text(_coalesce_dto("investorName")).alias("investor_name"),
        parsed_invest.alias("total_investment"),
        raw_invest.alias("_raw_invest"),
        currency.alias("currency"),
        clean_text(_coalesce_dto("decisionNo")).alias("decision_no"),
        parsed_dec_date.alias("decision_date"),
        raw_dec_date.alias("_raw_dec_date"),
        parsed_pub_date.alias("public_date"),
        raw_pub_date.alias("public_date_raw"),
        clean_pipe_code(_coalesce_loc("provCode")).alias("prov_code"),
        clean_text(_coalesce_loc("provName")).alias("_prov_name"),
        clean_pipe_code(_coalesce_loc("districtCode")).alias("district_code"),
        clean_text(_coalesce_loc("districtName")).alias("_district_name"),
        _coalesce_loc("plocation").alias("_plocation_raw"),
        clean_text(_coalesce_dto("status")).alias("status_raw"),
    )

    # 3. Shared silver_quarantine output
    quarantine_df = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNotNull()).select(
            sha256_key(
                F.lit("project"),
                F.lit("project_detail"),
                F.col("source_date"),
                F.col("source_id"),
                F.col("content_hash"),
                F.col("_quarantine_reason"),
            ).alias("quarantine_id"),
            F.lit("project").alias("resource"),
            F.lit("project_detail").alias("bronze_table"),
            F.col("source_date"),
            F.col("source_id"),
            F.col("run_id"),
            F.col("content_hash"),
            F.col("_quarantine_reason").alias("reason"),
            F.col("_raw_payload").alias("raw_payload"),
        ),
        partition_cols=["quarantine_id"],
        order_cols=[F.col("source_date").desc()],
    )

    # 4. Valid deduplicated rows + normalized field-level DQ extraction
    valid_deduped = dedup_latest(
        staged.filter(F.col("_quarantine_reason").isNull()),
        partition_cols=["revision_id"],
        order_cols=[F.col("ingested_at").desc_nulls_last(), F.col("source_date").desc()],
    )

    issue_specs = [
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
            condition=F.col("public_date_raw").isNotNull() & F.col("public_date").isNull(),
            field_name="public_date",
            issue_code="invalid_date",
            raw_value=F.col("public_date_raw"),
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
                & F.col("public_date").isNotNull()
                & (F.col("decision_date") > F.col("public_date"))
            ),
            field_name="decision_date",
            issue_code="invalid_date_order",
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
            condition=F.col("project_no").isNull(),
            field_name="project_no",
            issue_code="missing_business_number",
            raw_value=F.col("source_id"),
        ),
    ]

    annotated_valid, dq_issues_df = extract_dq_issues(
        valid_deduped,
        resource="project",
        table_name="project_revision",
        specs=issue_specs,
    )

    revision_df = annotated_valid.select(
        "revision_id",
        "entity_id",
        "source_id",
        "source_version",
        "source_date",
        "ingested_at",
        "run_id",
        "content_hash",
        "project_no",
        "title",
        "investor_code",
        "investor_name",
        "total_investment",
        "currency",
        "decision_no",
        "decision_date",
        "public_date",
        "public_date_raw",
        "prov_code",
        "district_code",
        "status_raw",
        "dq_status",
    )

    # 5. Child table: project_location
    with_parsed_loc = valid_deduped.withColumn(
        "_ploc_arr",
        F.when(
            F.col("_plocation_raw").startswith("["),
            F.from_json(F.col("_plocation_raw"), PLOCATION_ITEM_SCHEMA, _JSON_OPTS),
        ),
    )

    array_locs = (
        with_parsed_loc.filter(F.col("_ploc_arr").isNotNull() & (F.size(F.col("_ploc_arr")) > 0))
        .select(
            "revision_id",
            "source_date",
            F.posexplode(F.col("_ploc_arr")).alias("ordinal", "_loc"),
        )
        .select(
            "revision_id",
            "source_date",
            F.col("ordinal").cast("int").alias("ordinal"),
            clean_pipe_code(F.col("_loc.provCode")).alias("province_code"),
            clean_text(F.col("_loc.provName")).alias("province_name"),
            clean_pipe_code(F.col("_loc.districtCode")).alias("district_code"),
            clean_text(F.col("_loc.districtName")).alias("district_name"),
            clean_pipe_code(F.col("_loc.wardCode")).alias("ward_code"),
            clean_text(F.col("_loc.wardName")).alias("ward_name"),
        )
        .withColumn(
            "location_name",
            F.when(
                F.size(F.array_compact(F.array("ward_name", "district_name", "province_name"))) > 0,
                F.concat_ws(", ", F.array_compact(F.array("ward_name", "district_name", "province_name"))),
            ),
        )
    )

    fallback_locs = (
        with_parsed_loc.filter(F.col("_ploc_arr").isNull() | (F.size(F.col("_ploc_arr")) == 0))
        .select(
            "revision_id",
            "source_date",
            F.lit(0).cast("int").alias("ordinal"),
            F.col("prov_code").alias("province_code"),
            F.col("_prov_name").alias("province_name"),
            F.col("district_code").alias("district_code"),
            F.col("_district_name").alias("district_name"),
            F.lit(None).cast("string").alias("ward_code"),
            F.lit(None).cast("string").alias("ward_name"),
            F.col("_plocation_raw").alias("location_name"),
        )
    )

    location_df = (
        array_locs.unionByName(fallback_locs)
        .filter(
            F.col("province_code").isNotNull()
            | F.col("district_code").isNotNull()
            | F.col("location_name").isNotNull()
            | F.col("province_name").isNotNull()
            | F.col("district_name").isNotNull()
        )
        .withColumn(
            "location_id",
            sha256_key(
                F.col("revision_id"),
                F.col("ordinal"),
                F.col("province_code"),
                F.col("district_code"),
                F.col("ward_code"),
                F.col("location_name"),
            ),
        )
        .select(
            "location_id",
            "revision_id",
            "source_date",
            "ordinal",
            "province_code",
            "province_name",
            "district_code",
            "district_name",
            "ward_code",
            "ward_name",
            "location_name",
        )
    )

    # 6. Accurate per-day grouped quality metrics
    raw_day_counts = staged.groupBy("source_date").agg(
        F.count("*").alias("raw_input_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("_quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    warn_day_counts = annotated_valid.groupBy("source_date").agg(
        F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows"),
    )
    issue_day_counts = dq_issues_df.groupBy("source_date").agg(
        F.sum(F.when(F.col("issue_code") == "invalid_money", 1).otherwise(0)).alias("invalid_money_rows"),
        F.sum(F.when(F.col("issue_code") == "unknown_currency", 1).otherwise(0)).alias("unknown_currency_rows"),
        F.sum(F.when(F.col("issue_code") == "invalid_date_order", 1).otherwise(0)).alias("invalid_date_order_rows"),
    )
    per_day_metrics = (
        raw_day_counts.join(warn_day_counts, "source_date", "left")
        .join(issue_day_counts, "source_date", "left")
        .na.fill(0)
    )

    return ProjectTransformOutput(
        tables={
            "project_revision": revision_df,
            "project_location": location_df,
            SHARED_DQ_TABLE: dq_issues_df,
            SHARED_QUARANTINE_TABLE: quarantine_df,
        },
        per_day_metrics=per_day_metrics,
    )
