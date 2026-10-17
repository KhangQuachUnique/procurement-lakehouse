"""Centralized Silver Lakehouse Schema definitions using SQLAlchemy 2.0.

Single Source of Truth for all Silver tables across the procurement lifecycle.
Provides automated generators for DuckDB DDL SQL and PyArrow Schemas.
"""

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.sql.type_api import TypeEngine

metadata = sa.MetaData(schema="silver")

# ---------------------------------------------------------------------------
# TIER 1: TRACEABILITY & GOVERNANCE
# ---------------------------------------------------------------------------

observation_table = sa.Table(
    "observation",
    metadata,
    sa.Column("observation_id", sa.String, primary_key=True),
    sa.Column("resource", sa.String, nullable=False),
    sa.Column("source_date", sa.Date, nullable=False),
    sa.Column("run_id", sa.String, nullable=False),
    sa.Column("table_name", sa.String, nullable=False),
    sa.Column("file_key", sa.String, nullable=False),
    sa.Column("file_sha256", sa.String(64), nullable=False),
    sa.Column("row_ordinal", sa.BigInteger, nullable=False),
    sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("source_id", sa.String, nullable=True),
    sa.Column("source_version", sa.String, nullable=True),
    sa.Column("content_hash", sa.String(64), nullable=False),
    sa.Column("payload_json", sa.Text, nullable=False),
    comment="Physical raw observation record from Bronze Parquet with full lineage",
)

quality_issue_table = sa.Table(
    "quality_issue",
    metadata,
    sa.Column("issue_id", sa.String, primary_key=True),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("code", sa.String, nullable=False),
    sa.Column("path", sa.String, nullable=False),
    sa.Column("severity", sa.String, nullable=False),
    comment="Data quality anomalies and rule violations",
)

quarantine_table = sa.Table(
    "quarantine",
    metadata,
    sa.Column("observation_id", sa.String, primary_key=True),
    sa.Column("reason", sa.String, nullable=False),
    comment="Quarantined occurrences failing basic identity/format constraints",
)

# ---------------------------------------------------------------------------
# TIER 2: 5 CORE ENTITY REVISIONS
# ---------------------------------------------------------------------------

project_revision_table = sa.Table(
    "project_revision",
    metadata,
    sa.Column("revision_id", sa.String, primary_key=True),
    sa.Column("entity_id", sa.String, nullable=False),
    sa.Column("entity_type", sa.String, nullable=False),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("is_current", sa.Boolean, nullable=True),
    sa.Column("project_no", sa.String, nullable=True),
    sa.Column("business_number", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("investor_code", sa.String, nullable=True),
    sa.Column("investor_name", sa.String, nullable=True),
    sa.Column("buyer_id", sa.String, nullable=True),
    sa.Column("buyer_name", sa.String, nullable=True),
    sa.Column("total_investment", sa.Numeric(38, 6), nullable=True),
    sa.Column("amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("decision_no", sa.String, nullable=True),
    sa.Column("decision_date", sa.Date, nullable=True),
    sa.Column("prov_code", sa.String, nullable=True),
    sa.Column("district_code", sa.String, nullable=True),
    sa.Column("public_date", sa.Date, nullable=True),
    sa.Column("public_date_raw", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("root_json", sa.Text, nullable=False),
    comment="Investment project revision",
)

plan_revision_table = sa.Table(
    "plan_revision",
    metadata,
    sa.Column("revision_id", sa.String, primary_key=True),
    sa.Column("entity_id", sa.String, nullable=False),
    sa.Column("entity_type", sa.String, nullable=False),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("is_current", sa.Boolean, nullable=True),
    sa.Column("plan_no", sa.String, nullable=True),
    sa.Column("plan_version", sa.String, nullable=True),
    sa.Column("business_number", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("invest_target", sa.String, nullable=True),
    sa.Column("invest_scale", sa.String, nullable=True),
    sa.Column("investor_name", sa.String, nullable=True),
    sa.Column("buyer_id", sa.String, nullable=True),
    sa.Column("buyer_name", sa.String, nullable=True),
    sa.Column("project_id", sa.String, nullable=True),
    sa.Column("project_no", sa.String, nullable=True),
    sa.Column("total_investment", sa.Numeric(38, 6), nullable=True),
    sa.Column("amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("public_date", sa.Date, nullable=True),
    sa.Column("public_date_raw", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("root_json", sa.Text, nullable=False),
    comment="Procurement plan (KHLCNT) revision",
)

package_revision_table = sa.Table(
    "package_revision",
    metadata,
    sa.Column("revision_id", sa.String, primary_key=True),
    sa.Column("entity_id", sa.String, nullable=False),
    sa.Column("entity_type", sa.String, nullable=False),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("is_current", sa.Boolean, nullable=True),
    sa.Column("package_no", sa.String, nullable=True),
    sa.Column("business_number", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("plan_no", sa.String, nullable=True),
    sa.Column("plan_id", sa.String, nullable=True),
    sa.Column("bid_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("estimate_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("bid_field", sa.String, nullable=True),
    sa.Column("bid_form", sa.String, nullable=True),
    sa.Column("bid_mode", sa.String, nullable=True),
    sa.Column("contract_type", sa.String, nullable=True),
    sa.Column("execution_period", sa.String, nullable=True),
    sa.Column("is_domestic", sa.Boolean, nullable=True),
    sa.Column("is_internet", sa.Boolean, nullable=True),
    sa.Column("public_date", sa.Date, nullable=True),
    sa.Column("public_date_raw", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("root_json", sa.Text, nullable=False),
    comment="Package in procurement plan revision",
)

notice_revision_table = sa.Table(
    "notice_revision",
    metadata,
    sa.Column("revision_id", sa.String, primary_key=True),
    sa.Column("entity_id", sa.String, nullable=False),
    sa.Column("entity_type", sa.String, nullable=False),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("is_current", sa.Boolean, nullable=True),
    sa.Column("notify_no", sa.String, nullable=True),
    sa.Column("notify_version", sa.String, nullable=True),
    sa.Column("business_number", sa.String, nullable=True),
    sa.Column("package_no", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("procuring_entity_code", sa.String, nullable=True),
    sa.Column("procuring_entity_name", sa.String, nullable=True),
    sa.Column("buyer_id", sa.String, nullable=True),
    sa.Column("buyer_name", sa.String, nullable=True),
    sa.Column("investor_name", sa.String, nullable=True),
    sa.Column("bid_open_date", sa.DateTime(timezone=True), nullable=True),
    sa.Column("bid_close_date", sa.DateTime(timezone=True), nullable=True),
    sa.Column("bid_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("bid_field", sa.String, nullable=True),
    sa.Column("bid_form", sa.String, nullable=True),
    sa.Column("bid_mode", sa.String, nullable=True),
    sa.Column("notification_type", sa.String, nullable=True),
    sa.Column("public_date", sa.Date, nullable=True),
    sa.Column("public_date_raw", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("root_json", sa.Text, nullable=False),
    # Notice-specific extra columns (notice-2 contract)
    sa.Column("source_table", sa.String, nullable=True),
    sa.Column("source_family", sa.String, nullable=True),
    sa.Column("source_root_path", sa.String, nullable=True),
    sa.Column("source_document_id", sa.String, nullable=True),
    sa.Column("package_id", sa.String, nullable=True),
    sa.Column("plan_id", sa.String, nullable=True),
    sa.Column("plan_no", sa.String, nullable=True),
    sa.Column("plan_version", sa.String, nullable=True),
    sa.Column("plan_name", sa.String, nullable=True),
    sa.Column("plan_type_raw", sa.String, nullable=True),
    sa.Column("project_name", sa.String, nullable=True),
    sa.Column("investor_code", sa.String, nullable=True),
    sa.Column("capital_detail", sa.String, nullable=True),
    sa.Column("process_apply_raw", sa.String, nullable=True),
    sa.Column("reoffer_no", sa.String, nullable=True),
    sa.Column("reoffer_version", sa.String, nullable=True),
    sa.Column("reoffer_status_raw", sa.String, nullable=True),
    sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("original_published_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("original_public_date_raw", sa.String, nullable=True),
    sa.Column("bid_open_date_raw", sa.String, nullable=True),
    sa.Column("bid_close_date_raw", sa.String, nullable=True),
    sa.Column("reoffer_open_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("reoffer_close_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("plan_decision_date", sa.Date, nullable=True),
    sa.Column("estimate_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("estimate_currency", sa.String, nullable=True),
    sa.Column("guarantee_amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("guarantee_currency", sa.String, nullable=True),
    sa.Column("guarantee_form", sa.String, nullable=True),
    sa.Column("initial_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("price_step", sa.Numeric(38, 6), nullable=True),
    sa.Column("reoffer_currency", sa.String, nullable=True),
    sa.Column("contract_type", sa.String, nullable=True),
    sa.Column("contract_duration_raw", sa.String, nullable=True),
    sa.Column("contract_duration_unit", sa.String, nullable=True),
    sa.Column("bid_validity_raw", sa.String, nullable=True),
    sa.Column("bid_validity_unit", sa.String, nullable=True),
    sa.Column("is_internet", sa.Boolean, nullable=True),
    sa.Column("is_domestic", sa.Boolean, nullable=True),
    sa.Column("is_multi_lot", sa.Boolean, nullable=True),
    sa.Column("is_prequalification", sa.Boolean, nullable=True),
    sa.Column("is_medicine_raw", sa.String, nullable=True),
    sa.Column("medical_type_raw", sa.String, nullable=True),
    sa.Column("fee_type_raw", sa.String, nullable=True),
    sa.Column("fee_amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("issue_location", sa.String, nullable=True),
    sa.Column("receive_location", sa.String, nullable=True),
    sa.Column("open_location", sa.String, nullable=True),
    sa.Column("execution_location", sa.String, nullable=True),
    sa.Column("shortlist_no", sa.String, nullable=True),
    sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("cancel_reason", sa.String, nullable=True),
    sa.Column("cancel_type_raw", sa.String, nullable=True),
    sa.Column("cancel_decision_no", sa.String, nullable=True),
    sa.Column("cancel_decision_date", sa.Date, nullable=True),
    sa.Column("cancel_decision_agency", sa.String, nullable=True),
    sa.Column("cancel_file_id", sa.String, nullable=True),
    sa.Column("cancel_file_name", sa.String, nullable=True),
    sa.Column("report_file_id", sa.String, nullable=True),
    sa.Column("appraisal_file_id", sa.String, nullable=True),
    sa.Column("field_sources_json", sa.String, nullable=True),
    sa.Column("timestamp_policy", sa.String, nullable=True),
    comment="Tender invitation notice (TBMT) revision",
)

result_revision_table = sa.Table(
    "result_revision",
    metadata,
    sa.Column("revision_id", sa.String, primary_key=True),
    sa.Column("entity_id", sa.String, nullable=False),
    sa.Column("entity_type", sa.String, nullable=False),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("is_current", sa.Boolean, nullable=True),
    sa.Column("result_id", sa.String, nullable=True),
    sa.Column("result_version", sa.String, nullable=True),
    sa.Column("notify_no", sa.String, nullable=True),
    sa.Column("notify_version", sa.String, nullable=True),
    sa.Column("business_number", sa.String, nullable=True),
    sa.Column("package_no", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("decision_no", sa.String, nullable=True),
    sa.Column("decision_date", sa.Date, nullable=True),
    sa.Column("total_winning_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("public_date", sa.Date, nullable=True),
    sa.Column("public_date_raw", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("root_json", sa.Text, nullable=False),
    comment="Contractor selection result revision",
)

opening_revision_table = sa.Table(
    "opening_revision",
    metadata,
    sa.Column("revision_id", sa.String, primary_key=True),
    sa.Column("entity_id", sa.String, nullable=False),
    sa.Column("entity_type", sa.String, nullable=False),
    sa.Column("observation_id", sa.String, nullable=True),
    sa.Column("is_current", sa.Boolean, nullable=True),
    sa.Column("notify_no", sa.String, nullable=True),
    sa.Column("notify_version", sa.String, nullable=True),
    sa.Column("business_number", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("bid_mode", sa.String, nullable=True),
    sa.Column("actual_open_date", sa.DateTime(timezone=True), nullable=True),
    sa.Column("total_bidders", sa.BigInteger, nullable=True),
    sa.Column("public_date", sa.Date, nullable=True),
    sa.Column("public_date_raw", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("root_json", sa.Text, nullable=False),
    comment="Bid opening revision",
)

# Legacy compatibility tables
lot_table = sa.Table(
    "lot",
    metadata,
    sa.Column("lot_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("lot_no", sa.String, nullable=True),
    sa.Column("lot_name", sa.String, nullable=True),
    sa.Column("lot_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("estimate_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("winning_code", sa.String, nullable=True),
    sa.Column("status_raw", sa.String, nullable=True),
    sa.Column("raw_json", sa.Text, nullable=False),
    comment="Legacy generic lot table",
)

bid_participation_table = sa.Table(
    "bid_participation",
    metadata,
    sa.Column("participation_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("lot_no", sa.String, nullable=True),
    sa.Column("contractor_code", sa.String, nullable=True),
    sa.Column("contractor_name", sa.String, nullable=True),
    sa.Column("tax_code", sa.String, nullable=True),
    sa.Column("is_consortium", sa.Boolean, nullable=True),
    sa.Column("consortium_name", sa.String, nullable=True),
    sa.Column("bid_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("winning_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("is_winner", sa.Boolean, nullable=True),
    sa.Column("evaluation_rank", sa.BigInteger, nullable=True),
    sa.Column("raw_json", sa.Text, nullable=False),
    comment="Legacy generic bid participation table",
)

# ---------------------------------------------------------------------------
# TIER 3: TYPED SUB-ENTITIES (CHILD PROJECTIONS)
# ---------------------------------------------------------------------------

notice_lot_table = sa.Table(
    "notice_lot",
    metadata,
    sa.Column("child_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("source_id", sa.String, nullable=True),
    sa.Column("source_path", sa.String, nullable=True),
    sa.Column("ordinal", sa.BigInteger, nullable=False),
    sa.Column("raw_json", sa.Text, nullable=False),
    sa.Column("lot_no", sa.String, nullable=True),
    sa.Column("lot_name", sa.String, nullable=True),
    sa.Column("lot_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("estimate_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("guarantee_amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("contract_duration_raw", sa.String, nullable=True),
    sa.Column("contract_duration_unit", sa.String, nullable=True),
    sa.Column("notify_id", sa.String, nullable=True),
    sa.Column("notify_no", sa.String, nullable=True),
    comment="Tender invitation notice lot item projection",
)

notice_location_table = sa.Table(
    "notice_location",
    metadata,
    sa.Column("child_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("source_id", sa.String, nullable=True),
    sa.Column("source_path", sa.String, nullable=True),
    sa.Column("ordinal", sa.BigInteger, nullable=False),
    sa.Column("raw_json", sa.Text, nullable=False),
    sa.Column("province_code", sa.String, nullable=True),
    sa.Column("province_name", sa.String, nullable=True),
    sa.Column("district_code", sa.String, nullable=True),
    sa.Column("district_name", sa.String, nullable=True),
    sa.Column("ward_code", sa.String, nullable=True),
    sa.Column("ward_name", sa.String, nullable=True),
    comment="Tender invitation notice geographic location projection",
)

notice_reoffer_submission_table = sa.Table(
    "notice_reoffer_submission",
    metadata,
    sa.Column("child_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("source_id", sa.String, nullable=True),
    sa.Column("source_path", sa.String, nullable=True),
    sa.Column("ordinal", sa.BigInteger, nullable=False),
    sa.Column("raw_json", sa.Text, nullable=False),
    sa.Column("source_list", sa.String, nullable=False),
    sa.Column("submission_id", sa.String, nullable=True),
    sa.Column("reoffer_id", sa.String, nullable=True),
    sa.Column("lot_no", sa.String, nullable=True),
    sa.Column("contractor_code", sa.String, nullable=True),
    sa.Column("contractor_name", sa.String, nullable=True),
    sa.Column("venture_code", sa.String, nullable=True),
    sa.Column("venture_name", sa.String, nullable=True),
    sa.Column("offered_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("offered_at_raw", sa.String, nullable=True),
    sa.Column("times_raw", sa.String, nullable=True),
    sa.Column("price", sa.Numeric(38, 6), nullable=True),
    sa.Column("final_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("is_newest", sa.Boolean, nullable=True),
    sa.Column("form_code", sa.String, nullable=True),
    sa.Column("form_value", sa.String, nullable=True),
    comment="Online reoffer bid submission attempt projection",
)

# New Domain Extensions
project_location_table = sa.Table(
    "project_location",
    metadata,
    sa.Column("location_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("province_code", sa.String, nullable=True),
    sa.Column("province_name", sa.String, nullable=True),
    sa.Column("district_code", sa.String, nullable=True),
    sa.Column("district_name", sa.String, nullable=True),
    sa.Column("ward_code", sa.String, nullable=True),
    sa.Column("ward_name", sa.String, nullable=True),
    sa.Column("location_name", sa.String, nullable=True),
    comment="Investment project location breakdown",
)

package_lot_table = sa.Table(
    "package_lot",
    metadata,
    sa.Column("lot_id", sa.String, primary_key=True),
    sa.Column("package_revision_id", sa.String, nullable=False),
    sa.Column("ordinal", sa.BigInteger, nullable=False),
    sa.Column("lot_no", sa.String, nullable=False),
    sa.Column("lot_name", sa.String, nullable=True),
    sa.Column("budget_amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    comment="Package lot specification in plan",
)

notice_amendment_table = sa.Table(
    "notice_amendment",
    metadata,
    sa.Column("amendment_id", sa.String, primary_key=True),
    sa.Column("revision_id", sa.String, nullable=False),
    sa.Column("amendment_type", sa.String, nullable=False),
    sa.Column("old_close_date", sa.DateTime(timezone=True), nullable=True),
    sa.Column("new_close_date", sa.DateTime(timezone=True), nullable=True),
    sa.Column("reason", sa.String, nullable=True),
    sa.Column("raw_json", sa.Text, nullable=False),
    comment="Tender invitation deadline postponement and amendment history",
)

opening_submission_table = sa.Table(
    "opening_submission",
    metadata,
    sa.Column("submission_id", sa.String, primary_key=True),
    sa.Column("opening_revision_id", sa.String, nullable=False),
    sa.Column("phase", sa.String, nullable=False),
    sa.Column("lot_no", sa.String, nullable=True),
    sa.Column("contractor_code", sa.String, nullable=False),
    sa.Column("contractor_name", sa.String, nullable=False),
    sa.Column("is_consortium", sa.Boolean, nullable=False),
    sa.Column("venture_code", sa.String, nullable=True),
    sa.Column("venture_name", sa.String, nullable=True),
    sa.Column("bid_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("discount_rate", sa.Numeric(10, 4), nullable=True),
    sa.Column("final_bid_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("tech_result", sa.String, nullable=True),
    sa.Column("submission_time", sa.DateTime(timezone=True), nullable=True),
    comment="Bid submission at opening ceremony",
)

award_lot_table = sa.Table(
    "award_lot",
    metadata,
    sa.Column("award_lot_id", sa.String, primary_key=True),
    sa.Column("result_revision_id", sa.String, nullable=False),
    sa.Column("lot_no", sa.String, nullable=False),
    sa.Column("lot_name", sa.String, nullable=True),
    sa.Column("lot_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("winning_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("saving_amount", sa.Numeric(38, 6), nullable=True),
    sa.Column("saving_rate", sa.Numeric(10, 4), nullable=True),
    sa.Column("currency", sa.String, nullable=True),
    sa.Column("winning_code", sa.String, nullable=True),
    sa.Column("status", sa.String, nullable=True),
    comment="Contract award decision per lot",
)

award_contractor_table = sa.Table(
    "award_contractor",
    metadata,
    sa.Column("award_contractor_id", sa.String, primary_key=True),
    sa.Column("award_lot_id", sa.String, nullable=False),
    sa.Column("result_revision_id", sa.String, nullable=False),
    sa.Column("contractor_code", sa.String, nullable=False),
    sa.Column("contractor_name", sa.String, nullable=False),
    sa.Column("tax_code", sa.String, nullable=True),
    sa.Column("country", sa.String, nullable=True),
    sa.Column("is_consortium", sa.Boolean, nullable=False),
    sa.Column("venture_code", sa.String, nullable=True),
    sa.Column("venture_name", sa.String, nullable=True),
    sa.Column("role", sa.String, nullable=True),
    sa.Column("awarded_price", sa.Numeric(38, 6), nullable=True),
    sa.Column("evaluation_rank", sa.Integer, nullable=True),
    comment="Awarded contractor and consortium members",
)

# ---------------------------------------------------------------------------
# AUTOMATED SCHEMA CONVERTERS (DUCKDB DDL & PYARROW SCHEMA)
# ---------------------------------------------------------------------------


def _column_to_duckdb_type(col_type: TypeEngine) -> str:
    """Map SQLAlchemy column type to DuckDB SQL type string."""
    if isinstance(col_type, sa.Numeric):
        precision = col_type.precision or 38
        scale = col_type.scale or 6
        return f"DECIMAL({precision},{scale})"
    if isinstance(col_type, sa.DateTime):
        return "TIMESTAMPTZ"
    if isinstance(col_type, sa.Date):
        return "DATE"
    if isinstance(col_type, (sa.BigInteger, sa.Integer)):
        # To match existing legacy DuckDB DDL expectation: BIGINT for integer columns
        return "BIGINT"
    if isinstance(col_type, sa.Boolean):
        return "BOOLEAN"
    return "VARCHAR"


def to_duckdb_ddl(table: sa.Table) -> str:
    """Generate DuckDB SQL column definition string for a SQLAlchemy Table."""
    return ", ".join(f"{col.name} {_column_to_duckdb_type(col.type)}" for col in table.columns)


def _column_to_pyarrow_type(col_type: TypeEngine) -> pa.DataType:
    """Map SQLAlchemy column type to PyArrow DataType."""
    if isinstance(col_type, sa.Numeric):
        precision = col_type.precision or 38
        scale = col_type.scale or 6
        return pa.decimal128(precision, scale)
    if isinstance(col_type, sa.DateTime):
        return pa.timestamp("us", tz="UTC")
    if isinstance(col_type, sa.Date):
        return pa.date32()
    if isinstance(col_type, sa.BigInteger):
        return pa.int64()
    if isinstance(col_type, sa.Integer):
        return pa.int64()
    if isinstance(col_type, sa.Boolean):
        return pa.bool_()
    return pa.string()


def to_pyarrow_schema(table: sa.Table) -> pa.Schema:
    """Generate PyArrow Schema for a SQLAlchemy Table."""
    fields = [
        pa.field(col.name, _column_to_pyarrow_type(col.type), nullable=col.nullable)
        for col in table.columns
    ]
    return pa.schema(fields)


# ---------------------------------------------------------------------------
# COMPATIBILITY EXPORTS (BACKWARD COMPATIBLE WITH SCHEMAS AND CHILD_SCHEMAS)
# ---------------------------------------------------------------------------


def get_silver_schemas() -> dict[str, str]:
    """Generate the full dictionary of DuckDB SQL schemas for all Silver tables."""
    schemas: dict[str, str] = {}
    for table_name, table in metadata.tables.items():
        # Strip schema prefix if any (e.g. silver.observation -> observation)
        clean_name = table_name.split(".")[-1]
        schemas[clean_name] = to_duckdb_ddl(table)
    return schemas


def get_child_schemas() -> dict[str, str]:
    """Generate child table schemas compatible with `CHILD_SCHEMAS`."""
    child_table_names = {
        "notice_lot",
        "notice_location",
        "notice_reoffer_submission",
        "project_location",
        "package_lot",
        "notice_amendment",
        "opening_submission",
        "award_lot",
        "award_contractor",
    }
    schemas = get_silver_schemas()
    return {k: schemas[k] for k in child_table_names if k in schemas}
