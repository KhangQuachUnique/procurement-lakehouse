"""Regression tests for the approved 20-table Silver schema and bid opening contract."""

import sqlalchemy as sa

from procurement.silver.domain.schema import (
    get_child_schemas,
    get_partition_spec,
    get_primary_key,
    get_silver_schemas,
    metadata,
    to_iceberg_ddl,
)

TABLES = {
    "observation",
    "quality_issue",
    "quarantine",
    "contract_revision",
    "contract_performance_revision",
    "project_revision",
    "project_location",
    "plan_revision",
    "package_revision",
    "package_lot",
    "package_location",
    "notice_revision",
    "notice_lot",
    "notice_location",
    "notice_reoffer_submission",
    "notice_amendment",
    "lot",
    "bid_participation",
    "opening_revision",
    "opening_phase",
    "opening_submission",
    "opening_lot_submission",
    "result_revision",
    "award_lot",
    "award_contractor",
}
OPENING_COLUMNS = {
    "opening_revision": (
        "revision_id",
        "entity_id",
        "entity_type",
        "observation_id",
        "is_current",
        "notify_no",
        "notify_version",
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
        "cancel_date",
        "cancel_decision_date",
        "cancel_decision_no",
        "cancel_reason",
        "collection_status",
        "actual_open_date",
        "total_bidders",
        "public_date",
        "public_date_raw",
        "status_raw",
    ),
    "opening_phase": (
        "phase_id",
        "revision_id",
        "phase",
        "source_path",
        "opened_at",
        "reported_contractor_count",
        "submission_shape",
        "phase_status_raw",
    ),
    "opening_submission": (
        "submission_id",
        "revision_id",
        "opening_revision_id",
        "source_submission_id",
        "source_path",
        "phase",
        "lot_no",
        "contractor_code",
        "contractor_name",
        "is_consortium",
        "venture_code",
        "venture_name",
        "bid_price",
        "currency",
        "discount_rate",
        "final_bid_price",
        "tech_result",
        "tech_score",
        "submission_time",
    ),
    "opening_lot_submission": (
        "lot_submission_id",
        "revision_id",
        "submission_id",
        "phase",
        "source_path",
        "source_lot_id",
        "bid_no",
        "lot_no",
        "lot_name",
        "contractor_code",
        "contractor_name",
        "lot_price",
        "lot_final_price",
        "discount_rate",
        "currency",
        "passed_yn",
        "tech_score",
        "evaluation_price",
        "evaluation_rank",
    ),
}
PRIMARY_KEYS = {
    "opening_revision": "revision_id",
    "opening_phase": "phase_id",
    "opening_submission": "submission_id",
    "opening_lot_submission": "lot_submission_id",
}


def test_exact_table_inventory():
    assert set(get_silver_schemas()) == TABLES
    assert {k.removeprefix("silver.") for k in metadata.tables} == TABLES


def test_exact_bid_opening_column_contract_and_primary_keys():
    for name, columns in OPENING_COLUMNS.items():
        table = metadata.tables[f"silver.{name}"]
        assert tuple(table.columns.keys()) == columns
        assert get_primary_key(name) == PRIMARY_KEYS[name]
        assert get_primary_key(f"silver.{name}") == PRIMARY_KEYS[name]
        assert get_primary_key(name) == next(iter(table.primary_key.columns)).name


def test_bid_opening_important_types_and_nullable():
    revision = metadata.tables["silver.opening_revision"]
    assert isinstance(revision.c.financial_open_date.type, sa.DateTime)
    assert revision.c.financial_open_date.type.timezone is True
    assert isinstance(revision.c.cancel_decision_date.type, sa.Date)
    assert not isinstance(revision.c.cancel_decision_date.type, sa.DateTime)
    assert revision.c.collection_status.nullable

    phase = metadata.tables["silver.opening_phase"]
    assert phase.c.submission_shape.nullable is False

    submission = metadata.tables["silver.opening_submission"]
    assert submission.c.contractor_name.nullable
    assert submission.c.contractor_code.nullable is False
    bid_price_type = submission.c.bid_price.type

    assert isinstance(bid_price_type, sa.Numeric)
    assert (bid_price_type.precision, bid_price_type.scale) == (38, 6)
    discount_type = submission.c.discount_rate.type

    assert isinstance(discount_type, sa.Numeric)
    assert (discount_type.precision, discount_type.scale) == (10, 4)

    lot = metadata.tables["silver.opening_lot_submission"]
    assert lot.c.submission_id.nullable
    tech_score_type = lot.c.tech_score.type

    assert isinstance(tech_score_type, sa.Numeric)
    assert (tech_score_type.precision, tech_score_type.scale) == (18, 6)


def test_child_registration_and_partition_contract():
    child = get_child_schemas()
    assert {
        "opening_phase",
        "opening_submission",
        "opening_lot_submission",
    } <= child.keys()
    assert get_partition_spec("opening_revision") == "public_date"
    for name in ("opening_phase", "opening_submission", "opening_lot_submission"):
        assert get_partition_spec(name) is None
    for name in OPENING_COLUMNS:
        assert f"lake.silver.{name}" in to_iceberg_ddl(name)
