"""Tests for KHLCNT Plan (19-column Silver ERD including entity_type and Project references)."""

import json
from datetime import UTC, datetime

import pytest

from procurement.silver.domain.parsers import clean_project_reference
from procurement.silver.domain.protocols import (
    RawRecordEnvelope,
    SilverTransformerProtocol,
)
from procurement.silver.domain.registry import get_transformer
from procurement.silver.domain.transformers.plan import (
    PlanDetailTransformer,
)

PLAN_FIELDS = {
    "revision_id",
    "entity_id",
    "entity_type",
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
    "root_json",
}


def make_envelope(
    payload: dict, source_id: str = "plan-source-id"
) -> RawRecordEnvelope:
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name="khlcnt_plan_detail",
        resource="khlcnt",
        source_date="2024-01-01",
        run_id="run-test",
        row_ordinal=0,
        file_key="bronze/muasamcong/khlcnt_plan_detail/test.parquet",
        file_sha256="sha-test",
        source_id=source_id,
        source_version="00",
        content_hash="hash-test",
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC),
        payload=payload,
    )


def make_plan(**overrides) -> dict:
    root = {
        "id": "8e5ccebb-bad7-493d-b9b5-1a400ddf65db",
        "planVersion": "00",
        "status": "01",
        "name": "Cung cấp quà tặng KHCN nhân dịp tết dương lịch năm 2024",
        "investorName": "BIDV ĐỒNG NAI",
        "investTotal": 462150000,
        "investTotalUnit": "VND",
        "decisionNo": "680/QĐ-BIDV.ĐNAI",
        "decisionDate": "2023-12-04T23:59:59",
        "decisionAgency": "BIDV ĐỒNG NAI",
        "publicDate": "2024-01-01T22:34:28.283",
        "bidPack": 1,
        "planNo": "PL2400000054",
        "planType": "TX",
        "location": None,
        "pid": None,
        "pno": None,
    }
    root.update(overrides)
    return {
        "bidPoBidpPlanProjectDetailView": root,
        "bidpPlanDetailToProjectList": [],
        "project": None,
    }


def transform(**overrides):
    return PlanDetailTransformer().transform(make_envelope(make_plan(**overrides)))


def test_conforms_and_is_registered():
    t = PlanDetailTransformer()
    assert isinstance(t, SilverTransformerProtocol)
    assert (t.table_name, t.entity_type, t.id_scheme) == (
        "khlcnt_plan_detail",
        "plan",
        "uuid",
    )
    assert get_transformer("khlcnt_plan_detail") is not None


def test_all_columns_from_real_bronze_shape():
    out = transform()
    assert out.quarantine is None
    assert out.entity.source_identity == "8e5ccebb-bad7-493d-b9b5-1a400ddf65db"
    assert out.revision.source_version == "00"
    a = out.typed_attributes
    assert set(a) == PLAN_FIELDS
    assert a["plan_no"] == "PL2400000054"
    assert a["title"].startswith("Cung cấp quà tặng")
    assert a["total_investment"] == "462150000.000000"
    assert a["currency"] == "VND"
    assert a["decision_date"] == "2023-12-04"
    assert a["public_date"] == "2024-01-01"
    assert a["bid_pack"] == 1
    assert a["status_raw"] == "01"
    assert a["project_id"] is None
    assert out.references == []
    assert out.children == []
    assert out.issues == []
    assert json.loads(a["root_json"]) == make_plan()["bidPoBidpPlanProjectDetailView"]


def test_pid_and_pno_link_project_without_materializing_fk():
    out = transform(
        pid="98377056-2452-465a-81e3-58e65f3042cd",
        pno="PR2300020214",
        planType="KHAC",
    )
    assert out.typed_attributes["project_id"] is None
    assert out.typed_attributes["project_no"] == "PR2300020214"
    assert len(out.references) == 1
    ref = out.references[0]
    assert ref.target_type == "project"
    assert ref.id_scheme == "uuid"
    assert ref.target_identity == "98377056-2452-465a-81e3-58e65f3042cd"
    assert ref.field_name == "pid"
    assert ref.from_revision_id == out.revision.revision_id


@pytest.mark.parametrize("plan_type", ["DTPT", "DTMS", "TX", "KHAC"])
def test_reference_does_not_depend_on_plan_type(plan_type):
    out = transform(planType=plan_type, pid="project-123", pno="PR-123")
    assert len(out.references) == 1
    assert out.references[0].target_identity == "project-123"


def test_pid_present_pno_absent_must_still_create_reference():
    out = transform(pid="34496da3-7ed8-416f-9e35-55764eacf97d", pno=None)
    assert out.typed_attributes["project_no"] is None
    assert len(out.references) == 1
    assert out.references[0].target_identity == "34496da3-7ed8-416f-9e35-55764eacf97d"
    assert out.references[0].field_name == "pid"


def test_numeric_legacy_pid_keeps_leading_zeroes():
    out = transform(pid="03898378175944085742", pno=None)
    assert len(out.references) == 1
    assert out.references[0].target_identity == "03898378175944085742"
    assert out.references[0].id_scheme == "uuid"


def test_pno_number_fallback_when_pid_missing():
    out = transform(pid=None, pno="PR2300020214")
    assert out.typed_attributes["project_no"] == "PR2300020214"
    assert len(out.references) == 1
    assert out.references[0].id_scheme == "number"
    assert out.references[0].target_identity == "PR2300020214"
    assert out.references[0].field_name == "pno"


@pytest.mark.parametrize(
    "placeholder", ["N/A", "n/a", " NA ", "null", "NONE", "  ", None]
)
def test_placeholder_pno_is_not_project_reference(placeholder):
    out = transform(pid=None, pno=placeholder)
    assert out.quarantine is None
    assert out.typed_attributes["project_no"] is None
    assert out.references == []


def test_invalid_pid_placeholder_does_not_block_valid_pno_fallback():
    out = transform(pid="N/A", pno="PR2400006559")
    assert len(out.references) == 1
    assert out.references[0].id_scheme == "number"
    assert out.references[0].target_identity == "PR2400006559"


def test_clean_project_reference_never_converts_numeric_id():
    assert clean_project_reference(" 000123 ") == "000123"
    assert clean_project_reference(" n/A ") is None


def test_missing_root_quarantines():
    out = PlanDetailTransformer().transform(make_envelope({"project": {"id": "other"}}))
    assert out.quarantine.reason == "missing_root"
    assert out.entity is None
    assert out.revision is None
    assert any(i.code == "missing_root" for i in out.issues)


def test_missing_identity_quarantines():
    out = transform(id=None)
    assert out.quarantine.reason == "missing_identity"
    assert out.entity is None
    assert out.revision is None


def test_invalid_optional_fields_produce_warnings_not_quarantine():
    out = transform(
        investTotal="invalid", decisionDate="invalid", publicDate="bad", bidPack="1.2"
    )
    assert out.quarantine is None
    assert out.typed_attributes["total_investment"] is None
    assert out.typed_attributes["decision_date"] is None
    assert out.typed_attributes["public_date"] is None
    assert out.typed_attributes["bid_pack"] is None
    assert {(i.code, i.path) for i in out.issues} == {
        ("invalid_money", "bidPoBidpPlanProjectDetailView.investTotal"),
        ("invalid_date", "bidPoBidpPlanProjectDetailView.decisionDate"),
        ("invalid_date", "bidPoBidpPlanProjectDetailView.publicDate"),
        ("invalid_count", "bidPoBidpPlanProjectDetailView.bidPack"),
    }


@pytest.mark.parametrize("bad_count", [-1, 2.5, True, "2.0", "-2"])
def test_invalid_bid_pack_value_warns(bad_count):
    out = transform(bidPack=bad_count)
    assert out.quarantine is None
    assert out.typed_attributes["bid_pack"] is None
    assert any(i.code == "invalid_count" for i in out.issues)


def test_money_with_missing_currency_warns_without_inference():
    out = transform(investTotalUnit=None)
    assert out.quarantine is None
    assert out.typed_attributes["currency"] is None
    assert any(i.code == "unknown_currency" for i in out.issues)


def test_optional_nulls_are_not_errors():
    out = transform(
        investTotal=None,
        investTotalUnit=None,
        decisionDate=None,
        publicDate=None,
        bidPack=None,
        location=None,
    )
    assert out.quarantine is None
    assert out.issues == []


def test_repeated_transform_is_deterministic():
    x = transform(pid="project-123", pno=None)
    y = transform(pid="project-123", pno=None)
    assert x.entity.entity_id == y.entity.entity_id
    assert x.revision.revision_id == y.revision.revision_id
    assert x.references[0].relationship_id == y.references[0].relationship_id
