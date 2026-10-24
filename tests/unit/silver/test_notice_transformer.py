"""Notice contracts: family semantics, loss preservation and actual SQL writes."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest

from procurement.silver.application.observations import transform
from procurement.silver.application.resources import NOTICE_OUTPUT_TABLES, NOTICE_TABLES
from procurement.silver.application.revisions import assemble, validate
from procurement.silver.application.warehouse import SCHEMAS
from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.registry import get_transformer


def root():
    return {
        "id": "doc-001",
        "notifyNo": "IB001",
        "notifyVersion": "00",
        "bidId": "pkg-001",
        "bidNo": "BP001",
        "bidName": "Notice",
        "planNo": "PL001",
        "publicDate": "2025-01-01T10:30:00",
        "bidPrice": "12345678901234567890.123456",
        "bidPriceUnit": "VND",
        "investorCode": "INV001",
        "procuringEntityCode": "BUY001",
        "isInternet": "0",
    }


def payload(family, values=None):
    values = root() if values is None else values
    if family == "standard":
        return {"bidoNotifyContractorM": values}
    if family == "vk_adb":
        return {"bidoNotifyContractorP": values}
    return {**values, "reofferNo": "IB001", "reofferVersion": "00"}


def envelope(family="standard", data=None):
    return RawRecordEnvelope(
        namespace="test",
        table_name=f"notify_contractor_{family}_detail",
        resource="notify",
        source_date="2025-01-01",
        run_id="r1",
        row_ordinal=0,
        file_key=f"bronze/{family}.parquet",
        file_sha256="test-checksum",
        source_id="IB001",
        source_version="00",
        content_hash="hash",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload=payload(family) if data is None else data,
    )


def run(env):
    return get_transformer(env.table_name).transform(env)


def sql_roundtrip(tables):
    with duckdb.connect(":memory:") as con:
        for name, rows in tables.items():
            if not rows:
                continue
            con.execute(f"CREATE TABLE {name} ({SCHEMAS[name]})")
            con.register("candidate", pa.Table.from_pylist(rows))
            con.execute(f"INSERT INTO {name} BY NAME SELECT * FROM candidate")
            assert con.execute(f"SELECT count(*) FROM {name}").fetchone()[0] == len(rows)
            con.unregister("candidate")
        return con.execute(
            "SELECT bid_price, published_at, is_internet FROM notice_revision"
        ).fetchone()


@pytest.mark.parametrize("family", ["standard", "vk_adb", "reoffer"])
def test_family_schema_and_decimal_roundtrip(family):
    result = run(envelope(family))
    assert result.quarantine is None
    assert result.typed_attributes["buyer_id"] == "BUY001"
    assert result.typed_attributes["investor_code"] == "INV001"
    assert result.typed_attributes["public_date_raw"] == "2025-01-01T10:30:00"
    tables = assemble([result.to_dict()])
    assert validate(tables)["accepted"] == 1
    amount, published, online = sql_roundtrip(tables)
    assert amount == Decimal("12345678901234567890.123456")
    assert published == datetime(2025, 1, 1, 3, 30, tzinfo=UTC)
    assert online is False


def test_family_specific_fields_and_no_currency_inference():
    r = {
        **root(),
        "bidGuaranteeValue": 100,
        "bidGuaranteeForm": "bank",
        "cType": "TG",
        "cPeriod": 24,
        "cPeriodUnit": "W",
    }
    out = run(envelope("vk_adb", payload("vk_adb", r)))
    a = out.typed_attributes
    assert (a["guarantee_amount"], a["guarantee_form"]) == ("100.000000", "bank")
    assert (a["contract_duration_raw"], a["contract_duration_unit"]) == ("24", "W")
    assert a["guarantee_currency"] is None
    assert any(i.code == "unknown_currency" for i in out.issues)


def test_reoffer_prices_and_dates_are_not_conventional_notice_prices():
    p = payload(
        "reoffer",
        {
            **root(),
            "bidPrice": None,
            "reofferOpenDate": "2025-01-02T10:00:00",
            "priceInit": 1000,
            "priceStep": 0,
        },
    )
    result = run(envelope("reoffer", p))
    assert result.typed_attributes["bid_price"] is None
    assert result.typed_attributes["initial_price"] == "1000.000000"
    assert result.typed_attributes["price_step"] == "0.000000"
    assert result.typed_attributes["bid_open_date"] is None
    assert result.typed_attributes["reoffer_open_at"].endswith("+07:00")
    assert result.entity.id_scheme == "reoffer_no"


def test_children_preserve_mirrors_unknown_arrays_and_do_not_double_count_lots():
    lot = {"id": "lot-1", "lotNo": "L1", "lotPrice": 12, "lotPriceUnit": "VND"}
    p = {
        "bidoNotifyContractorM": {**root(), "lotDTOList": [lot]},
        "bidNoContractorResponse": {"bidNotification": {**root(), "lotDTOList": [lot]}},
        "bidpBidLocationList": [{"provCode": "|001|", "provName": "Province"}],
        "newUnknownList": [None, {"nested": [1, 2]}],
    }
    result = run(envelope(data=p))
    tables = assemble([result.to_dict()])
    assert len(tables["notice_lot"]) == 1
    assert "child" not in tables
    assert "child" not in NOTICE_OUTPUT_TABLES
    assert tables["notice_location"][0]["province_code"] == "001"
    assert json.loads(result.observation["payload_json"]) == p
    validate(tables)
    sql_roundtrip(tables)


def test_reoffer_lists_remain_separate_occurrences_with_typed_values():
    child = {
        "id": "x",
        "contractorCode": "0001",
        "reofferPrice": 0,
        "reofferPriceFinal": 100,
        "reofferDate": "2025-02-01T12:00:00Z",
        "isNewest": 0,
        "formValue": '{"price":1.23}',
    }
    p = {**payload("reoffer"), "contractorsResult": [child], "contractorsResultAll": [child]}
    result = run(envelope("reoffer", p))
    tables = assemble([result.to_dict()])
    rows = tables["notice_reoffer_submission"]
    assert len(rows) == 2 and len({r["child_id"] for r in rows}) == 2
    assert {r["source_list"] for r in rows} == {"contractorsResult", "contractorsResultAll"}
    assert all(r["is_newest"] is False and r["price"] == "0.000000" for r in rows)
    assert all(r["currency"] is None for r in rows)
    sql_roundtrip(tables)


@pytest.mark.parametrize(
    "field,value",
    [("id", "other"), ("notifyNo", "other"), ("notifyVersion", "01"), ("bidId", "other")],
)
def test_conflicting_mirror_quarantined_with_raw_evidence(field, value):
    p = {
        "bidoNotifyContractorM": root(),
        "bidNoContractorResponse": {"bidNotification": {**root(), field: value}},
    }
    result = run(envelope(data=p))
    assert result.quarantine.reason == "conflicting_identity"
    tables = assemble([result.to_dict()])
    assert validate(tables)["quarantined"] == 1
    assert json.loads(tables["observation"][0]["payload_json"]) == p


@pytest.mark.parametrize(
    "p",
    [
        {},
        {"bidoNotifyContractorM": None},
        {"bidoNotifyContractorM": {"id": None, "notifyNo": None}},
    ],
)
def test_missing_business_root_or_identity_quarantined(p):
    assert run(envelope(data=p)).quarantine is not None


def test_invalid_optional_values_are_visible_without_losing_record():
    p = payload("standard", {**root(), "bidPrice": "bad", "publicDate": "bad", "isInternet": 9})
    result = run(envelope(data=p))
    assert result.quarantine is None
    assert {i.code for i in result.issues} >= {
        "invalid_money",
        "invalid_datetime",
        "invalid_boolean",
    }
    assert result.typed_attributes["bid_price"] is None
    assert result.typed_attributes["public_date_raw"] == "bad"
    sql_roundtrip(assemble([result.to_dict()]))


@pytest.mark.parametrize("family", ["standard", "vk_adb", "reoffer"])
def test_build_dispatch_matches_registered_transformer(family):
    env = envelope(family)
    raw = json.dumps(env.payload)
    record = {
        "payload": raw,
        "ingested_at": env.ingested_at,
        "run_id": env.run_id,
        "source_id": env.source_id,
        "source_version": env.source_version,
        "content_hash": env.content_hash,
    }
    result = transform(
        env.namespace,
        {"resource": env.resource, "source_date": env.source_date},
        {"key": env.file_key, "sha256": env.file_sha256, "table": env.table_name},
        0,
        record,
    )
    expected = run(env).to_dict()
    assert result["typed"] == expected["typed"]
    assert result["revision"] == expected["revision"]
    assert result["observation"]["payload_json"] == raw
    assert env.table_name in NOTICE_TABLES


def test_verified_embedded_package_metadata_and_provenance():
    p = {
        **payload("standard"),
        "bidpPlanDetail": {"id": "pkg-001", "planId": "p", "planVersion": "03"},
    }
    result = run(envelope(data=p))
    assert result.typed_attributes["plan_version"] == "03"
    sources = json.loads(result.typed_attributes["field_sources_json"])
    assert sources["title"] == "bidoNotifyContractorM.bidName"
    assert sources["plan_id"] == "bidpPlanDetail.planId"
    p["bidpPlanDetail"]["id"] = "wrong"
    result = run(envelope(data=p))
    assert result.typed_attributes["plan_id"] is None
    assert any(i.code == "unverified_embedded_package" for i in result.issues)


def test_empty_primary_mirror_can_use_valid_alternate():
    p = {"bidoNotifyContractorM": {}, "bidNoContractorResponse": {"bidNotification": root()}}
    result = run(envelope(data=p))
    assert result.quarantine is None
    assert result.typed_attributes["source_root_path"] == "bidNoContractorResponse.bidNotification"


@pytest.mark.parametrize("field,value", [("source_id", "wrong"), ("source_version", "99")])
def test_envelope_conflict_quarantined(field, value):
    from dataclasses import replace

    result = run(replace(envelope(), **{field: value}))
    assert result.quarantine.reason == "conflicting_identity"


def test_source_version_fallback_preserves_original_observation():
    from dataclasses import replace

    result = run(replace(envelope(), source_version=None))
    assert result.revision.source_version == "00"
    assert result.observation["source_version"] is None


def test_notice_mapping_version_is_part_of_revision_identity():
    from procurement.silver.domain.transformers.notice import StandardNoticeTransformer

    current = StandardNoticeTransformer()
    newer = StandardNoticeTransformer()
    newer.revision_contracts = {**current.revision_contracts, "notice": "notice-next"}
    assert (
        current.transform(envelope()).revision.revision_id
        != newer.transform(envelope()).revision.revision_id
    )


def test_notice_fallback_extractions_from_plan_detail():
    p = {
        "bidoNotifyContractorM": root(),
        "bidpPlanDetail": {
            "id": "pkg-001",
            "isMultiLot": 1,
            "bidField": "XL",
            "cperiod": "180",
        },
    }
    result = run(envelope(data=p))
    attrs = result.typed_attributes
    assert attrs["is_multi_lot"] is True
    assert attrs["bid_field"] == "XL"
    assert attrs["contract_duration_raw"] == "180"


def test_notice_temporal_inversion_detected():
    r = root()
    r["bidCloseDate"] = "2025-01-10T10:00:00"
    r["bidOpenDate"] = "2025-01-05T10:00:00"  # Opens before it closes
    p = {"bidoNotifyContractorM": r}
    result = run(envelope(data=p))
    assert any(i.code == "invalid_date_order" for i in result.issues)

