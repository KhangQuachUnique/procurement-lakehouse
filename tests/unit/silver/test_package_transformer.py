"""Package transformer contract tests for KHLCNT bid package detail.

Covers:
- 25 revision fields including entity_type and public_date (partition key)
- Child extraction: 9-column package_location and 12-column package_lot
- Cleaners, plan reference resolution, medicine lots, and quarantine handling.
"""

from datetime import UTC, date, datetime
import json

import pytest

from procurement.silver.domain.protocols import (
    RawRecordEnvelope,
    SilverTransformerProtocol,
)
from procurement.silver.domain.registry import get_transformer
from procurement.silver.domain.transformers.package import (
    PACKAGE_LOCATION_COLUMNS,
    PACKAGE_LOT_COLUMNS,
    PACKAGE_REVISION_COLUMNS,
    PackageDetailTransformer,
)


def bronze_payload(**overrides):
    payload = {
        "id": "mi_40e6e536-d26b-4cec-a61c-da1c1ae7a7df",
        "bidNo": "BP2200029655",
        "bidName": "Thi công xây dựng",
        "planNo": "20220100496",
        "planId": "8809228217265904572",
        "planVersion": "00",
        "bidPrice": 9468825000,
        "bidPriceUnit": "VND",
        "processApply": "LDT",
        "bidField": "XL",
        "bidForm": "DTRR",
        "bidMode": "1_MTHS",
        "capitalDetail": "Ngân sách huyện",
        "ctype": "TG",
        "cperiod": 240,
        "cperiodUnit": "D",
        "isDomestic": 1,
        "isInternet": 1,
        "bidStartUnit": "Q",
        "bidStartYear": 2021,
        "bidStartQuarter": "IV",
        "bidStartMonth": 0,
        "isMultiLot": 0,
        "createdDate": "2022-10-19T15:40:53.781",
        "bidLocation": [
            {
                "id": "location-1",
                "provCode": "713",
                "provName": "Tỉnh Đồng Nai",
                "districtCode": None,
                "districtName": None,
                "wardCode": None,
                "wardName": None,
            }
        ],
        "bidpBidLotList": [
            {
                "id": "lot-id-1",
                "lotNo": "BP2200029655",
                "lotName": "Thi công xây dựng",
                "lotPrice": 9468825000,
                "medicineCode": None,
                "quantity": None,
                "uom": None,
                "pricePlan": None,
            }
        ],
    }
    payload.update(overrides)
    return payload


def envelope(payload, *, source_version=None):
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name="khlcnt_bid_package_detail",
        resource="khlcnt",
        source_date="2024-01-01",
        run_id="run-1",
        row_ordinal=0,
        file_key="bronze/khlcnt_bid_package_detail/source_date=2024-01-01/a.parquet",
        file_sha256="abc",
        source_id=payload.get("id") or "unknown",
        source_version=source_version,
        content_hash="fake_hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        payload=payload,
    )


def transform(payload, **kwargs):
    return PackageDetailTransformer().transform(envelope(payload, **kwargs))


def children_by_kind(result, kind):
    return [
        json.loads(child.payload_json)
        for child in result.children
        if child.kind == kind
    ]


def test_registered_protocol_and_identity_scheme():
    transformer = PackageDetailTransformer()
    assert isinstance(transformer, SilverTransformerProtocol)
    assert get_transformer(transformer.table_name) is not None
    assert transformer.entity_type == "package"
    assert transformer.id_scheme == "uuid"


def test_all_25_revision_fields_and_source_identity():
    result = transform(bronze_payload())
    assert result.quarantine is None
    assert result.entity.source_identity == "mi_40e6e536-d26b-4cec-a61c-da1c1ae7a7df"
    assert set(result.typed_attributes) == set(PACKAGE_REVISION_COLUMNS)
    assert len(PACKAGE_REVISION_COLUMNS) == 25
    typed = result.typed_attributes
    assert typed["entity_type"] == "package"
    assert typed["package_no"] == "BP2200029655"
    assert typed["title"] == "Thi công xây dựng"
    assert typed["plan_no"] == "20220100496"
    assert typed["plan_id"] is None
    assert typed["bid_price"] == "9468825000.000000"
    assert typed["is_domestic"] is True
    assert typed["is_internet"] is True
    assert typed["execution_period"] == 240
    assert typed["bid_start_quarter"] == "IV"
    assert typed["bid_start_month"] is None
    assert typed["public_date"] == "2022-10-19"
    assert json.loads(typed["root_json"])["planVersion"] == "00"
    assert not result.issues


def test_public_date_fallback_order():
    # 1. createdDate
    p1 = bronze_payload(
        createdDate="2023-05-10T10:00:00",
        originalCreateDate="2022-01-01T00:00:00",
        exchangeDate="2021-01-01T00:00:00",
    )
    assert transform(p1).typed_attributes["public_date"] == "2023-05-10"

    # 2. originalCreateDate
    p2 = bronze_payload(
        createdDate=None,
        originalCreateDate="2022-04-15T00:00:00",
        exchangeDate="2021-01-01T00:00:00",
    )
    assert transform(p2).typed_attributes["public_date"] == "2022-04-15"

    # 3. exchangeDate
    p3 = bronze_payload(
        createdDate=None,
        originalCreateDate=None,
        exchangeDate="2021-12-31T00:00:00",
    )
    assert transform(p3).typed_attributes["public_date"] == "2021-12-31"

    # 4. none
    p4 = bronze_payload(
        createdDate=None,
        originalCreateDate=None,
        exchangeDate=None,
    )
    assert transform(p4).typed_attributes["public_date"] is None


def test_link_plan_by_id_preserves_old_numeric_id():
    result = transform(bronze_payload())
    assert len(result.references) == 1
    ref = result.references[0]
    assert ref.target_type == "plan"
    assert ref.id_scheme == "uuid"
    assert ref.target_identity == "8809228217265904572"
    assert ref.field_name == "planId"
    assert result.typed_attributes["plan_id"] is None


@pytest.mark.parametrize("plan_no", ["PL2400000054", "20220100496"])
def test_fallback_plan_no_when_plan_id_absent(plan_no):
    result = transform(bronze_payload(planId=None, planNo=plan_no))
    assert len(result.references) == 1
    ref = result.references[0]
    assert (ref.target_type, ref.id_scheme, ref.target_identity, ref.field_name) == (
        "plan",
        "number",
        plan_no,
        "planNo",
    )


@pytest.mark.parametrize("placeholder", [None, "N/A", " null ", "", "NONE"])
def test_invalid_plan_identifiers_are_not_foreign_keys(placeholder):
    result = transform(bronze_payload(planId=placeholder, planNo=placeholder))
    assert result.references == []
    assert result.typed_attributes["plan_id"] is None


def test_location_child_exactly_nine_columns():
    result = transform(bronze_payload())
    locations = children_by_kind(result, "package_location")
    assert len(locations) == 1
    assert set(locations[0]) == set(PACKAGE_LOCATION_COLUMNS)
    assert len(PACKAGE_LOCATION_COLUMNS) == 9
    assert locations[0]["location_ordinal"] == 0
    assert locations[0]["province_code"] == "713"
    assert locations[0]["location_id"] is not None


def test_lot_child_exactly_twelve_columns():
    result = transform(bronze_payload())
    lots = children_by_kind(result, "package_lot")
    assert len(lots) == 1
    assert set(lots[0]) == set(PACKAGE_LOT_COLUMNS)
    assert len(PACKAGE_LOT_COLUMNS) == 12
    assert lots[0]["source_lot_id"] == "lot-id-1"
    assert lots[0]["lot_ordinal"] == 0
    assert lots[0]["lot_no"] == "BP2200029655"
    assert lots[0]["lot_price"] == "9468825000.000000"
    assert lots[0]["currency"] == "VND"
    assert lots[0]["medicine_code"] is None


def test_pipe_wrapped_location_codes_preserve_leading_zeroes():
    payload = bronze_payload(
        bidLocation=[
            {
                "id": "loc-2",
                "provCode": "|001|",
                "provName": "Thành phố Hà Nội",
                "districtCode": "|00123|",
                "districtName": "Quận A",
                "wardCode": "|00042|",
                "wardName": "Phường B",
            }
        ]
    )
    location = children_by_kind(transform(payload), "package_location")[0]
    assert (
        location["province_code"],
        location["district_code"],
        location["ward_code"],
    ) == ("001", "00123", "00042")


def test_three_lots_kept_when_is_multi_lot_zero():
    source_lots = [
        {
            "id": "L1",
            "lotNo": "PP2200005970",
            "lotName": "Sửa chữa văn phòng",
            "lotPrice": 7501519236,
        },
        {
            "id": "L2",
            "lotNo": "PP2200005971",
            "lotName": "Trang bị nội thất",
            "lotPrice": 3127066123,
        },
        {"id": "L3", "lotNo": "PP2200005972", "lotName": "Lô thứ ba", "lotPrice": 100},
    ]
    result = transform(bronze_payload(isMultiLot=0, bidpBidLotList=source_lots))
    lots = children_by_kind(result, "package_lot")
    assert [r["lot_ordinal"] for r in lots] == [0, 1, 2]
    assert [r["lot_no"] for r in lots] == [
        "PP2200005970",
        "PP2200005971",
        "PP2200005972",
    ]


def test_one_lot_kept_when_is_multi_lot_one():
    lots = children_by_kind(transform(bronze_payload(isMultiLot=1)), "package_lot")
    assert len(lots) == 1


def test_empty_lists_are_valid_zero_children():
    result = transform(bronze_payload(bidLocation=[], bidpBidLotList=[]))
    assert result.quarantine is None
    assert result.children == []


def test_medicine_price_quantity_unit_price_mapping():
    lot = {
        "id": "5b0035ad-a177-443f-bdad-948cf3ad0265",
        "lotNo": "PP2400435952",
        "lotName": "Estradiol valerate",
        "lotPrice": 14279000,
        "medicineCode": "2241110024748.01",
        "quantity": 4360,
        "uom": "Viên",
        "pricePlan": 3275,
        "nongDo": "2mg",
        "duongDung": "Uống",
        "dangBaoChe": "Viên",
    }
    result = transform(bronze_payload(bidpBidLotList=[lot]))
    row = children_by_kind(result, "package_lot")[0]
    assert row["lot_price"] == "14279000.000000"
    assert row["quantity"] == "4360.000000"
    assert row["planned_unit_price"] == "3275.000000"
    assert row["medicine_code"] == "2241110024748.01"
    assert row["uom"] == "Viên"
    assert float(row["quantity"]) * float(row["planned_unit_price"]) == float(
        row["lot_price"]
    )
    # Source-only attributes are retained in parent root_json.
    assert (
        json.loads(result.typed_attributes["root_json"])["bidpBidLotList"][0]["nongDo"]
        == "2mg"
    )


def test_3275_medicine_rows_are_not_truncated_or_collapsed():
    lots = [
        {
            "id": f"lot-{i}",
            "lotNo": f"PP{i:09d}",
            "lotPrice": i,
            "quantity": i + 1,
            "pricePlan": 2,
        }
        for i in range(3275)
    ]
    result = transform(bronze_payload(isMultiLot=1, bidpBidLotList=lots))
    transformed = children_by_kind(result, "package_lot")
    assert len(transformed) == 3275
    assert transformed[-1]["lot_ordinal"] == 3274
    assert len({r["lot_id"] for r in transformed}) == 3275


def test_duplicate_business_numbers_do_not_merge_source_entities():
    p1 = bronze_payload(id="different-package-A", bidNo="BP2200001443", planId="plan-A")
    p2 = bronze_payload(id="different-package-B", bidNo="BP2200001443", planId="plan-B")
    a, b = transform(p1), transform(p2)
    assert a.entity.entity_id != b.entity.entity_id
    assert a.typed_attributes["package_no"] == b.typed_attributes["package_no"]


def test_no_canonical_fk_is_assigned_in_transformer():
    result = transform(bronze_payload())
    assert result.typed_attributes["plan_id"] is None
    assert len(result.references) == 1


def test_package_plan_version_does_not_become_package_source_version():
    result = transform(bronze_payload(planVersion="22"))
    assert result.revision.source_version is None


def test_missing_package_identity_quarantines():
    result = transform(bronze_payload(id=None))
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_identity"


def test_empty_root_quarantines():
    result = transform({})
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_root"


def test_invalid_money_warns_but_preserves_package():
    result = transform(bronze_payload(bidPrice="not-money"))
    assert result.quarantine is None
    assert result.typed_attributes["bid_price"] is None
    assert any(
        issue.code == "invalid_money" and issue.path == "bidPrice"
        for issue in result.issues
    )


def test_invalid_optional_child_does_not_quarantine_parent():
    result = transform(
        bronze_payload(
            bidLocation=["bad", {"provCode": "713"}],
            bidpBidLotList=[None, {"id": "valid-lot", "lotPrice": "not-money"}],
        )
    )
    assert result.quarantine is None
    assert len(children_by_kind(result, "package_location")) == 1
    assert len(children_by_kind(result, "package_lot")) == 1
    assert any(i.code == "invalid_child" for i in result.issues)
    assert any(i.code == "invalid_money" for i in result.issues)


def test_malformed_collections_warn_without_crashing():
    result = transform(bronze_payload(bidLocation="bad", bidpBidLotList={}))
    assert result.quarantine is None
    assert result.children == []
    assert sum(i.code == "invalid_child_collection" for i in result.issues) == 2


def test_no_silent_currency_default_when_absent():
    result = transform(bronze_payload(bidPriceUnit=None))
    assert result.typed_attributes["currency"] is None
    assert children_by_kind(result, "package_lot")[0]["currency"] is None
    assert any(i.code == "unknown_currency" for i in result.issues)


def test_explicit_lot_currency_overrides_package_currency():
    payload = bronze_payload(
        bidpBidLotList=[{"id": "lot-1", "lotPrice": 100, "lotPriceUnit": "USD"}]
    )
    assert children_by_kind(transform(payload), "package_lot")[0]["currency"] == "USD"


def test_invalid_month_warns_and_preserves_parent():
    result = transform(bronze_payload(bidStartUnit="M", bidStartMonth=13))
    assert result.quarantine is None
    assert result.typed_attributes["bid_start_month"] is None
    assert any(i.code == "invalid_month" for i in result.issues)


def test_semantic_identity_and_child_ids_are_deterministic():
    payload = bronze_payload()
    a = transform(payload)
    b = transform(payload)
    assert a.entity.entity_id == b.entity.entity_id
    assert a.revision.revision_id == b.revision.revision_id
    assert [c.child_id for c in a.children] == [c.child_id for c in b.children]
