"""Unit tests for ContractorResultTransformer (contractor selection results)."""

from datetime import UTC, datetime

from procurement.silver.application.resources import (
    RESULT_MAPPING_VERSION,
    RESULT_OUTPUT_TABLES,
    get_silver_resource,
)
from procurement.silver.application.revisions import assemble, validate
from procurement.silver.application.warehouse import typed_arrow_rows
from procurement.silver.domain.protocols import (
    RawRecordEnvelope,
    SilverTransformerProtocol,
)
from procurement.silver.domain.registry import get_transformer
from procurement.silver.domain.transformers.result import ContractorResultTransformer


def make_envelope(payload: dict, source_id: str = "res-uuid-123") -> RawRecordEnvelope:
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name="contractor_result_detail",
        resource="contractor_result",
        source_date="2024-09-16",
        run_id="run-001",
        row_ordinal=0,
        file_key="bronze/contractor_result/2024-09-16/file.parquet",
        file_sha256="fake_sha256",
        source_id=source_id,
        source_version="00",
        content_hash="fake_content_hash",
        ingested_at=datetime(2024, 9, 16, 16, 0, 0, tzinfo=UTC),
        payload=payload,
    )


def test_contractor_result_protocol_conformance():
    transformer = ContractorResultTransformer()
    assert isinstance(transformer, SilverTransformerProtocol)
    assert transformer.table_name == "contractor_result_detail"
    assert transformer.entity_type == "result"
    assert transformer.id_scheme == "uuid"


def test_registry_lookup():
    transformer = get_transformer("contractor_result_detail")
    assert transformer is not None
    assert isinstance(transformer, ContractorResultTransformer)


def test_transform_valid_contractor_result():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "33afb06f-c20f-4e0f-88d3-deed0f22fb6e",
            "resultVersion": "00",
            "notifyNo": "IB2200000059",
            "notifyVersion": "00",
            "bidId": "a28a4270-a04a-4093-808d-6db3edb01e71",
            "bidNo": "BP2200001486",
            "bidName": "Gói thầu số 06: Lập HSMT, đánh giá HSDT gói thầu số 07",
            "status": "IS_PUBLISH",
            "procuringEntityCode": "vn0200288138",
            "procuringEntityName": "Trường Cao đẳng Hàng hải I",
            "investorCode": "vn0200288138",
            "investorName": "Trường Cao đẳng Hàng hải I",
            "publicDate": "2022-09-16T16:34:30",
            "decisionNo": "986/QĐ-CĐHHI",
            "decisionDate": "2022-09-16T09:32:57",
            "decisionAgency": "Trường Cao đẳng Hàng hải I",
            "bidEstimatePrice": "8000000",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "3506fae8-bd00-42a3-a41f-76e1be6f970b",
                    "bidNo": "BP2200001486",
                    "bidName": "Gói thầu số 06: Lập HSMT, đánh giá HSDT gói thầu số 07",
                    "lotPrice": "8000000",
                    "winningCode": "WIN-01",
                    "status": 1,
                    "contractorList": [
                        {
                            "id": "21db811b-b2ab-4420-97a6-17007bd914ee",
                            "orgCode": "vn0201604435",
                            "orgFullname": "CÔNG TY CỔ PHẦN TƯ VẤN VÀ ĐẦU TƯ XÂY DỰNG AN HƯNG",
                            "taxCode": "0201604435",
                            "taxNation": "VN",
                            "bidWiningPrice": "7448857",
                            "bidResult": 1,
                        }
                    ],
                }
            ],
        }
    }
    envelope = make_envelope(payload, source_id="33afb06f-c20f-4e0f-88d3-deed0f22fb6e")
    result = transformer.transform(envelope)

    # 1. Observation
    assert result.observation["table_name"] == "contractor_result_detail"
    assert result.observation["source_id"] == "33afb06f-c20f-4e0f-88d3-deed0f22fb6e"

    # 2. Entity & Revision
    assert result.entity is not None
    assert result.entity.entity_type == "result"
    assert result.entity.source_identity == "33afb06f-c20f-4e0f-88d3-deed0f22fb6e"
    assert result.revision is not None

    # 3. Attributes
    attrs = result.typed_attributes
    assert attrs["notify_no"] == "IB2200000059"
    assert attrs["package_no"] == "BP2200001486"
    assert attrs["title"] == "Gói thầu số 06: Lập HSMT, đánh giá HSDT gói thầu số 07"
    assert attrs["decision_no"] == "986/QĐ-CĐHHI"
    assert attrs["decision_agency"] == "Trường Cao đẳng Hàng hải I"
    assert attrs["total_winning_price"] == "7448857.000000"
    assert attrs["currency"] == "VND"
    assert attrs["procuring_entity_code"] == "vn0200288138"

    # 4. Children (award_lot and award_contractor)
    assert len(result.children) == 2
    lot_child = next(c for c in result.children if c.kind == "award_lot")
    assert lot_child.typed_table == "award_lot"
    assert lot_child.typed_attributes is not None
    assert lot_child.typed_attributes["lot_price"] == "8000000.000000"
    assert lot_child.typed_attributes["winning_price"] == "7448857.000000"
    assert "saving_amount" not in lot_child.typed_attributes
    assert "saving_rate" not in lot_child.typed_attributes

    contractor_child = next(c for c in result.children if c.kind == "award_contractor")
    assert contractor_child.typed_table == "award_contractor"
    assert contractor_child.typed_attributes["contractor_code"] == "vn0201604435"
    assert (
        contractor_child.typed_attributes["contractor_name"]
        == "CÔNG TY CỔ PHẦN TƯ VẤN VÀ ĐẦU TƯ XÂY DỰNG AN HƯNG"
    )
    assert contractor_child.typed_attributes["awarded_price"] == "7448857.000000"

    # 5. References
    assert len(result.references) == 2
    assert any(ref.target_type == "notice" for ref in result.references)
    assert any(ref.target_type == "package" for ref in result.references)

    # 6. Quality & Quarantine
    assert len(result.issues) == 0
    assert result.quarantine is None


def test_transform_missing_root_quarantine():
    transformer = ContractorResultTransformer()
    payload = {"unexpected_key": {}}
    envelope = make_envelope(payload)
    result = transformer.transform(envelope)

    assert result.entity is None
    assert result.revision is None
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_root"


def test_transform_missing_id_quarantine():
    transformer = ContractorResultTransformer()
    payload = {"bideContractorInputResultDTO": {"notifyNo": "IB12345"}}
    envelope = make_envelope(payload)
    result = transformer.transform(envelope)

    assert result.entity is None
    assert result.revision is None
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_identity"


def test_result_full_assembly_and_validation():
    contract = get_silver_resource("contractor_result")
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-001",
            "notifyNo": "IB2200000059",
            "bidNo": "BP2200001486",
            "bidName": "Gói thầu xây lắp",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "LOT-1",
                    "lotNo": "1",
                    "lotName": "Lô số 1",
                    "lotPrice": "1000000000",
                    "contractorList": [
                        {
                            "id": "CONT-1",
                            "orgCode": "vn123456",
                            "orgFullname": "Nhà thầu A",
                            "bidWiningPrice": "950000000",
                            "bidResult": 1,
                        }
                    ],
                }
            ],
        }
    }
    envelope = make_envelope(payload, source_id="RES-001")
    result = transformer.transform(envelope)
    tables = assemble([result.to_dict()])
    quality = validate(tables)

    assert quality["accepted"] == 1
    assert quality["quarantined"] == 0
    assert quality["blocking_issues"] == 0

    revisions = tables["result_revision"]
    assert len(revisions) == 1
    rev = revisions[0]
    assert rev["is_current"] is True
    assert rev["observation_id"] is not None
    assert rev["total_winning_price"] == "950000000.000000"

    lots = tables["award_lot"]
    assert len(lots) == 1
    assert lots[0]["lot_no"] == "1"
    assert "saving_amount" not in lots[0]
    assert "saving_rate" not in lots[0]

    contractors = tables["award_contractor"]
    assert len(contractors) == 1
    assert contractors[0]["contractor_code"] == "vn123456"
    assert contractors[0]["contractor_name"] == "Nhà thầu A"
    assert contractors[0]["awarded_price"] == "950000000.000000"

    # Verify PyArrow / DuckDB typed schema casting
    for name in contract.output_tables:
        arrow_table = typed_arrow_rows(name, tables[name])
        assert arrow_table is not None
        if name in {"result_revision", "award_lot", "award_contractor", "observation"}:
            assert arrow_table.num_rows == 1


def test_result_resource_contract():
    for alias in ("result", "contractor_result", "kq_lcnt"):
        contract = get_silver_resource(alias)
        assert contract.resource == "contractor_result"
        assert contract.mapping_version == RESULT_MAPPING_VERSION
        assert contract.output_tables == RESULT_OUTPUT_TABLES
        assert contract.select_tables() == ("contractor_result_detail",)


def test_result_plan_reference_extraction():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-PLAN-1",
            "notifyNo": "IB2200000001",
            "bidId": "PKG-001",
            "planNo": "PL2200001197",
        }
    }
    envelope = make_envelope(payload, source_id="RES-PLAN-1")
    result = transformer.transform(envelope)

    assert len(result.references) == 3
    assert any(
        r.target_type == "plan" and r.target_identity == "PL2200001197" for r in result.references
    )


def test_result_version_fallback_from_dto():
    from dataclasses import replace

    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-VER-1",
            "resultVersion": "02",
        }
    }
    envelope = replace(make_envelope(payload, source_id="RES-VER-1"), source_version=None)
    result = transformer.transform(envelope)

    assert result.revision.source_version == "02"
    assert result.observation["source_version"] is None


def test_result_temporal_inversion_detected():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-TEMP-1",
            "decisionDate": "2024-09-20T10:00:00",
            "publicDate": "2024-09-10T10:00:00",  # Published before decided!
        }
    }
    envelope = make_envelope(payload, source_id="RES-TEMP-1")
    result = transformer.transform(envelope)

    assert any(i.code == "invalid_date_order" for i in result.issues)


def test_result_transformer_dummy_placeholder_price():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-dummy-1",
            "notifyNo": "IB9999",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPrice": "1",  # dummy placeholder
                    "contractorList": [
                        {
                            "id": "c-1",
                            "orgCode": "vn123",
                            "bidResult": 1,
                            "bidWiningPrice": "500000000",
                        }
                    ],
                }
            ],
        }
    }
    env = make_envelope(payload, source_id="res-dummy-1")
    result = transformer.transform(env)
    lot_child = next(c for c in result.children if c.kind == "award_lot")
    assert lot_child.typed_attributes["lot_price"] is None
    assert any(i.code == "dummy_placeholder_price" for i in result.issues)


def test_award_contractor_only_confirmed_winners_bidresult_1_or_str_1():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-win-test-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPrice": "10000000",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "1000000"},
                        {
                            "id": "c2",
                            "orgCode": "v2",
                            "bidResult": "1",
                            "bidWiningPrice": "2000000",
                        },
                        {"id": "c3", "orgCode": "v3", "bidResult": 0, "bidWiningPrice": "3000000"},
                        {"id": "c4", "orgCode": "v4", "bidResult": 2},
                        {"id": "c5", "orgCode": "v5", "bidResult": "3"},
                    ],
                }
            ],
        }
    }
    env = make_envelope(payload, source_id="res-win-test-1")
    result = transformer.transform(env)

    # 5 contractor children total in payload
    contractor_children = [c for c in result.children if c.kind == "award_contractor"]
    assert len(contractor_children) == 5

    # Only c1 and c2 are confirmed winners with typed_table
    winner_children = [c for c in contractor_children if c.typed_table == "award_contractor"]
    assert len(winner_children) == 2
    assert {c.typed_attributes["contractor_code"] for c in winner_children} == {"v1", "v2"}

    # Non-winners have typed_table=None and typed_attributes=None
    non_winner_children = [c for c in contractor_children if c.typed_table is None]
    assert len(non_winner_children) == 3
    for c in non_winner_children:
        assert c.typed_attributes is None
        assert c.payload_json != ""  # Raw JSON retained

    # Check assembled typed table
    tables = assemble([result.to_dict()])
    assert len(tables["award_contractor"]) == 2
    assert {r["contractor_code"] for r in tables["award_contractor"]} == {"v1", "v2"}
    for r in tables["award_contractor"]:
        assert r["bid_result"] == 1
        assert r["price_source"] == "bidWiningPrice"
        assert r["currency"] == "VND"

    # Lot parsed correctly
    assert len(tables["award_lot"]) == 1
    assert tables["award_lot"][0]["lot_no"] == "1"

    # Non-winner c3 with bidWiningPrice triggers warning
    assert any(i.code == "non_winner_with_winning_price" for i in result.issues)


def test_keep_root_and_lot_with_zero_winners():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-zero-win-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPrice": "10000000",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 0},
                        {"id": "c2", "orgCode": "v2", "bidResult": 2},
                    ],
                }
            ],
        }
    }
    env = make_envelope(payload, source_id="res-zero-win-1")
    result = transformer.transform(env)
    tables = assemble([result.to_dict()])

    # Root revision kept with null total winning price
    assert len(tables["result_revision"]) == 1
    assert tables["result_revision"][0]["total_winning_price"] is None

    # Lot kept with null winning price
    assert len(tables["award_lot"]) == 1
    assert tables["award_lot"][0]["lot_price"] == "10000000.000000"
    assert tables["award_lot"][0]["winning_price"] is None

    # Zero rows in typed award_contractor
    assert len(tables["award_contractor"]) == 0


def test_winning_price_only_bid_wining_price_no_fallback():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-fallback-test-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPrice": "5000000",
                    "contractorList": [
                        {
                            "id": "c1",
                            "orgCode": "v1",
                            "bidResult": 1,
                            "bidWiningPrice": None,
                            "lotFinalPrice": "4000000",
                            "lotPrice": "3000000",
                        }
                    ],
                }
            ],
        }
    }
    env = make_envelope(payload, source_id="res-fallback-test-1")
    result = transformer.transform(env)
    tables = assemble([result.to_dict()])

    # No fallback: awarded_price is None
    assert len(tables["award_contractor"]) == 1
    assert tables["award_contractor"][0]["awarded_price"] is None
    assert tables["award_contractor"][0]["price_source"] is None

    # Incomplete winner price makes lot and root totals null
    assert tables["award_lot"][0]["winning_price"] is None
    assert tables["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "incomplete_winner_price" for i in result.issues)


def test_winning_price_handles_zero_explicitly_and_invalid_safely():
    transformer = ContractorResultTransformer()
    # Zero winning price: source price kept as 0, but flag unverified_zero_winning_price and null derived lot/root
    payload_zero = {
        "bideContractorInputResultDTO": {
            "id": "res-zero-price-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": 0}
                    ],
                }
            ],
        }
    }
    result_zero = transformer.transform(make_envelope(payload_zero, source_id="res-zero-price-1"))
    tables_zero = assemble([result_zero.to_dict()])
    assert tables_zero["award_contractor"][0]["awarded_price"] == "0.000000"
    assert tables_zero["award_lot"][0]["winning_price"] is None
    assert tables_zero["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "unverified_zero_winning_price" for i in result_zero.issues)

    # Invalid winning price (non-numeric)
    payload_inv = {
        "bideContractorInputResultDTO": {
            "id": "res-inv-price-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c1",
                            "orgCode": "v1",
                            "bidResult": 1,
                            "bidWiningPrice": "invalid_val",
                        }
                    ],
                }
            ],
        }
    }
    result_inv = transformer.transform(make_envelope(payload_inv, source_id="res-inv-price-1"))
    tables_inv = assemble([result_inv.to_dict()])
    assert tables_inv["award_contractor"][0]["awarded_price"] is None
    assert tables_inv["award_lot"][0]["winning_price"] is None
    assert tables_inv["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "invalid_money" for i in result_inv.issues)


def test_missing_and_conflicting_currency_no_default_vnd():
    transformer = ContractorResultTransformer()
    # Missing currency: neither unit supplied
    payload_missing = {
        "bideContractorInputResultDTO": {
            "id": "res-no-curr-1",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"}
                    ],
                }
            ],
        }
    }
    res_miss = transformer.transform(make_envelope(payload_missing, source_id="res-no-curr-1"))
    tables_miss = assemble([res_miss.to_dict()])
    assert tables_miss["result_revision"][0]["currency"] is None
    assert tables_miss["award_lot"][0]["currency"] is None
    assert tables_miss["award_contractor"][0]["currency"] is None
    assert any(i.code == "unknown_currency" for i in res_miss.issues)

    # Conflicting currency at root: bidPriceUnit USD vs bidEstimatePriceUnit VND
    payload_conflict = {
        "bideContractorInputResultDTO": {
            "id": "res-conflict-curr-1",
            "bidPriceUnit": "USD",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"}
                    ],
                }
            ],
        }
    }
    res_conf = transformer.transform(
        make_envelope(payload_conflict, source_id="res-conflict-curr-1")
    )
    tables_conf = assemble([res_conf.to_dict()])
    assert tables_conf["result_revision"][0]["currency"] is None
    assert any(i.code == "conflicting_currency" for i in res_conf.issues)


def test_incomplete_winner_prices_make_totals_null_not_partial():
    transformer = ContractorResultTransformer()
    # 2 winners in lot: C1 has price, C2 has missing price
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-incomp-price-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "1000000"},
                        {"id": "c2", "orgCode": "v2", "bidResult": 1, "bidWiningPrice": None},
                    ],
                }
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-incomp-price-1"))
    tables = assemble([result.to_dict()])

    # Lot total and Root total must be NULL, NOT 1000000.000000 (partial total)!
    assert tables["award_lot"][0]["winning_price"] is None
    assert tables["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "incomplete_winner_price" for i in result.issues)

    # Multi-lot: Lot 1 complete, Lot 2 has incomplete winner price
    payload_multilot = {
        "bideContractorInputResultDTO": {
            "id": "res-multi-incomp-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"}
                    ],
                },
                {
                    "id": "lot-2",
                    "lotNo": "2",
                    "contractorList": [
                        {"id": "c2", "orgCode": "v2", "bidResult": 1, "bidWiningPrice": None}
                    ],
                },
            ],
        }
    }
    result_multi = transformer.transform(
        make_envelope(payload_multilot, source_id="res-multi-incomp-1")
    )
    tables_multi = assemble([result_multi.to_dict()])
    lot_1 = next(l for l in tables_multi["award_lot"] if l["lot_no"] == "1")
    lot_2 = next(l for l in tables_multi["award_lot"] if l["lot_no"] == "2")
    assert lot_1["winning_price"] == "5000000.000000"
    assert lot_2["winning_price"] is None
    # Root must NOT be partial 5000000.000000!
    assert tables_multi["result_revision"][0]["total_winning_price"] is None


def test_consortium_ambiguity_conservative_null_and_warning():
    transformer = ContractorResultTransformer()
    # 2 consortium members winning same lot with repeated full total
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-consortium-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c1",
                            "orgCode": "v1",
                            "bidResult": 1,
                            "ventureCode": "VC-01",
                            "ventureName": "Liên danh An Bình",
                            "bidWiningPrice": "10000000",
                        },
                        {
                            "id": "c2",
                            "orgCode": "v2",
                            "bidResult": 1,
                            "ventureCode": "VC-01",
                            "ventureName": "Liên danh An Bình",
                            "bidWiningPrice": "10000000",
                        },
                    ],
                }
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-consortium-1"))
    tables = assemble([result.to_dict()])

    # Conservative null: never multiply repeated total to 20000000!
    assert tables["award_lot"][0]["winning_price"] is None
    assert tables["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "unverified_consortium_allocation" for i in result.issues)

    # Both contractors remain in award_contractor with individual prices
    assert len(tables["award_contractor"]) == 2
    for c in tables["award_contractor"]:
        assert c["awarded_price"] == "10000000.000000"
        assert c["is_consortium"] is True


def test_independent_same_price_winners_not_deduped():
    transformer = ContractorResultTransformer()
    # 2 independent winners with the exact same price -> do not dedup!
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-indep-same-price-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c1",
                            "orgCode": "v1",
                            "bidResult": 1,
                            "bidWiningPrice": "5000000",
                        },
                        {
                            "id": "c2",
                            "orgCode": "v2",
                            "bidResult": 1,
                            "bidWiningPrice": "5000000",
                        },
                    ],
                }
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-indep-same-price-1"))
    tables = assemble([result.to_dict()])

    # Independent same-price winners are not deduped: 5M + 5M = 10M
    assert tables["award_lot"][0]["winning_price"] == "10000000.000000"
    assert tables["result_revision"][0]["total_winning_price"] == "10000000.000000"
    assert len(tables["award_contractor"]) == 2


def test_outcome_warnings_for_missing_and_unsupported_codes():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-outcome-test-1",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": None},
                        {"id": "c2", "orgCode": "v2", "bidResult": "99"},
                    ],
                }
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-outcome-test-1"))
    assert any(i.code == "missing_outcome" for i in result.issues)
    assert any(i.code == "unsupported_outcome" for i in result.issues)


def test_raw_bid_result_strict_normalization_rejects_bool_float_accepts_stripped():
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-strict-norm-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c_strip",
                            "orgCode": "v_strip",
                            "bidResult": " 1 ",
                            "bidWiningPrice": "1000000",
                        },
                        {
                            "id": "c_bool_t",
                            "orgCode": "v_bool_t",
                            "bidResult": True,
                            "bidWiningPrice": "2000000",
                        },
                        {
                            "id": "c_bool_f",
                            "orgCode": "v_bool_f",
                            "bidResult": False,
                            "bidWiningPrice": "3000000",
                        },
                        {
                            "id": "c_flt",
                            "orgCode": "v_flt",
                            "bidResult": 1.0,
                            "bidWiningPrice": "4000000",
                        },
                    ],
                }
            ],
        }
    }
    env = make_envelope(payload, source_id="res-strict-norm-1")
    result = transformer.transform(env)
    tables = assemble([result.to_dict()])

    # Only " 1 " stripped exact string is accepted as winner!
    assert len(tables["award_contractor"]) == 1
    assert tables["award_contractor"][0]["contractor_code"] == "v_strip"
    assert tables["award_contractor"][0]["bid_result"] == 1

    # True, False, 1.0 are rejected as unsupported outcomes (never winner!)
    winner_children = [c for c in result.children if c.typed_table == "award_contractor"]
    assert len(winner_children) == 1
    assert winner_children[0].typed_attributes["contractor_code"] == "v_strip"

    # Verify bool/float flagged as unsupported_outcome
    unsupported_issues = [i for i in result.issues if i.code == "unsupported_outcome"]
    assert len(unsupported_issues) >= 3


def test_missing_or_unsupported_outcome_anywhere_in_lot_marks_unresolved():
    transformer = ContractorResultTransformer()
    # Lot has a confirmed winner, but also has a contractor with missing outcome
    payload_missing = {
        "bideContractorInputResultDTO": {
            "id": "res-unres-1",
            "bidEstimatePriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"},
                        {"id": "c2", "orgCode": "v2", "bidResult": None},
                    ],
                }
            ],
        }
    }
    result_missing = transformer.transform(make_envelope(payload_missing, source_id="res-unres-1"))
    tables_missing = assemble([result_missing.to_dict()])

    # Winner's known source price is kept in award_contractor
    assert len(tables_missing["award_contractor"]) == 1
    assert tables_missing["award_contractor"][0]["awarded_price"] == "5000000.000000"

    # But lot and root totals must be NULL (completeness unresolved!)
    assert tables_missing["award_lot"][0]["winning_price"] is None
    assert tables_missing["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "missing_outcome" for i in result_missing.issues)


def test_lot_currency_resolution_conflicts_salvaging_and_invalid_types():
    transformer = ContractorResultTransformer()

    # 1. Conflicting lotPriceUnit vs currency on the same lot
    payload_lot_conf = {
        "bideContractorInputResultDTO": {
            "id": "res-lot-curr-conf-1",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPriceUnit": "USD",
                    "currency": "VND",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"}
                    ],
                }
            ],
        }
    }
    result_lot_conf = transformer.transform(
        make_envelope(payload_lot_conf, source_id="res-lot-curr-conf-1")
    )
    tables_lot_conf = assemble([result_lot_conf.to_dict()])
    assert tables_lot_conf["award_lot"][0]["currency"] is None
    assert tables_lot_conf["award_lot"][0]["winning_price"] is None
    assert any(i.code == "conflicting_currency" for i in result_lot_conf.issues)

    # 2. Absent root currency, but unambiguous explicit lot currency salvages the lot
    payload_salvage = {
        "bideContractorInputResultDTO": {
            "id": "res-salvage-1",
            # No root currency
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "currency": "USD",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"}
                    ],
                }
            ],
        }
    }
    result_salvage = transformer.transform(
        make_envelope(payload_salvage, source_id="res-salvage-1")
    )
    tables_salvage = assemble([result_salvage.to_dict()])
    assert tables_salvage["award_lot"][0]["currency"] == "USD"
    assert tables_salvage["award_lot"][0]["winning_price"] == "5000000.000000"
    assert tables_salvage["result_revision"][0]["currency"] == "USD"
    assert tables_salvage["result_revision"][0]["total_winning_price"] == "5000000.000000"

    # 3. Currency field with bool or float is rejected
    payload_invalid_curr = {
        "bideContractorInputResultDTO": {
            "id": "res-inv-curr-1",
            "bidPriceUnit": True,
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "lotPriceUnit": 14.5,
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": "5000000"}
                    ],
                }
            ],
        }
    }
    result_inv_curr = transformer.transform(
        make_envelope(payload_invalid_curr, source_id="res-inv-curr-1")
    )
    tables_inv_curr = assemble([result_inv_curr.to_dict()])
    assert tables_inv_curr["award_lot"][0]["currency"] is None
    assert tables_inv_curr["award_lot"][0]["winning_price"] is None
    assert any(i.code == "unknown_currency" for i in result_inv_curr.issues)


def test_cross_lot_unresolved_outcome_nulls_root_total_while_keeping_valid_lot():
    """Reviewer fixture: root VND, lot L1 winner 1 price 100; lot L2 contractor outcome None / no price.

    Root total must be NULL when ANY lot has an unresolved outcome (even when that lot has zero confirmed winners),
    while preserving the valid lot L1 total for partial-scope analysis.
    """
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-cross-lot-1",
            "bidPriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c1",
                            "orgCode": "v1",
                            "bidResult": 1,
                            "bidWiningPrice": "100",
                        }
                    ],
                },
                {
                    "id": "lot-2",
                    "lotNo": "2",
                    "contractorList": [
                        {
                            "id": "c2",
                            "orgCode": "v2",
                            "bidResult": None,  # Missing outcome!
                        }
                    ],
                },
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-cross-lot-1"))
    tables = assemble([result.to_dict()])

    # Valid lot L1 total is preserved for partial-scope analysis
    l1 = next(l for l in tables["award_lot"] if l["lot_no"] == "1")
    assert l1["winning_price"] == "100.000000"

    # Lot L2 has no confirmed winner and is unresolved
    l2 = next(l for l in tables["award_lot"] if l["lot_no"] == "2")
    assert l2["winning_price"] is None

    # Root total MUST be NULL because lot L2 has an unresolved outcome!
    assert tables["result_revision"][0]["total_winning_price"] is None

    # Quality issue missing_outcome must be logged
    assert any(i.code == "missing_outcome" for i in result.issues)


def test_malformed_contractor_and_lot_entries_mark_issue_and_null_root():
    """Verify malformed contractor and lot entries raise malformed_entry issues and nullify root total."""
    transformer = ContractorResultTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-malformed-1",
            "bidPriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {
                            "id": "c1",
                            "orgCode": "v1",
                            "bidResult": 1,
                            "bidWiningPrice": "200",
                        }
                    ],
                },
                {
                    "id": "lot-2",
                    "lotNo": "2",
                    "contractorList": [
                        "corrupted_non_dict_entry",  # Malformed contractor entry
                    ],
                },
                "corrupted_non_dict_lot",  # Malformed lot entry
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-malformed-1"))
    tables = assemble([result.to_dict()])

    # Valid lot L1 total is kept
    l1 = next(l for l in tables["award_lot"] if l["lot_no"] == "1")
    assert l1["winning_price"] == "200.000000"

    # Root total is NULL due to malformed entries
    assert tables["result_revision"][0]["total_winning_price"] is None

    # Quality issue malformed_entry must be present
    malformed_issues = [i for i in result.issues if i.code == "malformed_entry"]
    assert len(malformed_issues) >= 2


def test_exact_large_value_sum_within_decimal_38_6():
    """Verify sum of large values exceeding default precision 28 maintains exact DECIMAL(38,6) precision."""
    transformer = ContractorResultTransformer()
    # 31 digits before dot + 6 fraction digits = 37 digits total (exceeds default Decimal precision 28!)
    v1 = "5000000000000000000000000000000.123456"
    v2 = "4000000000000000000000000000000.654321"
    expected_sum = "9000000000000000000000000000000.777777"

    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-large-sum-1",
            "bidPriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": v1},
                        {"id": "c2", "orgCode": "v2", "bidResult": 1, "bidWiningPrice": v2},
                    ],
                }
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-large-sum-1"))
    tables = assemble([result.to_dict()])

    # Exact sum preserved with all 37 digits without loss of fractional digits
    assert tables["award_lot"][0]["winning_price"] == expected_sum
    assert tables["result_revision"][0]["total_winning_price"] == expected_sum


def test_decimal_38_6_sum_overflow_detected_rather_than_rounded():
    """Verify that sum overflowing DECIMAL(38,6) capacity (>= 10^32) is detected and nullified."""
    transformer = ContractorResultTransformer()
    v_overflow = (
        "60000000000000000000000000000000.000000"  # 6 * 10^31 each -> sum is 1.2 * 10^32 >= 10^32
    )

    payload = {
        "bideContractorInputResultDTO": {
            "id": "res-overflow-1",
            "bidPriceUnit": "VND",
            "lotResultDTO": [
                {
                    "id": "lot-1",
                    "lotNo": "1",
                    "contractorList": [
                        {"id": "c1", "orgCode": "v1", "bidResult": 1, "bidWiningPrice": v_overflow},
                        {"id": "c2", "orgCode": "v2", "bidResult": 1, "bidWiningPrice": v_overflow},
                    ],
                }
            ],
        }
    }
    result = transformer.transform(make_envelope(payload, source_id="res-overflow-1"))
    tables = assemble([result.to_dict()])

    # Overflow must make totals NULL and flag invalid_money rather than wrapping or rounding
    assert tables["award_lot"][0]["winning_price"] is None
    assert tables["result_revision"][0]["total_winning_price"] is None
    assert any(i.code == "invalid_money" for i in result.issues)


def test_year_repair_retains_original_evidence_and_audit():
    payload = {
        "bideContractorInputResultDTO": {
            "id": "repair-example",
            "publicDate": "3023-09-16T12:00:00",
            "decisionDate": "0222-09-15T12:00:00",
        }
    }
    record = ContractorResultTransformer().transform(make_envelope(payload))
    assert record.typed_attributes["public_date"] == "2023-09-16"
    assert record.typed_attributes["decision_date"] == "2022-09-15"
    repaired = [i for i in record.issues if i.code.startswith("date_year_repaired")]
    assert len(repaired) == 2
    assert {i.code for i in repaired} == {
        "date_year_repaired_0222_to_2022",
        "date_year_repaired_3023_to_2023",
    }
    assert all(i.observation_id == record.observation["observation_id"] for i in repaired)
