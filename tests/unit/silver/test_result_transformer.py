"""Unit tests for ContractorResultTransformer (contractor selection results)."""

from datetime import UTC, datetime
from decimal import Decimal

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
    assert lot_child.typed_attributes["lot_price"] == "8000000.000000"
    assert lot_child.typed_attributes["winning_price"] == "7448857.000000"
    assert lot_child.typed_attributes["saving_amount"] == "551143.000000"
    assert lot_child.typed_attributes["saving_rate"] == Decimal("0.0689")

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
    assert lots[0]["saving_amount"] == "50000000.000000"
    assert lots[0]["saving_rate"] == Decimal("0.0500")

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
