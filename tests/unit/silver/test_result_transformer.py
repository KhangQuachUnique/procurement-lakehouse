"""Unit tests for ResultDetailTransformer (table: contractor_result_detail)."""

from datetime import UTC, datetime

from procurement.processing.silver.protocols import RawRecordEnvelope, SilverTransformerProtocol
from procurement.processing.silver.registry import get_transformer
from procurement.processing.silver.transformers.result import ResultDetailTransformer


def make_envelope(payload: dict, source_id: str = "result-123", source_version: str | None = "00") -> RawRecordEnvelope:
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name="contractor_result_detail",
        resource="contractor_result",
        source_date="2026-03-01",
        run_id="run-001",
        row_ordinal=0,
        file_key="bronze/contractor_result/2026-03-01/file.parquet",
        file_sha256="fake_sha256",
        source_id=source_id,
        source_version=source_version,
        content_hash="fake_content_hash",
        ingested_at=datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC),
        payload=payload,
    )


# ── Protocol & Registry ──────────────────────────────────────────────


def test_result_transformer_protocol_conformance():
    transformer = ResultDetailTransformer()
    assert isinstance(transformer, SilverTransformerProtocol)
    assert transformer.table_name == "contractor_result_detail"
    assert transformer.entity_type == "result"
    assert transformer.id_scheme == "uuid"


def test_registry_lookup():
    transformer = get_transformer("contractor_result_detail")
    assert transformer is not None
    assert isinstance(transformer, ResultDetailTransformer)


# ── Happy Path: Full Record ──────────────────────────────────────────


def test_transform_valid_result():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-001",
            "resultVersion": "01",
            "notifyNo": "IB2400340694",
            "notifyVersion": "03",
            "bidNo": "BP2400600025",
            "bidId": "PKG-UUID-001",
            "bidName": "Gói thầu xây dựng trường học",
            "bidPrice": "5000000000",
            "bidPriceUnit": "VND",
            "decisionNo": "QD-123",
            "decisionDate": "2026-02-28",
            "publicDate": "2026-03-01",
            "status": "APPROVED",
            "lotResultDTO": [
                {
                    "lotNo": "LOT-001",
                    "lotName": "Lô thầu số 1",
                    "lotPrice": "3000000000",
                    "estimatePrice": "2800000000",
                    "contractorList": [
                        {
                            "contractorCode": "CTR-001",
                            "contractorName": "Công ty TNHH Xây dựng ABC",
                            "taxCode": "0123456789",
                            "bidPrice": "2900000000",
                            "bidWiningPrice": "2850000000",
                            "isWinner": True,
                            "evaluationRank": 1,
                        },
                        {
                            "contractorCode": "CTR-002",
                            "contractorName": "Công ty CP XYZ",
                            "taxCode": "9876543210",
                            "bidPrice": "3100000000",
                            "isWinner": False,
                            "evaluationRank": 2,
                        },
                    ],
                }
            ],
        }
    }
    envelope = make_envelope(payload, source_id="IB2400340694", source_version="01")
    result = transformer.transform(envelope)

    # 1. Observation
    assert result.observation["table_name"] == "contractor_result_detail"
    assert result.observation["source_id"] == "IB2400340694"

    # 2. Entity
    assert result.entity is not None
    assert result.entity.entity_type == "result"
    assert result.entity.source_identity == "RES-001"

    # 3. Revision & Typed Attributes
    assert result.revision is not None
    assert result.typed_attributes is not None
    typed = result.typed_attributes
    assert typed["result_id"] == "RES-001"
    assert typed["result_version"] == "01"
    assert typed["notify_no"] == "IB2400340694"
    assert typed["notify_version"] == "03"
    assert typed["package_no"] == "BP2400600025"
    assert typed["title"] == "Gói thầu xây dựng trường học"
    assert typed["total_winning_price"] == "5000000000.000000"
    assert typed["currency"] == "VND"
    assert typed["decision_no"] == "QD-123"
    assert typed["decision_date"] == "2026-02-28"
    assert typed["public_date"] == "2026-03-01"
    assert typed["status_raw"] == "APPROVED"

    # 4. References (notice + package)
    assert len(result.references) == 2
    notice_ref = next(r for r in result.references if r.target_type == "notice")
    assert notice_ref.id_scheme == "notify_no"
    assert notice_ref.target_identity == "IB2400340694"
    assert notice_ref.field_name == "notifyNo"

    package_ref = next(r for r in result.references if r.target_type == "package")
    assert package_ref.id_scheme == "uuid"
    assert package_ref.target_identity == "PKG-UUID-001"
    assert package_ref.field_name == "bidId"

    # 5. Children: 1 lot + 2 participants = 3 total
    assert len(result.children) == 3
    lots = [c for c in result.children if c.kind == "lot"]
    participants = [c for c in result.children if c.kind == "participant"]
    assert len(lots) == 1
    assert len(participants) == 2

    # Verify lot data
    lot = lots[0]
    assert lot.source_id == "LOT-001"
    assert lot.role == "result_lot"
    assert '"lot_no":"LOT-001"' in lot.payload_json
    assert '"lot_price":"3000000000.000000"' in lot.payload_json

    # Verify winner participant
    winner = next(p for p in participants if '"is_winner":true' in p.payload_json)
    assert winner.source_id == "0123456789"
    assert winner.role == "bid_participation"
    assert '"winning_price":"2850000000.000000"' in winner.payload_json
    assert '"tax_code":"0123456789"' in winner.payload_json

    # 6. No issues or quarantine
    assert len(result.issues) == 0
    assert result.quarantine is None


# ── Quarantine Cases ─────────────────────────────────────────────────


def test_transform_missing_root_quarantine():
    transformer = ResultDetailTransformer()
    payload = {"unexpected_key": {"something": "else"}}
    envelope = make_envelope(payload)
    result = transformer.transform(envelope)

    assert result.entity is None
    assert result.revision is None
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_root"
    assert len(result.issues) == 1
    assert result.issues[0].severity == "error"


def test_transform_missing_id_quarantine():
    transformer = ResultDetailTransformer()
    payload = {"bideContractorInputResultDTO": {"bidName": "Gói thầu không có ID"}}
    envelope = make_envelope(payload)
    result = transformer.transform(envelope)

    assert result.entity is None
    assert result.revision is None
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_identity"


# ── Quality Issue Cases ──────────────────────────────────────────────


def test_transform_invalid_money_records_issue():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-BAD-MONEY",
            "bidName": "Gói thầu tiền lỗi",
            "bidPrice": "not_a_number",
            "bidPriceUnit": "VND",
        }
    }
    envelope = make_envelope(payload, source_id="RES-BAD-MONEY")
    result = transformer.transform(envelope)

    assert result.entity is not None
    assert result.typed_attributes["total_winning_price"] is None
    assert any(iss.code == "invalid_money" for iss in result.issues)


def test_transform_missing_currency_records_issue():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-NO-CUR",
            "bidPrice": "1000000",
        }
    }
    envelope = make_envelope(payload, source_id="RES-NO-CUR")
    result = transformer.transform(envelope)

    assert result.entity is not None
    assert result.typed_attributes["total_winning_price"] == "1000000.000000"
    assert result.typed_attributes["currency"] is None
    assert any(iss.code == "unknown_currency" for iss in result.issues)


def test_transform_invalid_date_records_issue():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-BAD-DATE",
            "decisionDate": "not-a-date",
            "publicDate": "2026-03-01",
        }
    }
    envelope = make_envelope(payload, source_id="RES-BAD-DATE")
    result = transformer.transform(envelope)

    assert result.entity is not None
    assert result.typed_attributes["decision_date"] is None
    assert result.typed_attributes["public_date"] == "2026-03-01"
    assert any(iss.code == "invalid_date" for iss in result.issues)


# ── Children Edge Cases ──────────────────────────────────────────────


def test_no_lots_returns_empty_children():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-NO-LOTS",
            "notifyNo": "IB0000000001",
        }
    }
    envelope = make_envelope(payload, source_id="IB0000000001")
    result = transformer.transform(envelope)

    assert result.entity is not None
    assert len(result.children) == 0


def test_lot_without_contractors():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-LOT-ONLY",
            "notifyNo": "IB0000000002",
            "lotResultDTO": [
                {
                    "lotNo": "LOT-SOLO",
                    "lotName": "Lô thầu duy nhất",
                }
            ],
        }
    }
    envelope = make_envelope(payload, source_id="IB0000000002")
    result = transformer.transform(envelope)

    assert len(result.children) == 1
    assert result.children[0].kind == "lot"
    assert result.children[0].source_id == "LOT-SOLO"


def test_contractor_winning_price_implies_winner():
    """If isWinner is missing but bidWiningPrice is present, infer is_winner=True."""
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-INFER",
            "notifyNo": "IB0000000003",
            "lotResultDTO": [
                {
                    "lotNo": "LOT-INF",
                    "contractorList": [
                        {
                            "taxCode": "1111111111",
                            "contractorName": "Nhà thầu trúng",
                            "bidWiningPrice": "999000000",
                        }
                    ],
                }
            ],
        }
    }
    envelope = make_envelope(payload, source_id="IB0000000003")
    result = transformer.transform(envelope)

    participants = [c for c in result.children if c.kind == "participant"]
    assert len(participants) == 1
    assert '"is_winner":true' in participants[0].payload_json
    assert '"winning_price":"999000000.000000"' in participants[0].payload_json


def test_multiple_lots_with_multiple_contractors():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-MULTI",
            "notifyNo": "IB0000000004",
            "bidPriceUnit": "VND",
            "lotResultDTO": [
                {
                    "lotNo": "LOT-A",
                    "contractorList": [
                        {"taxCode": "AAA", "bidWiningPrice": "100000"},
                        {"taxCode": "BBB"},
                    ],
                },
                {
                    "lotNo": "LOT-B",
                    "contractorList": [
                        {"taxCode": "CCC", "bidWiningPrice": "200000"},
                    ],
                },
            ],
        }
    }
    envelope = make_envelope(payload, source_id="IB0000000004")
    result = transformer.transform(envelope)

    lots = [c for c in result.children if c.kind == "lot"]
    participants = [c for c in result.children if c.kind == "participant"]
    assert len(lots) == 2
    assert len(participants) == 3


# ── References Edge Cases ────────────────────────────────────────────


def test_references_with_missing_notify_no():
    """References should still be created even with None target_identity."""
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-NO-REF",
        }
    }
    envelope = make_envelope(payload, source_id="RES-NO-REF")
    result = transformer.transform(envelope)

    assert len(result.references) == 2
    notice_ref = next(r for r in result.references if r.target_type == "notice")
    assert notice_ref.target_identity is None
    package_ref = next(r for r in result.references if r.target_type == "package")
    assert package_ref.target_identity is None


def test_transform_preserves_root_json():
    transformer = ResultDetailTransformer()
    payload = {
        "bideContractorInputResultDTO": {
            "id": "RES-JSON",
            "bidName": "Gói test JSON",
        }
    }
    envelope = make_envelope(payload, source_id="RES-JSON")
    result = transformer.transform(envelope)

    assert result.typed_attributes is not None
    root_json = result.typed_attributes["root_json"]
    assert '"id":"RES-JSON"' in root_json
    assert '"bidName":"Gói test JSON"' in root_json
