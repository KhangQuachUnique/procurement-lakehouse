"""Unit tests for ProjectDetailTransformer reference implementation."""

from datetime import UTC, datetime

from procurement.processing.silver.protocols import RawRecordEnvelope, SilverTransformerProtocol
from procurement.processing.silver.registry import get_transformer
from procurement.processing.silver.transformers.project import ProjectDetailTransformer


def make_envelope(payload: dict, source_id: str = "proj-123") -> RawRecordEnvelope:
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name="project_detail",
        resource="project",
        source_date="2026-03-01",
        run_id="run-001",
        row_ordinal=0,
        file_key="bronze/project/2026-03-01/file.parquet",
        file_sha256="fake_sha256",
        source_id=source_id,
        source_version="1",
        content_hash="fake_content_hash",
        ingested_at=datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC),
        payload=payload,
    )


def test_project_transformer_protocol_conformance():
    transformer = ProjectDetailTransformer()
    assert isinstance(transformer, SilverTransformerProtocol)
    assert transformer.table_name == "project_detail"
    assert transformer.entity_type == "project"
    assert transformer.id_scheme == "uuid"


def test_registry_lookup():
    transformer = get_transformer("project_detail")
    assert transformer is not None
    assert isinstance(transformer, ProjectDetailTransformer)


def test_transform_valid_project():
    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {
            "id": "PRJ-9999",
            "no": "NO-1234",
            "name": "Xây dựng trường học mẫu",
            "investorCode": "INV-001",
            "investorName": "Ban QLDA Tỉnh A",
            "investTotal": "5000000000",
            "investUnit": "VND",
            "publicDate": "2026-03-01",
            "decisionNo": "QD-456",
            "decisionDate": "2026-02-28",
            "status": "APPROVED",
            "provCode": "HN",
            "districtCode": "CG",
            "plocation": "Cầu Giấy, Hà Nội",
        }
    }
    envelope = make_envelope(payload, source_id="PRJ-9999")
    result = transformer.transform(envelope)

    # 1. Verification of Observation
    assert result.observation["table_name"] == "project_detail"
    assert result.observation["source_id"] == "PRJ-9999"

    # 2. Verification of Entity
    assert result.entity is not None
    assert result.entity.entity_type == "project"
    assert result.entity.source_identity == "PRJ-9999"

    # 3. Verification of Revision & Attributes
    assert result.revision is not None
    assert result.typed_attributes is not None
    assert result.typed_attributes["title"] == "Xây dựng trường học mẫu"
    assert result.typed_attributes["project_no"] == "NO-1234"
    assert result.typed_attributes["total_investment"] == "5000000000.000000"
    assert result.typed_attributes["investor_name"] == "Ban QLDA Tỉnh A"
    assert result.typed_attributes["prov_code"] == "HN"
    assert result.typed_attributes["amount"] == "5000000000.000000"
    assert result.typed_attributes["currency"] == "VND"

    # 4. Verification of Children (Location)
    assert len(result.children) == 1
    assert result.children[0].kind == "location"

    # 5. No issues or quarantine
    assert len(result.issues) == 0
    assert result.quarantine is None


def test_transform_missing_root_quarantine():
    transformer = ProjectDetailTransformer()
    payload = {"unexpected_key": {}}
    envelope = make_envelope(payload)
    result = transformer.transform(envelope)

    assert result.entity is None
    assert result.revision is None
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_root"
    assert len(result.issues) == 1
    assert result.issues[0].severity == "error"


def test_transform_missing_id_quarantine():
    transformer = ProjectDetailTransformer()
    payload = {"projectDTO": {"name": "Dự án không có ID"}}
    envelope = make_envelope(payload)
    result = transformer.transform(envelope)

    assert result.entity is None
    assert result.revision is None
    assert result.quarantine is not None
    assert result.quarantine.reason == "missing_identity"


def test_transform_invalid_money_records_issue():
    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {
            "id": "PRJ-1",
            "name": "Dự án số 1",
            "investTotal": "invalid_number_abc",
            "investUnit": "VND",
        }
    }
    envelope = make_envelope(payload, source_id="PRJ-1")
    result = transformer.transform(envelope)

    # Record should not be quarantined (non-fatal optional field error)
    assert result.entity is not None
    assert result.typed_attributes["amount"] is None
    # An issue is recorded
    assert any(iss.code == "invalid_money" for iss in result.issues)


def test_clean_pipe_code():
    from procurement.processing.silver.parsers import clean_pipe_code

    assert clean_pipe_code("|115|") == "115"
    assert clean_pipe_code("|11509|") == "11509"
    assert clean_pipe_code("||") is None
    assert clean_pipe_code(None) is None
    assert clean_pipe_code("115") == "115"
    assert clean_pipe_code("  |HN|  ") == "HN"


def test_transform_project_with_pipe_codes_and_toplevel_metadata():
    transformer = ProjectDetailTransformer()
    # Emulate real Muasamcong raw Bronze payload
    payload = {
        "provCode": "|115|",
        "districtCode": "|11509|",
        "plocation": "Huyện Đông Hưng, Tỉnh Thái Bình",
        "investTotalUnit": "VND",
        "projectDTO": {
            "id": "PRJ-REAL-123",
            "no": "PR2400000017",
            "name": "Kiên cố hóa kênh cấp I",
            "investorCode": "vnz000031466",
            "investorName": "UBND xã Liên Giang",
            "investTotal": "1143267000",
            "publicDate": "2024-01-01",
        },
    }
    envelope = make_envelope(payload, source_id="PRJ-REAL-123")
    result = transformer.transform(envelope)

    assert result.quarantine is None
    assert result.typed_attributes["prov_code"] == "115"
    assert result.typed_attributes["district_code"] == "11509"
    assert result.typed_attributes["currency"] == "VND"
    assert result.typed_attributes["amount"] == "1143267000.000000"
    assert len(result.issues) == 0

    assert len(result.children) == 1
    loc_child = result.children[0]
    assert loc_child.kind == "location"
    assert loc_child.source_id == "115"
    assert '"prov_code":"115"' in loc_child.payload_json
    assert '"district_code":"11509"' in loc_child.payload_json
