"""Unit tests for ProjectDetailTransformer reference implementation."""

from datetime import UTC, datetime

from procurement.silver.domain.protocols import RawRecordEnvelope, SilverTransformerProtocol
from procurement.silver.domain.registry import get_transformer
from procurement.silver.domain.transformers.project import ProjectDetailTransformer


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
    from procurement.silver.domain.parsers import clean_pipe_code

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


def test_project_full_assembly_and_validation():
    from procurement.silver.application.resources import get_silver_resource
    from procurement.silver.application.revisions import assemble, validate
    from procurement.silver.application.warehouse import typed_arrow_rows

    contract = get_silver_resource("project")
    transformer = ProjectDetailTransformer()
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
    tables = assemble([result.to_dict()])
    quality = validate(tables)

    assert quality["accepted"] == 1
    assert quality["quarantined"] == 0
    assert quality["blocking_issues"] == 0

    revisions = tables["project_revision"]
    assert len(revisions) == 1
    rev = revisions[0]
    assert rev["is_current"] is True
    assert rev["observation_id"] is not None
    assert rev["project_no"] == "PR2400000017"

    locations = tables["project_location"]
    assert len(locations) == 1
    loc = locations[0]
    assert loc["province_code"] == "115"
    assert loc["district_code"] == "11509"
    assert loc["location_name"] == "Huyện Đông Hưng, Tỉnh Thái Bình"

    # Verify PyArrow / DuckDB typed schema casting
    for name in contract.output_tables:
        arrow_table = typed_arrow_rows(name, tables[name])
        assert arrow_table is not None
        if name in {"project_revision", "project_location", "observation"}:
            assert arrow_table.num_rows == 1


def test_project_resource_catalog_and_contract():
    from procurement.silver.application.resources import (
        PROJECT_MAPPING_VERSION,
        PROJECT_OUTPUT_TABLES,
        get_silver_resource,
    )

    for alias in ("project", "du_an"):
        contract = get_silver_resource(alias)
        assert contract.resource == "project"
        assert contract.mapping_version == PROJECT_MAPPING_VERSION
        assert contract.output_tables == PROJECT_OUTPUT_TABLES
        assert contract.select_tables() == ("project_detail",)


def test_project_version_fallback_from_dto():
    from dataclasses import replace

    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {
            "id": "PRJ-V1",
            "no": "NO-V1",
            "name": "Dự án test version",
            "version": "02",
        }
    }
    # Envelope without source_version
    envelope = replace(make_envelope(payload, source_id="PRJ-V1"), source_version=None)
    result = transformer.transform(envelope)

    assert result.revision.source_version == "02"
    assert result.observation["source_version"] is None  # Preserves raw envelope


def test_project_extract_plan_references():
    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {
            "id": "PRJ-REF-1",
            "no": "NO-REF-1",
            "name": "Dự án có KHLCNT",
        },
        "bidpProjectProjectPlanList": [
            {"id": "PLAN-ID-1", "planNo": "PL20240001", "name": "KHLCNT Giai đoạn 1"},
            {"id": "PLAN-ID-2", "planNo": "PL20240002", "name": "KHLCNT Giai đoạn 2"},
        ],
    }
    envelope = make_envelope(payload, source_id="PRJ-REF-1")
    result = transformer.transform(envelope)

    assert len(result.references) == 2
    assert result.references[0].target_type == "plan"
    assert result.references[0].target_identity == "PL20240001"
    assert result.references[1].target_identity == "PL20240002"


def test_project_temporal_inversion_detected():
    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {
            "id": "PRJ-TEMP-1",
            "no": "NO-TEMP-1",
            "name": "Dự án sai ngày",
            "decisionDate": "2026-03-10",
            "publicDate": "2026-03-01",  # Published before decided!
        }
    }
    envelope = make_envelope(payload, source_id="PRJ-TEMP-1")
    result = transformer.transform(envelope)

    assert any(i.code == "invalid_date_order" for i in result.issues)


def test_project_parsed_json_plocation():
    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {"id": "PRJ-LOC-1", "name": "Dự án tại Nghệ An"},
        "plocation": '[{"districtCode":"40331","districtName":"Huyện Thanh Chương","provCode":"403","provName":"Tỉnh Nghệ An"}]',
    }
    envelope = make_envelope(payload, source_id="PRJ-LOC-1")
    result = transformer.transform(envelope)

    assert len(result.children) == 1
    child = result.children[0]
    assert child.typed_attributes["province_name"] == "Tỉnh Nghệ An"
    assert child.typed_attributes["district_name"] == "Huyện Thanh Chương"
    assert child.typed_attributes["location_name"] == "Huyện Thanh Chương, Tỉnh Nghệ An"


def test_project_multi_location_array():
    import json
    transformer = ProjectDetailTransformer()
    payload = {
        "projectDTO": {"id": "PRJ-MULTI-1", "name": "Đường dây tải điện liên tỉnh"},
        "plocation": json.dumps([
            {"districtCode": "40331", "districtName": "Huyện Thanh Chương", "provCode": "403", "provName": "Tỉnh Nghệ An"},
            {"districtCode": "40501", "districtName": "Huyện Hương Sơn", "provCode": "405", "provName": "Tỉnh Hà Tĩnh"},
        ]),
    }
    envelope = make_envelope(payload, source_id="PRJ-MULTI-1")
    result = transformer.transform(envelope)

    assert len(result.children) == 2
    assert result.children[0].typed_attributes["province_name"] == "Tỉnh Nghệ An"
    assert result.children[1].typed_attributes["province_name"] == "Tỉnh Hà Tĩnh"


def test_project_pre_golive_empty_location():
    transformer = ProjectDetailTransformer()
    # Emulate pre-2022-09-16 legacy records
    payload = {
        "projectDTO": {"id": "PRJ-OLD-1", "name": "Dự án năm 2022 cũ"},
        "provCode": "||",
        "districtCode": "||",
        "plocation": None,
    }
    envelope = make_envelope(payload, source_id="PRJ-OLD-1")
    result = transformer.transform(envelope)

    # Should not produce dummy empty location children
    assert len(result.children) == 0


