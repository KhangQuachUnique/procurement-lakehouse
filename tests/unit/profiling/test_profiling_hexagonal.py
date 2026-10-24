"""Unit tests for Hexagonal Profiling Architecture."""

import json
from pathlib import Path
import tempfile
from unittest.mock import MagicMock

import pytest

from procurement.profiling.adapters.driven.jsonl_stream import JsonlRecordStreamAdapter
from procurement.profiling.adapters.driven.report_export import MarkdownAndJsonReportAdapter
from procurement.profiling.application.service import ProfilingService
from procurement.profiling.domain.models import TableProfile, WindowDrift
from procurement.profiling.domain.profiler import FieldAccumulator, TableProfilerEngine
from procurement.profiling.domain.walker import (
    NestedJsonWalker,
    ValueState,
    classify_value_state,
    detect_type_name,
)
from procurement.profiling.ports.outbound.bronze_metadata_port import (
    ActiveCommitFileDescriptor,
)


def test_classify_value_state():
    assert classify_value_state(None) == ValueState.NULL
    assert classify_value_state("") == ValueState.EMPTY
    assert classify_value_state("   ") == ValueState.EMPTY
    assert classify_value_state([]) == ValueState.EMPTY
    assert classify_value_state({}) == ValueState.EMPTY
    
    # 0, 0.0, False are POPULATED, not missing/null
    assert classify_value_state(0) == ValueState.POPULATED
    assert classify_value_state(0.0) == ValueState.POPULATED
    assert classify_value_state(False) == ValueState.POPULATED
    assert classify_value_state("hello") == ValueState.POPULATED
    assert classify_value_state([1, 2]) == ValueState.POPULATED
    assert classify_value_state({"a": 1}) == ValueState.POPULATED


def test_detect_type_name():
    assert detect_type_name(None) == "null"
    assert detect_type_name(True) == "bool"
    assert detect_type_name(10) == "int"
    assert detect_type_name(3.14) == "float"
    assert detect_type_name("2025-01-02") == "date"
    assert detect_type_name("2025-01-02T10:30:00") == "datetime"
    assert detect_type_name("regular text") == "string"
    assert detect_type_name([1, 2]) == "array"
    assert detect_type_name({"k": "v"}) == "object"


def test_nested_json_walker():
    record = {
        "id": "REC-001",
        "nested": {
            "title": "Project Alpha",
            "score": None,
            "tags": ["urgent", "infra"],
        },
        "lots": [
            {"lot_id": 1, "price": 100.50},
            {"lot_id": 2, "price": 200.00},
        ],
        "empty_list": [],
    }

    paths = list(NestedJsonWalker.walk(record))
    path_names = [p[0] for p in paths]

    assert "$" in path_names
    assert "$.id" in path_names
    assert "$.nested" in path_names
    assert "$.nested.title" in path_names
    assert "$.nested.score" in path_names
    assert "$.nested.tags[]" in path_names
    assert "$.lots[].lot_id" in path_names
    assert "$.lots[].price" in path_names
    assert "$.empty_list" in path_names

    flattened = NestedJsonWalker.flatten_record(record)
    assert len(flattened["$.lots[].lot_id"]) == 2
    assert flattened["$.nested.score"][0][1] == ValueState.NULL
    assert flattened["$.empty_list"][0][1] == ValueState.EMPTY


def test_field_accumulator_metrics():
    acc = FieldAccumulator("$.amount")
    
    acc.update_record([(100.0, ValueState.POPULATED, "float")])
    acc.update_record([(200.0, ValueState.POPULATED, "float")])
    acc.update_record([(300.0, ValueState.POPULATED, "float")])
    acc.update_record([(None, ValueState.NULL, "null")])

    metrics = acc.finalize()
    assert metrics.total_records == 4
    assert metrics.present_records == 4
    assert metrics.populated_records == 3
    assert metrics.null_records == 1
    assert metrics.null_ratio == 0.25
    assert metrics.populated_ratio == 0.75
    assert metrics.completeness_tier == "Sparse"
    assert metrics.distinct_count == 3
    
    assert metrics.numeric_stats is not None
    assert metrics.numeric_stats.min == 100.0
    assert metrics.numeric_stats.max == 300.0
    assert metrics.numeric_stats.mean == 200.0
    assert metrics.numeric_stats.zero_count == 0


def test_table_profiler_engine_and_candidate_keys():
    records = [
        {"source_id": "S1", "source_version": "00", "payload": {"name": "A", "code": "N/A"}},
        {"source_id": "S2", "source_version": "00", "payload": {"name": "B", "code": "C2"}},
        {"source_id": "S3", "source_version": "00", "payload": {"name": "C", "code": "C3"}},
    ]

    engine = TableProfilerEngine(table_name="test_table", resource="test_resource")
    engine.feed_records(records)
    profile = engine.finalize()

    assert profile.profiled_records == 3
    assert "$.source_id" in profile.candidate_keys
    assert "$.source_version" not in profile.candidate_keys
    
    code_field = profile.fields.get("$.payload.code")
    assert code_field is not None
    assert any("sentinel_value_detected" in a for a in code_field.anomalies)


def test_window_drift_comparison():
    prof_a = TableProfile(
        table_name="test",
        resource="res",
        profiled_records=100,
        date_range=("2024-01-01", "2024-06-30"),
    )
    acc_a1 = FieldAccumulator("$.id")
    for i in range(100):
        acc_a1.update_record([(f"id_{i}", ValueState.POPULATED, "string")])
    prof_a.fields["$.id"] = acc_a1.finalize()

    prof_b = TableProfile(
        table_name="test",
        resource="res",
        profiled_records=100,
        date_range=("2025-01-01", "2025-06-30"),
    )
    acc_b1 = FieldAccumulator("$.id")
    for i in range(100):
        acc_b1.update_record([(f"id_{i}", ValueState.POPULATED, "string")])
    prof_b.fields["$.id"] = acc_b1.finalize()

    acc_b2 = FieldAccumulator("$.new_feature")
    for _ in range(100):
        acc_b2.update_record([(1, ValueState.POPULATED, "int")])
    prof_b.fields["$.new_feature"] = acc_b2.finalize()

    drift = TableProfilerEngine.compare_windows(prof_a, prof_b)
    assert "$.new_feature" in drift.new_fields
    assert len(drift.dropped_fields) == 0


def test_profiling_service_isolated_with_mock_ports():
    """Verify ProfilingService operates end-to-end using decoupled ports."""
    mock_meta_port = MagicMock()
    mock_stream_port = MagicMock()

    file_desc = ActiveCommitFileDescriptor(
        commit_id="c1",
        partition_id="p1",
        source="muasamcong",
        resource="project",
        source_date="2025-01-01",
        table_name="project_detail",
        bucket="test-bucket",
        object_key="bronze/test.parquet",
        file_number=1,
        row_count=2,
        size_bytes=100,
    )
    mock_meta_port.get_active_files.return_value = [file_desc]

    records = [
        (file_desc, {"source_id": "P1", "source_version": "00", "payload": {"budget": 1000}}),
        (file_desc, {"source_id": "P2", "source_version": "00", "payload": {"budget": 2000}}),
    ]
    mock_stream_port.stream_active_records.return_value = iter(records)

    service = ProfilingService(
        metadata_port=mock_meta_port,
        bronze_stream_port=mock_stream_port,
    )

    profile = service.profile_active_table("project_detail", "project")

    assert profile.table_name == "project_detail"
    assert profile.profiled_records == 2
    assert "$.source_id" in profile.candidate_keys
    assert profile.fields["$.payload.budget"].numeric_stats.mean == 1500.0


def test_jsonl_adapter_and_report_export():
    with tempfile.TemporaryDirectory(dir=".tmp_pytest" if Path(".tmp_pytest").exists() else None) as tmp_dir:
        jsonl_file = Path(tmp_dir) / "test.jsonl"
        with jsonl_file.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"id": "1", "name": "Alpha"}) + "\n")
            f.write(json.dumps({"id": "2", "name": "Beta"}) + "\n")

        jsonl_adapter = JsonlRecordStreamAdapter()
        report_adapter = MarkdownAndJsonReportAdapter()

        service = ProfilingService(
            jsonl_stream_port=jsonl_adapter,
            report_export_port=report_adapter,
        )

        profile = service.profile_jsonl_file(jsonl_file)
        assert profile.profiled_records == 2
        assert "$.id" in profile.fields
        assert "$.name" in profile.fields

        out_json = Path(tmp_dir) / "profile.json"
        out_report = Path(tmp_dir) / "report.md"

        service.export_reports(
            profiles={"test_jsonl": profile},
            drifts=[],
            summary=[],
            json_path=out_json,
            report_path=out_report,
        )

        assert out_json.exists()
        assert out_report.exists()
        content = out_report.read_text(encoding="utf-8")
        assert "Báo Cáo Khảo Sát & Kiểm Định Dữ Liệu Bronze" in content
