"""Regression tests for Silver flow hardening fixes (P0/P1)."""

import ast
import logging
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from procurement.orchestration.silver import LinkageIndexConfig
from procurement.silver.application.warehouse import ensure_views
from procurement.silver.domain.parsers import sanitize_procurement_date
from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.registry import get_transformer
from procurement.silver.domain.transformers.bid_opening import BidOpeningTransformer
from procurement.silver.domain.transformers.contract_performance import (
    ContractPerformanceTransformer,
)
from procurement.silver.domain.transformers.project import ProjectDetailTransformer
from procurement.silver.domain.transformers.result import ContractorResultTransformer


def _envelope(
    payload: dict,
    source_id: str = "id-1",
    table_name: str = "notify_contractor_standard_detail",
) -> RawRecordEnvelope:
    return RawRecordEnvelope(
        namespace="test",
        table_name=table_name,
        resource="test",
        source_date="2025-01-15",
        run_id="run-1",
        row_ordinal=0,
        file_key=f"bronze/{table_name}.parquet",
        file_sha256="test-checksum",
        source_id=source_id,
        source_version="00",
        content_hash="abc123",
        ingested_at=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
        payload=payload,
    )


def test_sanitize_procurement_date_returns_none_on_invalid_date():
    assert sanitize_procurement_date("not-a-date") is None
    assert sanitize_procurement_date("2025-99-99") is None
    assert sanitize_procurement_date("2025-05-01") == "2025-05-01"
    assert sanitize_procurement_date("1995-01-01") is None


def test_notice_standard_international_does_not_default_currency_to_vnd():
    transformer = get_transformer("notify_contractor_standard_detail")
    env = _envelope(
        {
            "bidoNotifyContractorM": {
                "id": "n-intl-1",
                "notifyNo": "IB2500001",
                "notifyVersion": "00",
                "bidName": "International Package",
                "isDomestic": False,
                "bidPrice": "100000",
                "bidEstimatePrice": "95000",
                "guaranteeValue": "2000",
            }
        },
        source_id="IB2500001",
    )
    res = transformer.transform(env)
    assert res.typed_attributes is not None
    assert res.typed_attributes["currency"] is None
    assert res.typed_attributes["estimate_currency"] is None
    assert res.typed_attributes["guarantee_currency"] is None
    issue_codes = {(i.code, i.path) for i in res.issues}
    assert ("unknown_currency", "estimate_price") in issue_codes
    assert ("unknown_currency", "guarantee_amount") in issue_codes


def test_notice_standard_domestic_still_inherits_vnd():
    transformer = get_transformer("notify_contractor_standard_detail")
    env = _envelope(
        {
            "bidoNotifyContractorM": {
                "id": "n-dom-1",
                "notifyNo": "IB2500002",
                "notifyVersion": "00",
                "bidName": "Domestic Package",
                "isDomestic": True,
                "bidEstimatePrice": "95000",
                "guaranteeValue": "2000",
            }
        },
        source_id="IB2500002",
    )
    res = transformer.transform(env)
    assert res.typed_attributes is not None
    assert res.typed_attributes["estimate_currency"] == "VND"
    assert res.typed_attributes["guarantee_currency"] == "VND"


def test_notice_invalid_embedded_boolean_emits_issue_instead_of_crashing():
    transformer = get_transformer("notify_contractor_standard_detail")
    env = _envelope(
        {
            "bidoNotifyContractorM": {
                "id": "n-bool-1",
                "notifyNo": "IB2500003",
                "notifyVersion": "00",
                "bidId": "pkg-1",
            },
            "bidpPlanDetail": {"id": "pkg-1", "isMultiLot": "not-a-bool"},
        },
        source_id="IB2500003",
    )
    res = transformer.transform(env)
    assert res.typed_attributes is not None
    assert any(i.code == "invalid_boolean" for i in res.issues)


def test_project_result_bid_opening_invalid_dates_emit_issues_instead_of_crashing():
    proj_res = ProjectDetailTransformer().transform(
        _envelope(
            {
                "projectDTO": {
                    "id": "p-1",
                    "no": "PR250001",
                    "publicDate": "bad-pub-date",
                    "decisionDate": "bad-dec-date",
                }
            },
            source_id="PR250001",
            table_name="project_detail",
        )
    )
    assert proj_res.typed_attributes is not None
    assert proj_res.typed_attributes["decision_date"] is None
    assert sum(1 for i in proj_res.issues if i.code == "invalid_date") == 2

    res_out = ContractorResultTransformer().transform(
        _envelope(
            {
                "bideContractorInputResultDTO": {
                    "id": "r-1",
                    "notifyNo": "IB250001",
                    "publicDate": "invalid-date",
                    "decisionDate": "also-invalid",
                }
            },
            source_id="r-1",
            table_name="contractor_result_detail",
        )
    )
    assert res_out.typed_attributes is not None
    assert sum(1 for i in res_out.issues if i.code == "invalid_date") == 2

    bo_out = BidOpeningTransformer().transform(
        _envelope(
            {
                "opening_out": {
                    "bidoNotifyContractorP": {
                        "id": "bo-1",
                        "notifyNo": "IB250001",
                        "notifyVersion": "00",
                        "publicDate": "not-a-date",
                    }
                }
            },
            source_id="bo-1",
            table_name="bid_opening_detail",
        )
    )
    assert bo_out.typed_attributes is not None
    assert bo_out.typed_attributes["public_date"] is None
    assert any(i.code == "invalid_date" for i in bo_out.issues)


def test_contract_performance_handles_null_resolution_gracefully():
    res = ContractPerformanceTransformer().transform(
        _envelope(
            {
                "performance": {
                    "id": "cp-1",
                    "contractId": "c-1",
                },
                "resolution": None,
            },
            source_id="cp-1",
            table_name="contract_performance_detail",
        )
    )
    assert res.typed_attributes is not None
    assert any(i.code == "missing_resolution" for i in res.issues)


def test_ensure_views_logs_warning_without_name_error(caplog):
    db = MagicMock()
    db.sql.return_value = None

    def execute_side_effect(sql: str):
        if sql.strip().startswith("CREATE OR REPLACE VIEW"):
            raise RuntimeError("simulated view error")

    db.execute.side_effect = execute_side_effect
    with caplog.at_level(logging.WARNING):
        created = ensure_views(db, namespace="silver_test")
    assert created == []
    assert any("Failed to create view" in rec.message for rec in caplog.records)


def test_silver_has_no_reverse_imports_from_gold():
    silver_root = Path("src/procurement/silver")
    offenders: list[str] = []
    for py_file in sorted(silver_root.rglob("*.py")):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("procurement.gold"):
                        offenders.append(f"{py_file}:{node.lineno} -> {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("procurement.gold"):
                offenders.append(f"{py_file}:{node.lineno} -> {node.module}")
    assert offenders == [], f"Silver must not import from Gold: {offenders}"


def test_linkage_index_config_defaults_to_incremental():
    assert LinkageIndexConfig().mode == "incremental"
