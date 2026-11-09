import ast
import html
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from pydantic import ValidationError

from procurement.gold.domain.models import RunPlan, SilverReadyInput, Snapshot, logic_version
from procurement.gold.domain.schema import DIMENSIONS, FACTS, TABLES, check_schema, create_tables

ROOT = Path(__file__).parents[2]


def test_exact_schema_matches_approved_diagram():
    # Parse original source independently, not the JSON used by implementation.
    source = ET.parse(ROOT / "docs/schemas/gold-star-schemas-compact-approved-2026-11-06.drawio")
    expected = {}
    for cell in source.iter("mxCell"):
        value = html.unescape(re.sub("<[^>]+>", " ", cell.get("value", ""))).strip()
        match = re.fullmatch(r"(?:(PK|FK)\*\s*)?(\w+)\s*:\s*(.+)", value)
        if match:
            expected.setdefault(cell.get("parent"), {})[match[2]] = (match[3], match[1])
    assert {
        n: {c.name: (c.sql_type, c.role) for c in t.columns} for n, t in TABLES.items()
    } == expected
    assert len(FACTS) == 7 and len(DIMENSIONS) == 9


def test_fact_grains_are_not_common_dimensions():
    assert TABLES["fact_demand_daily"].grain == (
        "report_date",
        "geography_key",
        "sector_key",
        "value_band_key",
        "currency_key",
    )
    assert TABLES["fact_duration_daily"].grain == (
        "report_date",
        "sector_key",
        "method_key",
        "metric_key",
    )
    assert TABLES["fact_status_daily"].grain == ("report_date", "status_key", "sector_key")
    assert "recipient_allocation_state" in TABLES["fact_supplier_daily"].grain


def test_schema_drift_rejected(db):
    create_tables(db, "candidate")
    db.execute("ALTER TABLE candidate.fact_demand_daily ADD COLUMN method_key VARCHAR")
    with pytest.raises(ValueError, match="Schema differs"):
        check_schema(db, "candidate")


@pytest.mark.parametrize(
    "change",
    [
        {"source_complete": False},
        {"quarantined": 1},
        {"blocking_issues": 1},
        {"tables": ()},
        {"completed_at": "2026-11-07T00:00:00"},
        {"namespace": "silver;DROP"},
        {"tables": tuple(Snapshot(table="observation", table_uuid="x", rows=0) for _ in range(8))},
    ],
)
def test_incomplete_or_invalid_readiness_is_not_accepted(plan, change):
    raw = plan.source.model_dump() | change
    with pytest.raises(ValidationError):
        SilverReadyInput.model_validate(raw)


def test_nonempty_source_without_pin_rejected():
    with pytest.raises(ValidationError):
        Snapshot(table="observation", table_uuid="uuid", rows=1)


@pytest.mark.parametrize(
    "change",
    [
        {"mode": "incremental"},
        {"target_namespace": "gold_reporting"},
        {"source_cutoff": "2026-11-08"},
        {"event_start": "2026-01-01"},
        {"observation_day": "2026-11-05"},
        {"run_id": "../bad"},
    ],
)
def test_invalid_plan_fails_closed(plan, change):
    with pytest.raises(ValidationError):
        RunPlan.model_validate(plan.model_dump() | change)


def test_resume_plan_has_no_runtime_clock(plan):
    assert RunPlan.model_validate_json(plan.model_dump_json()) == plan
    with pytest.raises(ValidationError):
        plan.observation_day = plan.source_cutoff


def test_v2_has_no_legacy_or_orchestration_imports():
    for path in (ROOT / "src/procurement/gold/v2").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            names = (
                [a.name for a in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else []
            )
            assert not any(
                n.startswith("procurement.orchestration")
                or n.startswith("procurement.gold.")
                and not n.startswith("procurement.gold.v2")
                for n in names
            ), path
    assert len(logic_version()) == 64
