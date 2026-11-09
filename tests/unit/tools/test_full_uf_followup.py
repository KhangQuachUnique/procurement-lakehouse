"""A timeout must not cause recomputation or trust an unrelated parity proof."""

import importlib.util
import json
from pathlib import Path

import pytest

from procurement.gold.domain.schema import TABLES


def load_script():
    path = Path(__file__).parents[3] / "scripts/run_gold_full_uf_followup.py"
    spec = importlib.util.spec_from_file_location("full_uf_followup", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_complete_parity_reused_only_for_both_exact_plans(tmp_path):
    script = load_script()
    path = tmp_path / "proof.json"
    assert script.retained_parity(path, "full", "stream") is None
    tables = dict.fromkeys(TABLES, 0)
    proof = {"full_identity": "full", "stream_identity": "stream", "tables": tables}
    path.write_text(json.dumps(proof))
    assert script.retained_parity(path, "full", "stream") == tables
    with pytest.raises(ValueError, match="different plans"):
        script.retained_parity(path, "other", "stream")
    with pytest.raises(ValueError, match="different plans"):
        script.retained_parity(path, "full", "other")


@pytest.mark.parametrize("bad", ["missing", "mismatch", "boolean", "extra"])
def test_incomplete_or_invalid_parity_is_never_reused(tmp_path, bad):
    script = load_script()
    tables = dict.fromkeys(TABLES, 0)
    name = next(iter(tables))
    if bad == "missing":
        del tables[name]
    elif bad == "mismatch":
        tables[name] = 1
    elif bad == "boolean":
        tables[name] = False
    else:
        tables["unknown_table"] = 0
    path = tmp_path / "proof.json"
    path.write_text(json.dumps({
        "full_identity": "full", "stream_identity": "stream", "tables": tables,
    }))
    with pytest.raises(ValueError, match="incomplete or failed"):
        script.retained_parity(path, "full", "stream")
