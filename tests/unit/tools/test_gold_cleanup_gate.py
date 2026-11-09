import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from procurement.gold.domain.schema import TABLES

spec = importlib.util.spec_from_file_location(
    "cleanup_gate", Path(__file__).parents[3] / "scripts/check_gold_cleanup_gate.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture(root):
    job = {"status": "COMMITTED_VERIFIED", "current": "complete"}
    receipt = {
        "status": "COMMITTED_VERIFIED",
        "scope": ["2022-01-01", "2025-12-31"],
        "gold_target": "gold_v2_main_2022_2025",
        "source": {
            "namespace": "silver_v2_main_2022_2025",
            "source_start": "2022-01-01",
            "source_end": "2025-12-31",
        },
        "tables": {name: {} for name in TABLES},
        "verification": {
            "verified": True,
            "tables": {n: {"matches_checkpoint": True, "rows": 0} for n in TABLES},
        },
    }
    (root / "job.json").write_text(json.dumps(job))
    (root / "receipt.json").write_text(json.dumps(receipt))
    return receipt


def test_only_complete_stopped_main_can_wake_cleanup(tmp_path):
    fixture(tmp_path)
    assert module.check(tmp_path, {"ActiveState": "inactive", "MainPID": "0"})["ready"]
    assert not module.check(tmp_path, {"ActiveState": "active", "MainPID": "123"})["ready"]


@pytest.mark.parametrize("bad", ["scope", "target", "source", "missing_table", "mismatch"])
def test_unsafe_receipt_never_unblocks_cleanup(tmp_path, bad):
    r = fixture(tmp_path)
    if bad == "scope":
        r["scope"][1] = "2023-12-28"
    elif bad == "target":
        r["gold_target"] = "gold_v2_test"
    elif bad == "source":
        r["source"]["namespace"] = "old_silver"
    elif bad == "missing_table":
        r["verification"]["tables"].pop(next(iter(TABLES)))
    else:
        r["verification"]["tables"][next(iter(TABLES))]["matches_checkpoint"] = False
    (tmp_path / "receipt.json").write_text(json.dumps(r))
    result = module.check(tmp_path, {"ActiveState": "inactive", "MainPID": "0"})
    assert not result["ready"] and result["failed"]


def test_failed_pipeline_notifies_blocker_without_cleanup(tmp_path):
    (tmp_path / "job.json").write_text(json.dumps({"status": "FAILED"}))
    result = module.check(tmp_path, {"ActiveState": "failed", "MainPID": "0"})
    assert result["failed"] and not result["ready"]


def test_verified_cleanup_is_not_executed_again(tmp_path):
    fixture(tmp_path)
    (tmp_path / "cleanup").mkdir()
    (tmp_path / "cleanup/receipt.json").write_text(json.dumps({"status": "CLEANUP_VERIFIED"}))
    result = module.check(tmp_path, {"ActiveState": "inactive", "MainPID": "0"})
    assert result["cleaned"] and not result["ready"] and not result["failed"]


def test_direct_scheduler_invocation_without_pythonpath(tmp_path):
    fixture(tmp_path)
    (tmp_path / "active-owner.json").write_text(
        json.dumps({"unit": "procurement-gold-main-test.service"})
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    systemctl = bin_dir / "systemctl"
    systemctl.write_text("#!/bin/sh\nprintf 'ActiveState=inactive\\nMainPID=0\\n'\n")
    systemctl.chmod(0o755)
    env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ["PATH"])
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-I", str(Path(module.__file__).resolve()), str(tmp_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert json.loads(result.stdout)["ready"] is True
    assert result.stderr == ""
