"""Event-driven bounded supervisor; same tested CLIs, no data/business logic.

Wait on the EXISTING canonical worker's pidfd (no sleep/model polling), then run
isolated publication and Gold acceptance serially with separate cgroup ceilings.
Never restart an existing stopped/failed data stage or activate production.
"""

import argparse
import json
import os
import select
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from procurement.common.file_lock import exclusive_file_lock

WORKTREE = Path("/home/soul/Dự án/procurement/procurement-gold-v2")
PYTHON = Path("/home/soul/Dự án/procurement/procurement-lakehouse/.venv/bin/python")
ENV = Path("/home/soul/Dự án/procurement/procurement-lakehouse/.env")
ROOT = WORKTREE / "data/gold-v2-acceptance"


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2))
    os.replace(temporary, path)


def properties(unit):
    result = subprocess.run(
        [
            "systemctl",
            "--user",
            "show",
            unit,
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "MainPID",
            "-p",
            "Result",
            "-p",
            "ExecMainStatus",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def wait_existing(unit, marker, expected_status):
    state = json.loads(marker.read_text())
    props = properties(unit)
    if state["status"] == expected_status:
        return state
    if state["status"] != "RUNNING" or props.get("ActiveState") != "active":
        raise RuntimeError("Source stage is stopped or blocked: " + state["status"])
    pid = int(props.get("MainPID", 0))
    if not pid or state.get("pid") != pid:
        raise RuntimeError("Existing source worker identity differs")
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        descriptor = None
    if descriptor is not None:
        try:
            select.select([descriptor], [], [])
        finally:
            os.close(descriptor)
    state = json.loads(marker.read_text())
    props = properties(unit)
    if state["status"] != expected_status:
        raise RuntimeError("Source stage did not pass: " + state["status"])
    # Process exit and successful data receipt are both required. systemd-run may
    # still be settling its final unit state; nonzero main status is never success.
    if props.get("ExecMainStatus") not in ("0", None):
        raise RuntimeError("Source service main status was not successful")
    return state


def launch(
    unit, module, arguments, marker, expected_status, log, *, resume=False, runtime_seconds=21600
):
    if marker.exists():
        state = json.loads(marker.read_text())
        if state["status"] == expected_status:
            return state
        if not resume or state["status"] not in ("FAILED", "BLOCKED"):
            raise RuntimeError(
                "Existing unfinished stage requires inspected same-plan resume: " + unit
            )
    props = properties(unit)
    if props.get("ActiveState") in ("active", "activating"):
        raise RuntimeError("Refusing duplicate stage: " + unit)
    command = [
        "systemd-run",
        "--user",
        "--unit=" + unit.removesuffix(".service"),
        "--property=WorkingDirectory=" + str(WORKTREE),
        "--property=CPUQuota=100%",
        "--property=MemoryHigh=2500M",
        "--property=MemoryMax=3G",
        "--property=MemorySwapMax=256M",
        "--property=RuntimeMaxSec=" + str(runtime_seconds),
        "--setenv=PYTHONPATH=src",
        "--setenv=OPENBLAS_NUM_THREADS=1",
        "--setenv=OMP_NUM_THREADS=1",
        "--wait",
        "--pipe",
        str(PYTHON),
        "-m",
        module,
        *arguments,
        "--env-file",
        str(ENV),
        "--execute-isolated",
    ]
    with log.open("w") as stream:
        result = subprocess.run(
            command, cwd=WORKTREE, stdout=stream, stderr=subprocess.STDOUT, check=False
        )
    if result.returncode:
        raise RuntimeError("Data stage failed, exit " + str(result.returncode) + ": " + unit)
    state = json.loads(marker.read_text())
    if state["status"] != expected_status:
        raise RuntimeError("Stage receipt was not accepted: " + unit)
    return state


def _workflow(root, *, waiter, launcher):
    output = root / "full-history-workflow-v1"
    output.mkdir(parents=True, exist_ok=True)
    marker = output / "job.json"
    state = {
        "status": "RUNNING",
        "pid": os.getpid(),
        "current": "canonical",
        "started_at": datetime.now(UTC).isoformat(),
        "production_written": False,
        "steps": {},
    }
    if marker.exists():
        raise RuntimeError("Workflow already exists; inspect before resuming")
    save(marker, state)
    try:
        local = waiter(
            "procurement-gold-v2-history-20261107.service",
            root / "full-history-canonical-v1/job.json",
            "LOCAL_SILVER_VALIDATED",
        )
        if (
            local["reviewed_resource_days"]
            or local["completed_resource_days"] != 4383
            or local["raw_records"] != 5359973
        ):
            raise RuntimeError("Full canonical source coverage/quality did not pass")
        state["steps"]["canonical"] = {
            "status": local["status"],
            "cohorts": 4383,
            "raw_records": local["raw_records"],
        }
        state["current"] = "publish"
        save(marker, state)
        published = launcher(
            "procurement-gold-v2-history-publish-20261107.service",
            "procurement.tools.gold_history_publish",
            [
                "--source",
                str(root / "full-history-canonical-v1"),
                "--output",
                str(root / "full-history-publication-v1"),
            ],
            root / "full-history-publication-v1/job.json",
            "SILVER_READY",
            output / "publish.log",
        )
        state["steps"]["publish"] = {
            "status": published["status"],
            "namespace": published["namespace"],
        }
        state["current"] = "gold"
        save(marker, state)
        launcher(
            "procurement-gold-v2-history-gold-20261107.service",
            "procurement.tools.gold_history_acceptance",
            [
                "--full-bundle",
                str(root / "full-history-publication-v1/silver-ready.json"),
                "--prefix-bundle",
                str(root / "full-history-publication-v1/silver-ready-through-2024-12-31.json"),
                "--output",
                str(root / "full-history-gold-v1"),
            ],
            root / "full-history-gold-v1/job.json",
            "PASS",
            output / "gold.log",
        )
        receipt = json.loads((root / "full-history-gold-v1/receipt.json").read_text())
        if (
            receipt["status"] != "PASS"
            or receipt["scope"] != ["2022-01-01", "2025-12-31"]
            or not receipt["actual_suffix_tested"]
            or receipt["verified_tables"] != 16
            or any(receipt["incremental_clean_mismatches"].values())
            or not receipt["recovery"]["resumed"]
            or not any(receipt["actual_multi_revision_entities"].values())
        ):
            raise RuntimeError("Gold full-history acceptance evidence incomplete")
        state["steps"]["gold"] = receipt
        state.update(status="PASS", current="complete", finished_at=datetime.now(UTC).isoformat())
        save(marker, state)
        return state
    except BaseException as exc:
        state.update(
            status="BLOCKED",
            error_type=type(exc).__name__,
            error=str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__,
            stopped_at=datetime.now(UTC).isoformat(),
        )
        save(marker, state)
        raise


def workflow(root=ROOT, *, waiter=wait_existing, launcher=launch):
    output = root / "full-history-workflow-v1"
    output.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(output / "supervisor.lock"):
        return _workflow(root, waiter=waiter, launcher=launcher)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute-authorized-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_authorized_isolated:
        raise SystemExit("Explicit authorized isolated workflow flag required")
    try:
        print(json.dumps(workflow()), flush=True)
    except RuntimeError as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}), flush=True)
        sys.exit(1)
