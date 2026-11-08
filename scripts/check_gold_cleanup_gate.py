"""Read-only small gate for owner-authorised post-publication cleanup."""

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

from procurement.gold.v2.schema import TABLES


def check(root, properties):
    root = Path(root)
    marker = root / "job.json"
    receipt = root / "receipt.json"
    job = json.loads(marker.read_text()) if marker.exists() else {}
    active = properties.get("ActiveState") in (
        "active",
        "activating",
        "deactivating",
    ) and properties.get("MainPID") not in (None, "0")
    final = {}
    if receipt.exists():
        final = json.loads(receipt.read_text())
    verify = final.get("verification", {})
    actual = verify.get("tables", {})
    ready = (
        not active
        and job.get("status") == "COMMITTED_VERIFIED"
        and final.get("status") == "COMMITTED_VERIFIED"
        and final.get("scope") == ["2022-01-01", "2025-12-31"]
        and final.get("gold_target") == "gold_v2_main_2022_2025"
        and final.get("source", {}).get("namespace") == "silver_v2_main_2022_2025"
        and final.get("source", {}).get("source_start") == "2022-01-01"
        and final.get("source", {}).get("source_end") == "2025-12-31"
        and verify.get("verified") is True
        and set(actual) == set(TABLES)
        and set(final.get("tables", {})) == set(TABLES)
        and all(v.get("matches_checkpoint") is True for v in actual.values())
    )
    done = root / "cleanup/receipt.json"
    cleaned = done.exists() and json.loads(done.read_text()).get("status") == "CLEANUP_VERIFIED"
    stopped = not active and bool(job) and not ready and not cleaned
    stamp = hashlib.sha256(
        receipt.read_bytes() if ready else marker.read_bytes() if marker.exists() else b"no-marker"
    ).hexdigest()
    return {
        "ready": bool(ready and not cleaned),
        "failed": stopped,
        "cleaned": bool(cleaned),
        "status": job.get("status", "WAITING"),
        "current": job.get("current"),
        "fingerprint": ("READY:" if ready else "BLOCKED:") + stamp,
        "root": str(root),
    }


def main():
    root = Path(sys.argv[1]).resolve()
    owner = json.loads((root / "active-owner.json").read_text())
    unit = owner["unit"]
    if not re.fullmatch(r"procurement-gold-main-[a-z0-9-]+\.service", unit):
        raise ValueError("Invalid main owner")
    result = subprocess.run(
        ["systemctl", "--user", "show", unit, "-p", "ActiveState", "-p", "MainPID"],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    props = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    print(json.dumps(check(root, props)))


if __name__ == "__main__":
    main()
