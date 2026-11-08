"""Read-only, headless completion gate. Never invokes a model or changes a job."""

import json
import subprocess
import sys
from pathlib import Path


def main():
    root, unit = Path(sys.argv[1]), sys.argv[2]
    try:
        status = subprocess.run(
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
                "ExecMainStatus",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        props = dict(line.split("=", 1) for line in status.stdout.splitlines() if "=" in line)
        job = json.loads((root / "job.json").read_text())
        running = props.get("ActiveState") == "active" and props.get("SubState") == "running"
        done = job.get("status") != "RUNNING" or not running
        current = job.get("steps", [])[-1] if job.get("steps") else {}
        result = {
            "done": done,
            "status": job.get("status"),
            "stage": current.get("name"),
            "unit": unit,
            "service": props,
            "job_file": str(root / "job.json"),
            "log": current.get("log"),
        }
    except (OSError, ValueError) as exc:
        result = {
            "done": True,
            "status": "MONITOR_ERROR",
            "error": str(exc),
            "job_file": str(root / "job.json"),
        }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
