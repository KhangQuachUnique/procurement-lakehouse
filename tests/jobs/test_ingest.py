import subprocess
import sys

from procurement.jobs.lock import execution_lock


def test_host_lock_blocks_overlap_and_releases_after_exit(tmp_path):
    script = """
import sys
from pathlib import Path
from procurement.jobs.lock import execution_lock
try:
    with execution_lock(Path(sys.argv[1])):
        pass
except RuntimeError:
    raise SystemExit(9)
"""
    with execution_lock(tmp_path):
        result = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path)], timeout=15, check=False
        )
        assert result.returncode == 9
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path)], timeout=15, check=False)
    assert result.returncode == 0
