import ast
from pathlib import Path

import pytest
import yaml

pytest.importorskip("dagster")
from dagster import DagsterInstance

ROOT = Path(__file__).resolve().parents[2]


def test_domain_packages_do_not_import_dagster():
    for package in ("common", "ingestion", "storage", "quality", "processing", "analytics"):
        for path in (ROOT / "src/procurement" / package).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
                imports = ([alias.name for alias in node.names] if isinstance(node, ast.Import)
                           else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
                assert all(not name.startswith(("dagster", "procurement.orchestration"))
                           for name in imports), str(path)


def test_local_instance_enforces_pool_limit_and_disables_retry(tmp_path):
    config = yaml.safe_load((ROOT / "infra/dagster/local/dagster.yaml").read_text())
    with DagsterInstance.local_temp(str(tmp_path), overrides=config) as instance:
        concurrency = instance.get_concurrency_config()
        assert concurrency.pool_config.default_pool_limit == 1
        assert concurrency.pool_config.pool_granularity.value == "op"
        assert concurrency.run_queue_config.max_concurrent_runs == 1
        assert not instance.run_retries_enabled
