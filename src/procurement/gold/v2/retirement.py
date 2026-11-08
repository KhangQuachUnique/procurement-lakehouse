"""Code-only legacy inventory and recoverable archive. Never deletes data/code.

Actual retirement is a post-cutover/acceptance stage. This audit exposes import
blockers instead of assuming an unused-looking module is safe to remove.
"""

import ast
import hashlib
import json
import zipfile
from pathlib import Path


def audit(repository):
    repository = Path(repository).resolve()
    legacy = repository / "src/procurement/gold"
    candidates = sorted(
        p
        for p in legacy.rglob("*.py")
        if "v2" not in p.relative_to(legacy).parts and p != legacy / "__init__.py"
    )
    modules = {
        ".".join(p.relative_to(repository / "src").with_suffix("").parts).removesuffix(".__init__")
        for p in candidates
    }
    blockers = []
    for tree in ("src", "scripts", "tests"):
        for path in (repository / tree).rglob("*.py"):
            if path in candidates or "__pycache__" in path.parts:
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                names = (
                    [a.name for a in node.names]
                    if isinstance(node, ast.Import)
                    else [node.module or ""]
                    if isinstance(node, ast.ImportFrom)
                    else []
                )
                for name in names:
                    if any(name == module or name.startswith(module + ".") for module in modules):
                        blockers.append(
                            {
                                "path": str(path.relative_to(repository)),
                                "line": node.lineno,
                                "import": name,
                                "category": "test" if tree == "tests" else "runtime_or_script",
                            }
                        )
    return {
        "code_only": True,
        "deletion_performed": False,
        "files": [
            {
                "path": str(p.relative_to(repository)),
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
            for p in candidates
        ],
        "blockers": blockers,
        "production_cutover_and_acceptance_required": True,
        "excludes": [
            "Silver source Parquet",
            "Iceberg data/manifests/metadata/refs",
            "Gold state/outbox/backups",
            "running original worker",
        ],
    }


def archive(repository, destination):
    repository = Path(repository).resolve()
    destination = Path(destination)
    report = audit(repository)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "x", compression=zipfile.ZIP_DEFLATED) as bundle:
        for entry in report["files"]:
            path = repository / entry["path"]
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
                raise ValueError("Legacy source changed during archive")
            bundle.writestr(entry["path"], raw)
        bundle.writestr("retirement-manifest.json", json.dumps(report, indent=2))
    # Check every archived byte, not just the existence of the ZIP.
    with zipfile.ZipFile(destination) as bundle:
        for entry in report["files"]:
            if hashlib.sha256(bundle.read(entry["path"])).hexdigest() != entry["sha256"]:
                raise ValueError("Legacy archive verification failed")
    return report | {
        "archive": str(destination),
        "archive_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
    }
