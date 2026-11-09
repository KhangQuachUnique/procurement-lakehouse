"""Locked local backup/isolated restore. Remote Iceberg data is never copied."""

import hashlib
import json
import os
import shutil
from pathlib import Path

import duckdb

from .state import StateStore


def sha256(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def inventory(db):
    return {
        schema + "." + name: db.execute(f'SELECT count(*) FROM "{schema}"."{name}"').fetchone()[0]
        for schema, name in db.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_catalog=current_database() AND table_type='BASE TABLE' ORDER BY 1,2"
        ).fetchall()
    }


def copy_file(source, target):
    with Path(source).open("rb") as reader, Path(target).open("xb") as writer:
        shutil.copyfileobj(reader, writer, 1024 * 1024)
        writer.flush()
        os.fsync(writer.fileno())


def backup(root, destination):
    root, destination = Path(root).resolve(), Path(destination).resolve()
    if not (root / "state.duckdb").is_file():
        raise ValueError("No v2 state to back up")
    with StateStore(root) as state:
        destination.mkdir(parents=True, exist_ok=False)
        state.db.execute("CHECKPOINT")
        counts = inventory(state.db)
        copy_file(root / "state.duckdb", destination / "state.duckdb")
        receipt = {
            "version": 1,
            "sha256": sha256(destination / "state.duckdb"),
            "tables": counts,
            "remote_data_backed_up": False,
            "requires_remote_reconciliation_before_write": True,
        }
        with (destination / "manifest.json").open("x") as stream:
            json.dump(receipt, stream, sort_keys=True, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
    return receipt


def restore(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    manifest = json.loads((source / "manifest.json").read_text())
    if manifest.get("version") != 1 or sha256(source / "state.duckdb") != manifest["sha256"]:
        raise ValueError("Backup integrity failure")
    destination.mkdir(parents=True, exist_ok=False)
    copy_file(source / "state.duckdb", destination / "state.duckdb")
    with duckdb.connect(str(destination / "state.duckdb"), read_only=True) as db:
        if inventory(db) != manifest["tables"]:
            raise ValueError("Restored table inventory mismatch")
    if sha256(destination / "state.duckdb") != manifest["sha256"]:
        raise ValueError("Restored bytes differ")
    (destination / "restore-receipt.json").write_text(json.dumps(manifest, indent=2))
    return manifest
