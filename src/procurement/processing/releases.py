"""Single-host publication of immutable releases and a checked current pointer."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from procurement.common.file_lock import exclusive_file_lock
from procurement.processing.silver.selection import digest
from procurement.processing.warehouse import stage
from procurement.storage.iceberg import identifier
from procurement.storage.io import read_json, write_json


class ReleaseCommitUncertainError(RuntimeError):
    pass


def prefix(bucket, namespace):
    return f"{bucket}/_releases/{identifier(namespace)}"


def current(fs, bucket, namespace):
    return read_json(fs, prefix(bucket, namespace) + "/current.json")


def load(fs, bucket, namespace, release_id=None):
    pointer = current(fs, bucket, namespace) if release_id is None else {"release_id": release_id}
    if pointer is None:
        raise ValueError("No certified release")
    rid = identifier(pointer["release_id"])
    release = read_json(fs, f"{prefix(bucket, namespace)}/{rid}.json")
    if not release or release["status"] != "certified" or release["namespace"] != namespace:
        raise ValueError("Invalid certified release")
    actual = digest({key: value for key, value in release.items() if key != "manifest_hash"})
    if actual != release["manifest_hash"] or ("manifest_hash" in pointer and pointer["manifest_hash"] != actual):
        raise ValueError("Release manifest checksum mismatch")
    return release


def _set_pointer(fs, key, pointer):
    try:
        write_json(fs, key, pointer)
    except Exception as exc:
        try:
            if read_json(fs, key) == pointer:
                return
        except Exception:  # noqa: BLE001, S110
            pass
        raise ReleaseCommitUncertainError("Release pointer was not acknowledged; reconcile before retry") from exc
    if read_json(fs, key) != pointer:
        raise ReleaseCommitUncertainError("Release pointer readback differs; reconcile before retry")


def publish(fs, db, catalog, *, bucket, namespace, tables, report, inputs, expected_parent,
            lock_dir, schemas=None):
    if report.get("blocking_issues", 0) or report.get("quarantined", 0):
        raise ValueError("Candidate has blocking quality issues; inspect its report")
    rid = "r" + uuid4().hex
    with exclusive_file_lock(Path(lock_dir) / "lakehouse-publisher.lock"):
        parent = current(fs, bucket, namespace)
        if parent != expected_parent:
            raise ValueError("Certified parent changed; rebuild/review candidate")
        kwargs = {} if schemas is None else {"schemas": schemas}
        snapshots = stage(db, catalog, namespace, tables, rid, **kwargs)
        release = {"release_id": rid, "namespace": namespace, "status": "certified",
                   "created_at": datetime.now(UTC).isoformat(), "parent": parent,
                   "tables": snapshots, "quality": report, "inputs": inputs}
        release["manifest_hash"] = digest(release)
        key = f"{prefix(bucket, namespace)}/{rid}.json"
        try:
            write_json(fs, key, release)
        except Exception as exc:
            if read_json(fs, key) != release:
                raise ReleaseCommitUncertainError("Cannot verify immutable release manifest") from exc
        if read_json(fs, key) != release:
            raise ReleaseCommitUncertainError("Immutable release readback mismatch")
        if current(fs, bucket, namespace) != parent:
            raise ValueError("Release parent changed before publication")
        _set_pointer(fs, prefix(bucket, namespace) + "/current.json",
                     {"release_id": rid, "manifest_hash": release["manifest_hash"]})
    return release


def rollback(fs, db, *, bucket, namespace, release_id, expected_parent, lock_dir):
    from procurement.processing.warehouse import read_pinned

    with exclusive_file_lock(Path(lock_dir) / "lakehouse-publisher.lock"):
        if current(fs, bucket, namespace) != expected_parent:
            raise ValueError("Certified parent changed")
        release = load(fs, bucket, namespace, release_id)
        for name, entry in release["tables"].items():
            if len(read_pinned(db, release, name)) != entry["rows"]:
                raise ValueError("Rollback snapshot is missing or corrupt")
        _set_pointer(fs, prefix(bucket, namespace) + "/current.json",
                     {"release_id": release_id, "manifest_hash": release["manifest_hash"]})
        return release
