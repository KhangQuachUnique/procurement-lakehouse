"""Rebuildable SQLite read model. Each request sees one committed sync snapshot."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path


class IndexNotReady(RuntimeError):
    pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS objects (
    key TEXT PRIMARY KEY, fingerprint TEXT, kind TEXT NOT NULL,
    source TEXT NOT NULL, resource TEXT NOT NULL, run_id TEXT NOT NULL,
    source_date TEXT, start_date TEXT, end_date TEXT, status TEXT, started_at TEXT,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS objects_runs
    ON objects(kind, source, resource, status, started_at DESC);
CREATE INDEX IF NOT EXISTS objects_recent
    ON objects(kind, source, resource, started_at DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS objects_run_dates
    ON objects(kind, source, resource, run_id, source_date);
CREATE INDEX IF NOT EXISTS objects_dates
    ON objects(kind, source, resource, source_date);
CREATE TABLE IF NOT EXISTS errors (
    object_key TEXT NOT NULL REFERENCES objects(key) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL, source TEXT NOT NULL, resource TEXT NOT NULL,
    run_id TEXT NOT NULL, source_date TEXT NOT NULL, stage TEXT NOT NULL,
    error_type TEXT NOT NULL, occurred_at TEXT NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY(object_key, ordinal)
);
CREATE INDEX IF NOT EXISTS errors_recent ON errors(source, resource, occurred_at DESC);
CREATE INDEX IF NOT EXISTS errors_attempt ON errors(source, resource, run_id, source_date);
"""


def journal_mode(version=sqlite3.sqlite_version_info):
    # WAL reset race is fixed in these releases. Older bundled SQLite uses rollback journaling.
    # https://www.sqlite.org/wal.html#walreset
    fixed = version >= (3, 51, 3) or (3, 44, 6) <= version < (3, 45, 0)
    fixed = fixed or (3, 50, 7) <= version < (3, 51, 0)
    return "WAL" if fixed else "DELETE"


def _utc(value):
    return datetime.fromisoformat(value).astimezone(UTC).isoformat() if value else None


class OpsIndex:
    def __init__(self, path: str | Path, *, namespace: str):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("Unsupported Ops index schema; rebuild at a new OPS_INDEX_PATH")
            if version == 1:
                previous = db.execute("SELECT value FROM metadata WHERE key='namespace'").fetchone()
                if previous is None or previous[0] != namespace:
                    raise ValueError("Ops index belongs to another storage; choose another OPS_INDEX_PATH")
                return
            db.execute(f"PRAGMA journal_mode={journal_mode()}")
            db.executescript(SCHEMA)
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT value FROM metadata WHERE key='namespace'").fetchone()
            if previous and previous[0] != namespace:
                raise ValueError("Ops index belongs to another storage; choose another OPS_INDEX_PATH")
            db.execute("INSERT OR IGNORE INTO metadata VALUES ('namespace', ?)", (namespace,))
            db.execute("PRAGMA user_version=1")

    @contextmanager
    def connect(self):
        # FastAPI may resume a sync dependency on another thread; each request owns its connection.
        db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def snapshot(self):
        with self.connect() as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            state = self.status(db)
            if not state["ready"]:
                raise IndexNotReady("Ops is building its index. Please retry shortly.")
            yield db, state

    @staticmethod
    def status(db):
        values = dict(db.execute("SELECT key, value FROM metadata"))
        return {
            "ready": "last_success_at" in values,
            "last_success_at": values.get("last_success_at"),
            "snapshot_started_at": values.get("snapshot_started_at"),
            "last_error_at": values.get("last_error_at"),
            "last_error": values.get("last_error"),
            "objects": int(values.get("objects", 0)),
        }

    def get_status(self):
        with self.connect() as db:
            return self.status(db)

    @staticmethod
    def set_metadata(db, values):
        db.executemany(
            "INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [(key, str(value)) for key, value in values.items()],
        )

    def record_failure(self, message):
        with self.connect() as db:
            self.set_metadata(db, {
                "last_error": message, "last_error_at": datetime.now(UTC).isoformat(),
            })

    def apply(self, objects, removed, *, started_at):
        # Payloads were downloaded before taking a write lock. Publish rows and cursor together.
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for item in objects:
                key, fingerprint, kind, source, resource, run_id, payload = item
                fields = payload if isinstance(payload, dict) else {}
                db.execute("""
                    INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET fingerprint=excluded.fingerprint,
                    source_date=excluded.source_date, start_date=excluded.start_date,
                    end_date=excluded.end_date, status=excluded.status,
                    started_at=excluded.started_at, payload=excluded.payload
                """, (key, fingerprint, kind, source, resource, run_id,
                      fields.get("source_date"), fields.get("start_date"), fields.get("end_date"),
                      fields.get("status"), _utc(fields.get("started_at")), json.dumps(payload)))
                if kind == "errors":
                    db.execute("DELETE FROM errors WHERE object_key=?", (key,))
                    db.executemany("INSERT INTO errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                        (key, n, source, resource, run_id, record["source_date"], record["stage"],
                         record["error_type"], _utc(record["occurred_at"]), json.dumps(record))
                        for n, record in enumerate(payload)
                    ])
            db.executemany("DELETE FROM objects WHERE key=?", [(key,) for key in removed])
            self.set_metadata(db, {
                "last_success_at": datetime.now(UTC).isoformat(),
                "snapshot_started_at": started_at,
                "objects": db.execute("SELECT count(*) FROM objects").fetchone()[0],
            })
            db.execute("DELETE FROM metadata WHERE key IN ('last_error', 'last_error_at')")
